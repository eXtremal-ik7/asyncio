#include "asyncio/smtp.h"
#include "asyncio/asyncio.h"
#include "asyncio/base64.h"
#include "asyncio/dynamicBuffer.h"
#include "asyncio/socketSSL.h"
#include "asyncio/socket.h"
#include <memory.h>
#include <string.h>

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;
static ConcurrentQueue objectPool;

typedef enum SmtpOpTy {
  SmtpOpConnect = OPCODE_WRITE,
  SmtpOpStartTls,
  SmtpOpCommand
} SmtpOpTy;

typedef enum SmtpOpState {
  stInitialize = 0,
  stReadGreeting,
  stEhlo1,
  stEhlo2,
  stSendStartTls,
  stStartTls,
  stLogin,
  stSendLogin,
  stSendPassword,
  stFrom,
  stTo,
  stSendData,
  stText,
  stFinished
} SmtpOpState;

typedef struct SMTPClient {
  aioObjectRoot root;
  SmtpServerType Type;
  aioObject *PlainSocket;
  SSLSocket *TlsSocket;

  char buffer[8192];
  char *ptr;
  char *end;

  unsigned ResultCode;
  const char *Response;
} SMTPClient;

typedef struct SMTPOp {
  asyncOpRoot Root;
  HostAddress Address;
  int State;
  dynamicBuffer Buffer;

  // Specific data
  int startTls;
  char *ehlo;
  char *login;
  char *password;
  char *from;
  char *to;
  char *text;
  size_t loginSize;
  size_t ehloSize;
  size_t passwordSize;
  size_t fromSize;
  size_t toSize;
  size_t textSize;
} SMTPOp;

static inline size_t putBase64(dynamicBuffer *buffer, const void *data, size_t *size)
{
  size_t length = strlen(data);
  size_t base64Length = base64getEncodeLength(length);
  size_t offset = buffer->offset;
  char *ptr = dynamicBufferAlloc(buffer, base64Length + 2);
  base64Encode(ptr, data, length);
  ptr[base64Length]   = '\r';
  ptr[base64Length+1] = '\n';
  *size = base64Length+2;
  return offset;
}

static inline void dynamicBufferWriteString(dynamicBuffer *buffer, const char *data)
{
  dynamicBufferWrite(buffer, data, strlen(data));
}

// Envelope addresses and header values: drop CR/LF so user data cannot
// inject protocol or header lines
static void dynamicBufferWriteNoCrlf(dynamicBuffer *buffer, const char *data)
{
  for (;;) {
    size_t run = strcspn(data, "\r\n");
    dynamicBufferWrite(buffer, data, run);
    data += run;
    if (*data == 0)
      break;
    data++;
  }
}

// Body writer, RFC 5321 4.5.2/2.3.8: doubles a leading dot on every line (the
// text cannot contain the DATA terminator), normalizes bare LF/CR line
// endings to CRLF (CR and LF reach the wire only as a pair) and appends the
// ".<CRLF>" terminator without an extra blank line when the text already ends
// with a line break
static void dynamicBufferWriteMailBody(dynamicBuffer *buffer, const char *text)
{
  const char *p = text;
  while (*p) {
    if (*p == '.')
      dynamicBufferWrite(buffer, ".", 1);
    size_t content = strcspn(p, "\r\n");
    dynamicBufferWrite(buffer, p, content);
    dynamicBufferWrite(buffer, "\r\n", 2);
    p += content;
    if (*p == '\r')
      p++;
    if (*p == '\n')
      p++;
  }
  dynamicBufferWrite(buffer, ".\r\n", 3);
}

static int cancel(asyncOpRoot *opptr)
{
  // Target the object child operations are queued on (SSL ops forward the
  // cancel to the underlying socket themselves)
  SMTPClient *client = (SMTPClient*)opptr->object;
  if (client->TlsSocket)
    cancelIo(&client->TlsSocket->root);
  else
    cancelIo(aioObjectHandle(client->PlainSocket));
  return 0;
}

static void connectFinish(asyncOpRoot *opptr)
{
  ((smtpConnectCb*)opptr->callback)(opGetStatus(opptr), (SMTPClient*)opptr->object, opptr->arg);
}

static void commandFinish(asyncOpRoot *opptr)
{
  SMTPClient *client = (SMTPClient*)opptr->object;
  ((smtpResponseCb*)opptr->callback)(opGetStatus(opptr), client->ResultCode, client, opptr->arg);
}

static void releaseProc(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  dynamicBufferFree(&op->Buffer);
}


static SMTPOp *allocSmtpOp(aioExecuteProc executeProc,
                           aioFinishProc finishProc,
                           SMTPClient *client,
                           int type,
                           void *callback,
                           void *arg,
                           AsyncFlags flags,
                           uint64_t timeout)
{
  SMTPOp *op = 0;
  asyncOpAlloc(client->root.header.base, sizeof(SMTPOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op);
  dynamicBufferInit(&op->Buffer, 1024);

  initAsyncOpRoot(&op->Root, executeProc, cancel, finishProc, releaseProc, &client->root, callback, arg, flags, type, timeout);
  op->State = stInitialize;
  dynamicBufferClear(&op->Buffer);
  return op;
}

static void smtpConnectProc(AsyncOpStatus status, aioObject *object, void *arg)
{
  __UNUSED(object);
  resumeParent((asyncOpRoot*)arg, status);
}

static void smtpsConnectProc(AsyncOpStatus status, SSLSocket *object, void *arg)
{
  __UNUSED(object);
  resumeParent((asyncOpRoot*)arg, status);
}

static int isDigit(char s)
{
  return (s >= '0' && s <= '9');
}

static inline int isDigits(const char *s, size_t size) {
  for (size_t i = 0; i < size; i++) {
    if (!isDigit(s[i]))
      return 0;
  }

  return 1;
}

// Try to extract one complete server response from [ptr, end); aosPending
// means more data is needed. On success ptr is advanced past the response,
// Response and ResultCode are set
static AsyncOpStatus smtpTryParse(SMTPClient *client)
{
  char firstReplyCode[4];
  char *base = client->ptr;

  if (client->end - base < 5)
    return aosPending;

  // Check begin of response: 3-digit code and a '-'/' ' separator
  if (!isDigits(base, 3) || (base[3] != '-' && base[3] != ' '))
    return (AsyncOpStatus)smtpInvalidFormat;

  firstReplyCode[0] = base[0];
  firstReplyCode[1] = base[1];
  firstReplyCode[2] = base[2];
  firstReplyCode[3] = 0;
  client->ResultCode = atoi(firstReplyCode);

  if (base[3] == '-') {
    // Multiline response detected
    // Find end of message
    size_t linesCount = 1;
    char *lf = memchr(base + 4, '\n', client->end - base - 4);
    if (!lf)
      return aosPending;
    if (lf[-1] != '\r') {
      // Lines must end with CRLF; the squash below strips exactly the CR
      // (also keeps the length from underflowing on a line like "NNN-<LF>")
      return (AsyncOpStatus)smtpInvalidFormat;
    }

    const char *p = lf + 1;
    for (;;) {
      // Get end of line
      lf = memchr(p, '\n', client->end - p);
      if (!lf)
        return aosPending;

      linesCount++;
      if (lf - p < 5 || lf[-1] != '\r' || memcmp(p, firstReplyCode, 3) != 0 ||
          (p[3] != '-' && p[3] != ' '))
        return (AsyncOpStatus)smtpInvalidFormat;

      if (p[3] == ' ') {
        // Last line in multiline answer
        break;
      }

      p = lf + 1;
    }

    // Squash multiline text to single C-string
    p = base;
    char *out = base;
    for (size_t i = 0; i < linesCount; i++) {
      lf = memchr(p, '\n', client->end - p);

      size_t lineSize = lf-1-(p+4);
      memmove(out, p+4, lineSize);

      out[lineSize] = '\n';
      p = lf + 1;
      out += lineSize+1;
    }

    *out = 0;
    client->ptr = (char*)p;
    client->Response = base;
  } else {
    // Other response parse
    // Find '\n'
    char *lf = memchr(base + 4, '\n', client->end - base - 4);
    if (!lf)
      return aosPending;
    if (lf[-1] != '\r') {
      // Line must end with CRLF: zeroing *(lf-1) terminates Response exactly
      // at the CR
      return (AsyncOpStatus)smtpInvalidFormat;
    }
    client->Response = base + 4;
    *(lf-1) = 0;
    client->ptr = lf+1;
  }

  return aosSuccess;
}

// Distinguished expectation for smtpCheckReply: RCPT accepts 250 (delivered),
// 251 (will forward) and 252 (cannot verify, will attempt delivery)
enum { smtpExpectRcpt = 1 };

// Reply codes are phase-specific: expected is the exact code for the current
// step, smtpExpectRcpt the RCPT set, 0 keeps the generic 2xx/3xx acceptance
// for user-issued commands
static AsyncOpStatus smtpCheckReply(SMTPClient *client, unsigned expected)
{
  unsigned code = client->ResultCode;
  int accepted;
  if (expected == 0)
    accepted = code >= 200 && code <= 399;
  else if (expected == smtpExpectRcpt)
    accepted = code >= 250 && code <= 252;
  else
    accepted = code == expected;
  return accepted ? aosSuccess : (AsyncOpStatus)smtpError;
}

// Read callbacks only account transferred bytes and signal the parent; the
// continuation (parse, next read) is driven by the executeProc
static void smtpReadCb(AsyncOpStatus status, aioObject *object, size_t bytesRead, void *arg)
{
  __UNUSED(object);
  SMTPOp *op = (SMTPOp*)arg;
  if (status == aosSuccess)
    ((SMTPClient*)op->Root.object)->end += bytesRead;
  resumeParent(&op->Root, status);
}

static void smtpSslReadCb(AsyncOpStatus status, SSLSocket *object, size_t bytesRead, void *arg)
{
  __UNUSED(object);
  SMTPOp *op = (SMTPOp*)arg;
  if (status == aosSuccess)
    ((SMTPClient*)op->Root.object)->end += bytesRead;
  resumeParent(&op->Root, status);
}

// Parse the buffered response, reading more from the transport as needed.
// Runs only under the client combiner (executeProc context): a read posted
// here is published before the operation can go pending, so a later cancel
// sweep always finds the outstanding child and a timed-out parent is
// guaranteed a resume
static AsyncOpStatus smtpReadResponse(SMTPClient *client, SMTPOp *op, unsigned expectedCode)
{
  for (;;) {
    AsyncOpStatus status = smtpTryParse(client);
    if (status != aosPending)
      return status == aosSuccess ? smtpCheckReply(client, expectedCode) : status;

    size_t remaining = client->end - client->ptr;
    if (remaining == sizeof(client->buffer)) {
      // Buffer full without a complete response: a zero-size read would
      // complete with 0 bytes and spin here forever
      return (AsyncOpStatus)smtpInvalidFormat;
    }
    if (remaining && client->ptr != client->buffer)
      memmove(client->buffer, client->ptr, remaining);
    client->ptr = client->buffer;
    client->end = client->buffer + remaining;
    client->Response = 0;

    size_t bytesTransferred = 0;
    asyncOpRoot *readOp;
    if (client->TlsSocket)
      readOp = implSslRead(client->TlsSocket, client->end, sizeof(client->buffer) - remaining, afNone, 0, smtpSslReadCb, op, &bytesTransferred);
    else
      readOp = implRead(client->PlainSocket, client->end, sizeof(client->buffer) - remaining, afNone, 0, smtpReadCb, op, &bytesTransferred);
    if (readOp) {
      combinerPushOperation(readOp);
      return aosPending;
    }
    client->end += bytesTransferred;
  }
}

static void smtpWriteCb(AsyncOpStatus status, aioObject *object, size_t bytesTransferred, void *arg)
{
  __UNUSED(object);
  __UNUSED(bytesTransferred);
  resumeParent(&((SMTPOp*)arg)->Root, status);
}

static void smtpSslWriteCb(AsyncOpStatus status, SSLSocket *object, size_t bytesTransferred, void *arg)
{
  __UNUSED(object);
  __UNUSED(bytesTransferred);
  resumeParent(&((SMTPOp*)arg)->Root, status);
}

// Send one command; the caller state parses its response only after the write
// completed, so a write error fails the operation instead of being dropped.
// Data buffered before the command was sent cannot be its reply: a peer
// speaking ahead of the protocol must not advance the state machine
static AsyncOpStatus smtpWriteCommand(SMTPClient *client, SMTPOp *op, const void *data, size_t size)
{
  if (client->ptr != client->end)
    return (AsyncOpStatus)smtpInvalidFormat;

  asyncOpRoot *writeOp;
  if (client->TlsSocket) {
    writeOp = implSslWrite(client->TlsSocket, data, size, afWaitAll, 0, smtpSslWriteCb, op);
  } else {
    size_t bytesTransferred = 0;
    writeOp = implWrite(client->PlainSocket, data, size, afWaitAll, 0, smtpWriteCb, op, &bytesTransferred);
  }
  if (writeOp) {
    combinerPushOperation(writeOp);
    return aosPending;
  }
  return aosSuccess;
}

// One protocol exchange: finish reading the response to the previous command
// (which must carry expectedCode), then send the next one; its response is
// consumed on entry to nextState
static AsyncOpStatus smtpStepReadWrite(SMTPClient *client, SMTPOp *op, unsigned expectedCode, const void *data, size_t size, int nextState)
{
  AsyncOpStatus status = smtpReadResponse(client, op, expectedCode);
  if (status != aosSuccess)
    return status;
  op->State = nextState;
  return smtpWriteCommand(client, op, data, size);
}

// Initial transport connect; the greeting is read by the next step
static AsyncOpStatus smtpStepConnect(SMTPClient *client, SMTPOp *op)
{
  op->State = stReadGreeting;
  if (client->Type == smtpServerPlain)
    aioConnect(client->PlainSocket, &op->Address, 0, smtpConnectProc, op);
  else
    aioSslConnect(client->TlsSocket, &op->Address, 0, 0, smtpsConnectProc, op);
  return aosPending;
}

// STARTTLS upgrade: wrap the plain socket into a TLS session and run the
// handshake over the established connection
static AsyncOpStatus smtpStepUpgradeTls(SMTPClient *client, SMTPOp *op, int nextState)
{
  // Plaintext buffered past the STARTTLS response would later be parsed as if
  // it were TLS-protected (plaintext injection) - refuse the upgrade
  if (client->ptr != client->end)
    return (AsyncOpStatus)smtpInvalidFormat;

  op->State = nextState;
  client->TlsSocket = sslSocketNew(aioGetBase(client->PlainSocket), client->PlainSocket, 0);
  if (!client->TlsSocket)
    return aosUnknownError;
  aioSslConnect(client->TlsSocket, 0, 0, 0, smtpsConnectProc, op);
  return aosPending;
}

static AsyncOpStatus smtpConnectStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  if (op->State == stInitialize)
    return smtpStepConnect(client, op);
  else
    return smtpReadResponse(client, op, 220);
}

static AsyncOpStatus smtpStartTlsStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  for (;;) {
    AsyncOpStatus status = aosSuccess;
    if (op->State == stInitialize) {
      const char startTls[] = "STARTTLS\r\n";
      op->State = stStartTls;
      status = smtpWriteCommand(client, op, startTls, sizeof(startTls)-1);
    } else if (op->State == stStartTls) {
      if ((status = smtpReadResponse(client, op, 220)) == aosSuccess)
        return smtpStepUpgradeTls(client, op, stFinished);
    } else {
      return aosSuccess;
    }
    if (status != aosSuccess)
      return status;
  }
}

static AsyncOpStatus smtpLoginStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  for (;;) {
    AsyncOpStatus status = aosSuccess;
    if (op->State == stInitialize) {
      const char authLogin[] = "AUTH LOGIN\r\n";
      op->State = stSendLogin;
      status = smtpWriteCommand(client, op, authLogin, sizeof(authLogin)-1);
    } else if (op->State == stSendLogin) {
      status = smtpStepReadWrite(client, op, 334, op->login, op->loginSize, stSendPassword);
    } else if (op->State == stSendPassword) {
      status = smtpStepReadWrite(client, op, 334, op->password, op->passwordSize, stFinished);
    } else {
      return smtpReadResponse(client, op, 235);
    }
    if (status != aosSuccess)
      return status;
  }
}

static AsyncOpStatus smtpSendMailStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  for (;;) {
    AsyncOpStatus status = aosSuccess;
    switch (op->State) {
      case stInitialize :
        return smtpStepConnect(client, op);

      case stReadGreeting :
        if ((status = smtpReadResponse(client, op, 220)) == aosSuccess)
          op->State = stEhlo1;
        break;

      case stEhlo1 :
        op->State = op->startTls ? stSendStartTls : stLogin;
        status = smtpWriteCommand(client, op, op->ehlo, op->ehloSize);
        break;

      case stSendStartTls : {
        const char startTls[] = "STARTTLS\r\n";
        status = smtpStepReadWrite(client, op, 250, startTls, sizeof(startTls)-1, stStartTls);
        break;
      }

      case stStartTls :
        if ((status = smtpReadResponse(client, op, 220)) == aosSuccess)
          return smtpStepUpgradeTls(client, op, stEhlo2);
        break;

      case stEhlo2 :
        op->State = stLogin;
        status = smtpWriteCommand(client, op, op->ehlo, op->ehloSize);
        break;

      case stLogin : {
        const char authLogin[] = "AUTH LOGIN\r\n";
        status = smtpStepReadWrite(client, op, 250, authLogin, sizeof(authLogin)-1, stSendLogin);
        break;
      }

      case stSendLogin :
        status = smtpStepReadWrite(client, op, 334, op->login, op->loginSize, stSendPassword);
        break;

      case stSendPassword :
        status = smtpStepReadWrite(client, op, 334, op->password, op->passwordSize, stFrom);
        break;

      case stFrom :
        status = smtpStepReadWrite(client, op, 235, op->from, op->fromSize, stTo);
        break;

      case stTo :
        status = smtpStepReadWrite(client, op, 250, op->to, op->toSize, stSendData);
        break;

      case stSendData : {
        const char data[] = "DATA\r\n";
        status = smtpStepReadWrite(client, op, smtpExpectRcpt, data, sizeof(data)-1, stText);
        break;
      }

      case stText :
        status = smtpStepReadWrite(client, op, 354, op->text, op->textSize, stFinished);
        break;

      default :
        return smtpReadResponse(client, op, 250);
    }
    if (status != aosSuccess)
      return status;
  }
}

static AsyncOpStatus smtpCommandStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  if (op->State == stInitialize) {
    op->State = stFinished;
    AsyncOpStatus status = smtpWriteCommand(client, op, op->Buffer.data, op->Buffer.size);
    if (status != aosSuccess)
      return status;
  }
  return smtpReadResponse(client, op, 0);
}

static void smtpClientDestructor(aioObjectRoot *root)
{
  SMTPClient *client = (SMTPClient*)root;
  if (client->TlsSocket)
    sslSocketDelete(client->TlsSocket);
  else
    deleteAioObject(client->PlainSocket);

  objectFree(&objectPool, client, sizeof(SMTPClient));
}

SMTPClient *smtpClientNew(asyncBase *base, HostAddress localAddress, SmtpServerType type)
{
  // Create and socket first
  socketTy socket = socketCreate(localAddress.family, SOCK_STREAM, IPPROTO_TCP, 1);
  socketReuseAddr(socket);
  if (socketBind(socket, &localAddress) != 0) {
    socketClose(socket);
    return 0;
  }

  SMTPClient *client = (SMTPClient*)objectAlloc(&objectPool,
                                                sizeof(SMTPClient),
                                                16);
  if (!client) {
    socketClose(socket);
    return 0;
  }

  initObjectRoot(&client->root, base, ioObjectUserDefined, smtpClientDestructor);
  client->TlsSocket = 0;
  client->Type = type;
  client->ptr = client->buffer;
  client->end = client->buffer;
  client->Response = 0;
  if (type == smtpServerPlain) {
    client->PlainSocket = newSocketIo(base, socket);
    if (!client->PlainSocket) {
      socketClose(socket);
      objectFree(&objectPool, client, sizeof(SMTPClient));
      return 0;
    }
  } else if (type == smtpServerSmtps) {
    aioObject *plain = newSocketIo(base, socket);
    client->PlainSocket = plain;
    if (plain)
      client->TlsSocket = sslSocketNew(base, plain, 0);
    if (!client->TlsSocket) {
      if (plain)
        deleteAioObject(plain);
      else
        socketClose(socket);
      objectFree(&objectPool, client, sizeof(SMTPClient));
      return 0;
    }
  }

  return client;
}

void smtpClientDelete(SMTPClient *client)
{
  objectDelete(&client->root);
}

int smtpClientGetResultCode(SMTPClient *client)
{
  return client->ResultCode;
}

const char *smtpClientGetResponse(SMTPClient *client)
{
  return client->Response;
}

// Shared tail of every coroutine-style entry point: run the operation, wait
// for its completion and convert the status to the 0/-status convention
static int smtpIoResult(SMTPOp *op)
{
  combinerPushOperation(&op->Root);
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->Root);
  releaseAsyncOp(&op->Root);
  return status == aosSuccess ? 0 : -status;
}

static void smtpLoginPrepare(SMTPOp *op, const char *login, const char *password)
{
  size_t loginOffset = putBase64(&op->Buffer, login, &op->loginSize);
  size_t passwordOffset = putBase64(&op->Buffer, password, &op->passwordSize);
  op->login = (char*)op->Buffer.data + loginOffset;
  op->password = (char*)op->Buffer.data + passwordOffset;
}

void aioSmtpConnect(SMTPClient *client, HostAddress address, uint64_t usTimeout, smtpConnectCb callback, void *arg)
{
  SMTPOp *op = allocSmtpOp(smtpConnectStart, connectFinish, client, SmtpOpConnect, (void*)callback, arg, afNone, usTimeout);
  op->Address = address;
  combinerPushOperation(&op->Root);
}

void aioSmtpStartTls(SMTPClient *client, AsyncFlags flags, uint64_t usTimeout, smtpResponseCb callback, void *arg)
{
  SMTPOp *op = allocSmtpOp(smtpStartTlsStart, commandFinish, client, SmtpOpStartTls, (void*)callback, arg, flags, usTimeout);
  combinerPushOperation(&op->Root);
}

void aioSmtpLogin(SMTPClient *client, const char *login, const char *password, AsyncFlags flags, uint64_t usTimeout, smtpResponseCb callback, void *arg)
{
  SMTPOp *op = allocSmtpOp(smtpLoginStart, commandFinish, client, SmtpOpCommand, (void*)callback, arg, flags, usTimeout);
  smtpLoginPrepare(op, login, password);
  combinerPushOperation(&op->Root);
}

void aioSmtpCommand(SMTPClient *client, const char *command, AsyncFlags flags, uint64_t usTimeout, smtpResponseCb callback, void *arg)
{
  SMTPOp *op = allocSmtpOp(smtpCommandStart, commandFinish, client, SmtpOpCommand, (void*)callback, arg, flags, usTimeout);
  dynamicBufferWriteString(&op->Buffer, command);
  dynamicBufferWriteString(&op->Buffer, "\r\n");
  combinerPushOperation(&op->Root);
}

int ioSmtpConnect(SMTPClient *client, HostAddress address, uint64_t usTimeout)
{
  SMTPOp *op = allocSmtpOp(smtpConnectStart, 0, client, SmtpOpConnect, 0, 0, afCoroutine, usTimeout);
  op->Address = address;
  return smtpIoResult(op);
}

int ioSmtpStartTls(SMTPClient *client, AsyncFlags flags, uint64_t usTimeout)
{
  SMTPOp *op = allocSmtpOp(smtpStartTlsStart, 0, client, SmtpOpStartTls, 0, 0, flags | afCoroutine, usTimeout);
  return smtpIoResult(op);
}

int ioSmtpLogin(SMTPClient *client, const char *login, const char *password, AsyncFlags flags, uint64_t usTimeout)
{
  SMTPOp *op = allocSmtpOp(smtpLoginStart, 0, client, SmtpOpCommand, 0, 0, flags | afCoroutine, usTimeout);
  smtpLoginPrepare(op, login, password);
  return smtpIoResult(op);
}

int ioSmtpCommand(SMTPClient *client, const char *command, AsyncFlags flags, uint64_t usTimeout)
{
  SMTPOp *op = allocSmtpOp(smtpCommandStart, 0, client, SmtpOpCommand, 0, 0, flags | afCoroutine, usTimeout);
  dynamicBufferWriteString(&op->Buffer, command);
  dynamicBufferWriteString(&op->Buffer, "\r\n");
  return smtpIoResult(op);
}

// Builds the whole mail transaction in the operation buffer: base64 login and
// password, EHLO/MAIL From/RCPT To lines and the message itself. Pointers into
// the buffer are resolved only after the last write, when no further growth
// can move the storage.
static void smtpSendMailPrepare(SMTPOp *op,
                                HostAddress smtpServerAddress,
                                int startTls,
                                const char *localHost,
                                const char *login,
                                const char *password,
                                const char *from,
                                const char *to,
                                const char *subject,
                                const char *text)
{
  size_t ehloOffset;
  size_t fromOffset;
  size_t toOffset;
  size_t textOffset;

  size_t loginOffset = putBase64(&op->Buffer, login, &op->loginSize);
  size_t passwordOffset = putBase64(&op->Buffer, password, &op->passwordSize);

  {
    ehloOffset = op->Buffer.offset;
    dynamicBufferWriteString(&op->Buffer, "EHLO ");
    dynamicBufferWriteNoCrlf(&op->Buffer, localHost);
    dynamicBufferWriteString(&op->Buffer, "\r\n");
    op->ehloSize = op->Buffer.offset - ehloOffset;
  }
  {
    fromOffset = op->Buffer.offset;
    dynamicBufferWriteString(&op->Buffer, "MAIL From: <");
    dynamicBufferWriteNoCrlf(&op->Buffer, from);
    dynamicBufferWriteString(&op->Buffer, ">\r\n");
    op->fromSize = op->Buffer.offset - fromOffset;
  }
  {
    toOffset = op->Buffer.offset;
    dynamicBufferWriteString(&op->Buffer, "RCPT To: <");
    dynamicBufferWriteNoCrlf(&op->Buffer, to);
    dynamicBufferWriteString(&op->Buffer, ">\r\n");
    op->toSize = op->Buffer.offset - toOffset;
  }
  {
    textOffset = op->Buffer.offset;
    dynamicBufferWriteString(&op->Buffer, "From: ");
    dynamicBufferWriteNoCrlf(&op->Buffer, from);
    dynamicBufferWriteString(&op->Buffer, "\r\n");

    dynamicBufferWriteString(&op->Buffer, "To: ");
    dynamicBufferWriteNoCrlf(&op->Buffer, to);
    dynamicBufferWriteString(&op->Buffer, "\r\n");

    dynamicBufferWriteString(&op->Buffer, "Subject: ");
    dynamicBufferWriteNoCrlf(&op->Buffer, subject);
    // Empty line separates the header block from the body (RFC 5322)
    dynamicBufferWriteString(&op->Buffer, "\r\n\r\n");

    dynamicBufferWriteMailBody(&op->Buffer, text);
    op->textSize = op->Buffer.offset - textOffset;
  }

  op->Address = smtpServerAddress;
  op->ehlo = (char*)op->Buffer.data + ehloOffset;
  op->login = (char*)op->Buffer.data + loginOffset;
  op->password = (char*)op->Buffer.data + passwordOffset;
  op->from = (char*)op->Buffer.data + fromOffset;
  op->to = (char*)op->Buffer.data + toOffset;
  op->text = (char*)op->Buffer.data + textOffset;
  op->startTls = startTls;
}

void aioSmtpSendMail(SMTPClient *client,
                     HostAddress smtpServerAddress,
                     int startTls,
                     const char *localHost,
                     const char *login,
                     const char *password,
                     const char *from,
                     const char *to,
                     const char *subject,
                     const char *text,
                     AsyncFlags flags,
                     uint64_t usTimeout,
                     smtpResponseCb callback,
                     void *arg)
{
  SMTPOp *op = allocSmtpOp(smtpSendMailStart, commandFinish, client, SmtpOpCommand, (void*)callback, arg, flags, usTimeout);
  smtpSendMailPrepare(op, smtpServerAddress, startTls, localHost, login, password, from, to, subject, text);
  combinerPushOperation(&op->Root);
}

int ioSmtpSendMail(SMTPClient *client,
                   HostAddress smtpServerAddress,
                   int startTls,
                   const char *localHost,
                   const char *login,
                   const char *password,
                   const char *from,
                   const char *to,
                   const char *subject,
                   const char *text,
                   AsyncFlags flags,
                   uint64_t usTimeout)
{
  SMTPOp *op = allocSmtpOp(smtpSendMailStart, 0, client, SmtpOpCommand, 0, 0, flags | afCoroutine, usTimeout);
  smtpSendMailPrepare(op, smtpServerAddress, startTls, localHost, login, password, from, to, subject, text);
  return smtpIoResult(op);
}
