#include "asyncio/smtp.h"
#include "asyncio/asyncio.h"
#include "asyncio/base64.h"
#include "asyncio/dynamicBuffer.h"
#include "asyncio/socketSSL.h"
#include "asyncio/socket.h"
#include <memory.h>

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

static int cancel(asyncOpRoot *opptr)
{
  SMTPClient *client = (SMTPClient*)opptr->object;
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

static void smtpRead(SMTPClient *client, SMTPOp *op);

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

static void smtpParse(AsyncOpStatus status, SMTPClient *client, size_t bytesRead, SMTPOp *op)
{
  char firstReplyCode[4];
  if (status != aosSuccess) {
    resumeParent(&op->Root, status);
    return;
  }

  client->end += bytesRead;
  if (client->end - client->buffer < 5) {
    smtpRead(client, op);
    return;
  }

  // Check begin of response (must be 3-digit number)
  {
    if (!isDigits(client->buffer, 3)) {
      resumeParent(&op->Root, (AsyncOpStatus)smtpInvalidFormat);
      return;
    }

    firstReplyCode[0] = client->buffer[0];
    firstReplyCode[1] = client->buffer[1];
    firstReplyCode[2] = client->buffer[2];
    firstReplyCode[3] = 0;
    client->ResultCode = atoi(firstReplyCode);
  }

  if (client->buffer[3] == '-') {
    // Multiline response detected
    // Find end of message
    size_t linesCount = 1;
    char *lf = memchr(&client->buffer[4], '\n', client->end - client->buffer - 4);
    if (!lf) {
      smtpRead(client, op);
      return;
    }

    const char *p = lf + 1;
    for (;;) {
      // Get end of line
      lf = memchr(p, '\n', client->end - p);
      if (!lf) {
        smtpRead(client, op);
        return;
      }

      linesCount++;
      if (client->end - p < 3 || memcmp(p, firstReplyCode, 3) != 0) {
        resumeParent(&op->Root, (AsyncOpStatus)smtpInvalidFormat);
        return;
      }

      if (p[3] == ' ') {
        // Last line in multiline answer
        break;
      }

      p = lf + 1;
    }

    // Squash multiline test to single C-string
    p = client->buffer;
    char *out = client->buffer;
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
    client->Response = client->buffer;
    resumeParent(&op->Root, (client->ResultCode >= 200 && client->ResultCode <= 399) ? aosSuccess : (AsyncOpStatus)smtpError);
  } else {
    // Orher response parse
    // Find '\n'
    char *lf = memchr(&client->buffer[4], '\n', client->end - client->buffer - 4);
    if (lf) {
      client->Response = client->buffer + 4;
      *(lf-1) = 0;
      client->ptr = lf+1;
      resumeParent(&op->Root, (client->ResultCode >= 200 && client->ResultCode <= 399) ? aosSuccess : (AsyncOpStatus)smtpError);
    } else {
      smtpRead(client, op);
    }
  }
}

static void smtpReadCb(AsyncOpStatus status, aioObject *object, size_t bytesRead, void *arg)
{
  __UNUSED(object);
  SMTPOp *op = (SMTPOp*)arg;
  smtpParse(status, (SMTPClient*)op->Root.object, bytesRead, op);
}

static void smtpSslReadCb(AsyncOpStatus status, SSLSocket *object, size_t bytesRead, void *arg)
{
  __UNUSED(object);
  SMTPOp *op = (SMTPOp*)arg;
  smtpParse(status, (SMTPClient*)op->Root.object, bytesRead, op);
}

static void smtpRead(SMTPClient *client, SMTPOp *op)
{
  size_t remaining = client->end - client->ptr;
  if (remaining) {
    memmove(client->buffer, client->ptr, remaining);
  }
  client->ptr = client->buffer;
  client->end = client->buffer + remaining;

  client->Response = 0;
  if (client->TlsSocket)
    aioSslRead(client->TlsSocket, client->end, sizeof(client->buffer) - remaining, afNone, 0, smtpSslReadCb, op);
  else
    aioRead(client->PlainSocket, client->end, sizeof(client->buffer) - remaining, afNone, 0, smtpReadCb, op);
}

static void smtpWrite(SMTPClient *client, const void *data, size_t size)
{
  if (client->TlsSocket)
    aioSslWrite(client->TlsSocket, data, size, afWaitAll, 0, 0, 0);
  else
    aioWrite(client->PlainSocket, data, size, afWaitAll, 0, 0, 0);
}

// One protocol exchange: advance the state machine, send a command and post a
// read for the server response
static AsyncOpStatus smtpStepWriteRead(SMTPClient *client, SMTPOp *op, const void *data, size_t size, int nextState)
{
  op->State = nextState;
  smtpWrite(client, data, size);
  smtpRead(client, op);
  return aosPending;
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
  if (op->State == stInitialize) {
    return smtpStepConnect(client, op);
  } else if (op->State == stReadGreeting) {
    op->State = stFinished;
    smtpRead(client, op);
    return aosPending;
  } else {
    return aosSuccess;
  }
}

static AsyncOpStatus smtpStartTlsStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  if (op->State == stInitialize) {
    const char startTls[] = "STARTTLS\r\n";
    return smtpStepWriteRead(client, op, startTls, sizeof(startTls)-1, stStartTls);
  } else if (op->State == stStartTls) {
    return smtpStepUpgradeTls(client, op, stFinished);
  } else {
    return aosSuccess;
  }
}

static AsyncOpStatus smtpLoginStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  if (op->State == stInitialize) {
    const char authLogin[] = "AUTH LOGIN\r\n";
    return smtpStepWriteRead(client, op, authLogin, sizeof(authLogin)-1, stSendLogin);
  } else if (op->State == stSendLogin) {
    return smtpStepWriteRead(client, op, op->login, op->loginSize, stSendPassword);
  } else if (op->State == stSendPassword) {
    return smtpStepWriteRead(client, op, op->password, op->passwordSize, stFinished);
  } else {
    return aosSuccess;
  }
}

static AsyncOpStatus smtpSendMailStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  switch (op->State) {
    case stInitialize :
      return smtpStepConnect(client, op);

    case stReadGreeting : {
      op->State = stEhlo1;
      smtpRead(client, op);
      return aosPending;
    }

    case stEhlo1 :
      return smtpStepWriteRead(client, op, op->ehlo, op->ehloSize, op->startTls ? stSendStartTls : stLogin);

    case stSendStartTls : {
      const char startTls[] = "STARTTLS\r\n";
      return smtpStepWriteRead(client, op, startTls, sizeof(startTls)-1, stStartTls);
    }

    case stStartTls :
      return smtpStepUpgradeTls(client, op, stEhlo2);

    case stEhlo2 :
      return smtpStepWriteRead(client, op, op->ehlo, op->ehloSize, stLogin);

    case stLogin : {
      const char authLogin[] = "AUTH LOGIN\r\n";
      return smtpStepWriteRead(client, op, authLogin, sizeof(authLogin)-1, stSendLogin);
    }

    case stSendLogin :
      return smtpStepWriteRead(client, op, op->login, op->loginSize, stSendPassword);

    case stSendPassword :
      return smtpStepWriteRead(client, op, op->password, op->passwordSize, stFrom);

    case stFrom :
      return smtpStepWriteRead(client, op, op->from, op->fromSize, stTo);

    case stTo :
      return smtpStepWriteRead(client, op, op->to, op->toSize, stSendData);

    case stSendData : {
      const char data[] = "DATA\r\n";
      return smtpStepWriteRead(client, op, data, sizeof(data)-1, stText);
    }

    case stText :
      return smtpStepWriteRead(client, op, op->text, op->textSize, stFinished);

    default :
      return aosSuccess;
  }
}

static AsyncOpStatus smtpCommandStart(asyncOpRoot *opptr)
{
  SMTPOp *op = (SMTPOp*)opptr;
  SMTPClient *client = (SMTPClient*)opptr->object;
  if (op->State == stInitialize) {
    return smtpStepWriteRead(client, op, op->Buffer.data, op->Buffer.size, stFinished);
  } else {
    return aosSuccess;
  }
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
    dynamicBufferWriteString(&op->Buffer, localHost);
    dynamicBufferWriteString(&op->Buffer, "\r\n");
    op->ehloSize = op->Buffer.offset - ehloOffset;
  }
  {
    fromOffset = op->Buffer.offset;
    dynamicBufferWriteString(&op->Buffer, "MAIL From: <");
    dynamicBufferWriteString(&op->Buffer, from);
    dynamicBufferWriteString(&op->Buffer, ">\r\n");
    op->fromSize = op->Buffer.offset - fromOffset;
  }
  {
    toOffset = op->Buffer.offset;
    dynamicBufferWriteString(&op->Buffer, "RCPT To: <");
    dynamicBufferWriteString(&op->Buffer, to);
    dynamicBufferWriteString(&op->Buffer, ">\r\n");
    op->toSize = op->Buffer.offset - toOffset;
  }
  {
    textOffset = op->Buffer.offset;
    dynamicBufferWriteString(&op->Buffer, "From: ");
    dynamicBufferWriteString(&op->Buffer, from);
    dynamicBufferWriteString(&op->Buffer, "\r\n");

    dynamicBufferWriteString(&op->Buffer, "To: ");
    dynamicBufferWriteString(&op->Buffer, to);
    dynamicBufferWriteString(&op->Buffer, "\r\n");

    dynamicBufferWriteString(&op->Buffer, "Subject: ");
    dynamicBufferWriteString(&op->Buffer, subject);
    dynamicBufferWriteString(&op->Buffer, "\r\n");

    dynamicBufferWriteString(&op->Buffer, text);
    dynamicBufferWriteString(&op->Buffer, "\r\n.\r\n");
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
