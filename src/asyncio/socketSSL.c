#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include "asyncio/socket.h"
#include "asyncio/socketSSL.h"
#include "asyncioImpl.h"
#include "atomic.h"
#include <openssl/bio.h>
#include <openssl/ssl.h>
#include <string.h>

#define DEFAULT_SSL_READ_BUFFER_SIZE 16384
#define DEFAULT_SSL_WRITE_BUFFER_SIZE 16384

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;
static ConcurrentQueue objectPool;

struct Context {
  aioExecuteProc *StartProc;
  aioFinishProc *FinishProc;
  void *Buffer;
  size_t TransactionSize;
  size_t BytesTransferred;
  ssize_t Result;
};

static inline void fillContext(struct Context *context,
                               aioExecuteProc *startProc,
                               aioFinishProc *finishProc,
                               void *buffer,
                               size_t transactionSize)
{
  context->StartProc = startProc;
  context->FinishProc = finishProc;
  context->Buffer = buffer;
  context->TransactionSize = transactionSize;
  context->BytesTransferred = 0;
  context->Result = -aosPending;
}

typedef enum {
  sslStInitalize = 0,
  sslStProcessing
} SSLSocketStateTy;

typedef enum {
  sslOpConnect = 0,
  sslOpRead,
  sslOpWrite
} SSLOpTy;

__NO_PADDING_BEGIN
typedef struct coroReturnStruct {
  coroutineTy *coroutine;
  AsyncOpStatus status;
  size_t transferred;
} coroReturnStruct;
__NO_PADDING_END

static AsyncOpStatus connectProc(asyncOpRoot *opptr);
static AsyncOpStatus readProc(asyncOpRoot *opptr);
static void sslWriteWriteCb(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg);

static int cancel(asyncOpRoot *opptr)
{
  SSLSocket *S = (SSLSocket*)opptr->object;
  cancelIo((aioObjectRoot*)S->object);
  return 0;
}

static void connectFinish(asyncOpRoot *opptr)
{
  ((sslConnectCb*)opptr->callback)(opGetStatus(opptr), (SSLSocket*)opptr->object, opptr->arg);
}

static void rwFinish(asyncOpRoot *opptr)
{
  SSLOp *op = (SSLOp*)opptr;
  ((sslCb*)opptr->callback)(opGetStatus(opptr), (SSLSocket*)opptr->object, op->bytesTransferred, opptr->arg);
}

static void releaseOp(asyncOpRoot *opptr)
{
  SSLOp *op = (SSLOp*)opptr;
  if (op->internalBuffer) {
    free(op->internalBuffer);
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }
}

static asyncOpRoot *newReadAsyncOp(aioObjectRoot *object,
                                   AsyncFlags flags,
                                   uint64_t usTimeout,
                                   void *callback,
                                   void *arg,
                                   int opCode,
                                   void *contextPtr)
{
  SSLOp *op = 0;
  struct Context *context = (struct Context*)contextPtr;
  if (asyncOpAlloc(object->header.base, sizeof(SSLOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseOp, object, callback, arg, flags, opCode, usTimeout);
  op->buffer = context->Buffer;
  op->transactionSize = context->TransactionSize;
  op->bytesTransferred = 0;
  op->state = sslStInitalize;
  return &op->root;
}

static asyncOpRoot *newWriteAsyncOp(aioObjectRoot *object,
                                    AsyncFlags flags,
                                    uint64_t usTimeout,
                                    void *callback,
                                    void *arg,
                                    int opCode,
                                    void *contextPtr)
{
  SSLOp *op = 0;
  struct Context *context = (struct Context*)contextPtr;
  if (asyncOpAlloc(object->header.base, sizeof(SSLOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseOp, object, callback, arg, flags, opCode, usTimeout);
  if (!(flags & afNoCopy) && context->TransactionSize) {
    if (op->internalBuffer == 0) {
      op->internalBuffer = malloc(context->TransactionSize);
      op->internalBufferSize = context->TransactionSize;
    } else if (op->internalBufferSize < context->TransactionSize) {
      op->internalBufferSize = context->TransactionSize;
      op->internalBuffer = realloc(op->internalBuffer, context->TransactionSize);
    }

    memcpy(op->internalBuffer, context->Buffer, context->TransactionSize);
    op->buffer = op->internalBuffer;
  } else {
    op->buffer = context->Buffer;
  }

  op->transactionSize = context->TransactionSize;
  op->bytesTransferred = 0;
  op->state = sslStInitalize;
  return &op->root;
}

static ssize_t coroutineRwFinish(SSLOp *op, SSLSocket *object)
{
  __UNUSED(object);
  AsyncOpStatus status = opGetStatus(&op->root);
  size_t bytesTransferred = op->bytesTransferred;
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? (ssize_t)bytesTransferred : -(int)status;
}

size_t copyFromOut(SSLSocket *S)
{
  size_t nBytes = BIO_ctrl_pending(S->bioOut);
  if (nBytes > S->sslWriteBufferSize) {
    S->sslWriteBuffer = realloc(S->sslWriteBuffer, nBytes);
    S->sslWriteBufferSize = nBytes;
  }

  // TODO: correct processing >4Gb data blocks
  BIO_read(S->bioOut, S->sslWriteBuffer, (int)nBytes);
  return nBytes;
}

static void sslConnectConnectCb(AsyncOpStatus status, aioObject *object, void *arg)
{
  __UNUSED(object);
  resumeParent((asyncOpRoot*)arg, status);
}

static void sslConnectReadCb(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  SSLOp *op = (SSLOp*)arg;
  SSLSocket *socket = (SSLSocket*)op->root.object;
  // TODO: correct processing >4Gb data blocks
  BIO_write(socket->bioIn, socket->sslReadBuffer, (int)transferred);
  resumeParent((asyncOpRoot*)arg, status);
}

static AsyncOpStatus connectProc(asyncOpRoot *opptr)
{
  SSLOp *op = (SSLOp*)opptr;
  SSLSocket *socket = (SSLSocket*)op->root.object;

  if (op->state == sslStInitalize) {
    op->state = sslStProcessing;
    aioConnect(socket->object, &op->address, 0, sslConnectConnectCb, op);
    return aosPending;
  }

  int connectResult = SSL_connect(socket->ssl);
  int errCode = SSL_get_error(socket->ssl, connectResult);
  if (connectResult == 1) {
    // Successfully connected. In TLS 1.3 the handshake completes on our side
    // right after the final flight (Finished) is queued to bioOut: flush it,
    // or the server never finishes its accept
    size_t finalFlightSize = copyFromOut(socket);
    if (finalFlightSize)
      aioWrite(socket->object, socket->sslWriteBuffer, finalFlightSize, afWaitAll, 0, 0, 0);
    return aosSuccess;
  } else if (errCode == SSL_ERROR_WANT_READ) {
    // Need data exchange
    size_t connectSize = copyFromOut(socket);
    aioWrite(socket->object, socket->sslWriteBuffer, connectSize, afWaitAll, 0, 0, 0);
    aioRead(socket->object, socket->sslReadBuffer, socket->sslReadBufferSize, afNone, 0, sslConnectReadCb, op);
    return aosPending;
  } else {
    return aosUnknownError;
  }
}

static void sslReadReadCb(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  SSLOp *op = (SSLOp*)arg;
  SSLSocket *socket = (SSLSocket*)op->root.object;
  // TODO: correct processing >4Gb data blocks
  if (status == aosSuccess)
    BIO_write(socket->bioIn, socket->sslReadBuffer, (int)transferred);
  resumeParent((asyncOpRoot*)arg, status);
}

static AsyncOpStatus readProc(asyncOpRoot *opptr)
{
  SSLOp *op = (SSLOp*)opptr;
  SSLSocket *socket = (SSLSocket*)op->root.object;

  for (;;) {
    uint8_t *ptr = ((uint8_t*)op->buffer) + op->bytesTransferred;
    size_t size = op->transactionSize-op->bytesTransferred;

    size_t readResult = 0;
    int R;
    // TODO: correct processing >4Gb data blocks
    while ( (R = SSL_read(socket->ssl, ptr, (int)size)) > 0) {
      readResult += (size_t)R;
      ptr += R;
      size -= (size_t)R;
    }

    op->bytesTransferred += readResult;
    if (op->bytesTransferred == op->transactionSize || (op->bytesTransferred && !(op->root.flags & afWaitAll))) {
      return aosSuccess;
    } else {
      // only "want more transport data" may continue the loop: a fatal TLS
      // error is sticky, no amount of new data can revive the stream
      int sslError = SSL_get_error(socket->ssl, R);
      if (sslError != SSL_ERROR_WANT_READ)
        return sslError == SSL_ERROR_ZERO_RETURN ? aosDisconnected : aosUnknownError;

      size_t bytes = 0;
      asyncOpRoot *readOp = implRead(socket->object, socket->sslReadBuffer, socket->sslReadBufferSize, afNone, 0, sslReadReadCb, op, &bytes);
      if (!readOp) {
        BIO_write(socket->bioIn, socket->sslReadBuffer, (int)bytes);
      } else {
        combinerPushOperation(readOp);
        return aosPending;
      }
    }
  }
}

void sslSocketDestructor(aioObjectRoot *root)
{
  SSLSocket *socket = (SSLSocket*)root;
  SSL_free(socket->ssl);
  SSL_CTX_free(socket->sslContext);
  deleteAioObject(socket->object);
  objectFree(&objectPool, socket, sizeof(SSLSocket));
}


SSLSocket *sslSocketNew(asyncBase *base, aioObject *socket)
{
  // The caller always provides the transport socket and thereby chooses the
  // address family, as in the http/btc/zmtp/rlpx modules
  if (!socket)
    return 0;

  SSLSocket *S = (SSLSocket*)objectAlloc(&objectPool, sizeof(SSLSocket), 16);
  if (!S)
    return 0;
  if (!S->sslReadBuffer) {
    S->sslReadBufferSize = DEFAULT_SSL_READ_BUFFER_SIZE;
    S->sslReadBuffer = (uint8_t*)malloc(S->sslReadBufferSize);
    S->sslWriteBufferSize = DEFAULT_SSL_READ_BUFFER_SIZE;
    S->sslWriteBuffer = (uint8_t*)malloc(S->sslWriteBufferSize);
    if (!S->sslReadBuffer || !S->sslWriteBuffer) {
      free(S->sslReadBuffer);
      free(S->sslWriteBuffer);
      S->sslReadBuffer = S->sslWriteBuffer = 0;
      objectFree(&objectPool, S, sizeof(SSLSocket));
      return 0;
    }
  }

#ifdef DEPRECATEDIN_1_1_0
  S->sslContext = SSL_CTX_new (TLS_client_method());
#else
  S->sslContext = SSL_CTX_new (TLS_method());
#endif
  if (!S->sslContext) {
    objectFree(&objectPool, S, sizeof(SSLSocket));
    return 0;
  }
  SSL_CTX_set_verify(S->sslContext, SSL_VERIFY_NONE, NULL);
  S->ssl = SSL_new(S->sslContext);
  S->bioIn = BIO_new(BIO_s_mem());
  S->bioOut = BIO_new(BIO_s_mem());
  if (!S->ssl || !S->bioIn || !S->bioOut) {
    if (S->ssl)
      SSL_free(S->ssl);
    BIO_free(S->bioIn);
    BIO_free(S->bioOut);
    SSL_CTX_free(S->sslContext);
    objectFree(&objectPool, S, sizeof(SSLSocket));
    return 0;
  }
  SSL_set_bio(S->ssl, S->bioIn, S->bioOut);

  initObjectRoot(&S->root, base, ioObjectUserDefined, sslSocketDestructor);
  S->object = socket;
  return S;
}

void sslSocketDelete(SSLSocket *socket)
{
  objectDelete(&socket->root);
}

socketTy sslGetSocket(const SSLSocket *socket)
{
  return aioObjectSocket(socket->object);
}

void aioSslConnect(SSLSocket *socket,
                   const HostAddress *address,
                   const char *tlsextHostName,
                   uint64_t usTimeout,
                   sslConnectCb callback,
                   void *arg)
{
  struct Context context;
  fillContext(&context, connectProc, connectFinish, (void*)(uintptr_t)tlsextHostName, tlsextHostName ? strlen(tlsextHostName)+1 : 0);
  SSLOp *op = (SSLOp*)newWriteAsyncOp(&socket->root, afNone, usTimeout, (void*)callback, arg, sslOpConnect, &context);

  if (address)
    op->address = *address;
  else
    op->state = sslStProcessing;

  if (!__uintptr_atomic_compare_and_swap(&socket->root.initializationOp,
                                         0,
                                         (uintptr_t)&op->root,
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(&op->root, aosUnknownError);
    addToGlobalQueue(&op->root);
    return;
  }

  // The SSL state machine belongs to the initialization owner: after a lost CAS a
  // handshake may be running on it right now, so it must not be touched
  SSL_set_connect_state(socket->ssl);
  if (tlsextHostName)
    SSL_set_tlsext_host_name(socket->ssl, op->buffer);

  combinerPushOperation(&op->root);
}

asyncOpRoot *implSslRead(SSLSocket *socket,
                         void *buffer,
                         size_t size,
                         AsyncFlags flags,
                         uint64_t usTimeout,
                         sslCb callback,
                         void *arg,
                         size_t *bytesTransferred)
{
  size_t sslBytesTransferred = 0;

  for (;;) {
    uint8_t *ptr = ((uint8_t*)buffer) + sslBytesTransferred;
    size_t remaining = size - sslBytesTransferred;
    size_t readResult = 0;
    int R;
    // TODO: correct processing >4Gb data blocks
    while ( (R = SSL_read(socket->ssl, ptr, (int)remaining)) > 0) {
      readResult += (size_t)R;
      ptr += R;
      remaining -= (size_t)R;
    }

    sslBytesTransferred += readResult;
    if (sslBytesTransferred == size || (sslBytesTransferred && !(flags & afWaitAll))) {
      *bytesTransferred = sslBytesTransferred;
      return 0;
    } else {
      // only "want more transport data" may continue the loop: a fatal TLS
      // error is sticky, no amount of new data can revive the stream; report
      // it through an already finished operation
      int sslError = SSL_get_error(socket->ssl, R);
      if (sslError != SSL_ERROR_WANT_READ) {
        struct Context context;
        fillContext(&context, readProc, rwFinish, buffer, size);
        SSLOp *sslOp = (SSLOp*)newReadAsyncOp(&socket->root, flags, usTimeout, (void*)callback, arg, sslOpRead, &context);
        sslOp->bytesTransferred = sslBytesTransferred;
        opForceStatus(&sslOp->root, sslError == SSL_ERROR_ZERO_RETURN ? aosDisconnected : aosUnknownError);
        return &sslOp->root;
      }

      size_t bytes = 0;
      asyncOpRoot *readOp = implRead(socket->object, socket->sslReadBuffer, socket->sslReadBufferSize, afNone, 0, sslReadReadCb, 0, &bytes);
      if (!readOp) {
        BIO_write(socket->bioIn, socket->sslReadBuffer, (int)bytes);
      } else {
        struct Context context;
        fillContext(&context, readProc, rwFinish, buffer, size);
        SSLOp *sslOp = (SSLOp*)newReadAsyncOp(&socket->root, flags | afRunning, usTimeout, (void*)callback, arg, sslOpRead, &context);
        sslOp->bytesTransferred = sslBytesTransferred;
        readOp->arg = sslOp;
        combinerPushOperation(readOp);
        return &sslOp->root;
      }
    }
  }
}

static asyncOpRoot *implSslReadProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  return implSslRead((SSLSocket*)object, context->Buffer, context->TransactionSize, flags, usTimeout, (sslCb*)callback, arg, &context->BytesTransferred);
}

static void makeResult(void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  context->Result = (ssize_t)context->BytesTransferred;
}

static void initOp(asyncOpRoot *op, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  ((SSLOp*)op)->bytesTransferred = context->BytesTransferred;
}

ssize_t aioSslRead(SSLSocket *socket,
                   void *buffer,
                   size_t size,
                   AsyncFlags flags,
                   uint64_t usTimeout,
                   sslCb callback,
                   void *arg)
{
  struct Context context;
  fillContext(&context, readProc, rwFinish, buffer, size);
  runAioOperation(&socket->root, newReadAsyncOp, implSslReadProxy, makeResult, initOp, flags, usTimeout, (void*)callback, arg, sslOpRead, &context);
  return context.Result;
}

void sslWriteWriteCb(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  SSLOp *op = (SSLOp*)arg;
  if (transferred > 0)
    op->bytesTransferred = op->transactionSize;
  resumeParent(&op->root, status);
}

static AsyncOpStatus writeProc(asyncOpRoot *opptr)
{
  SSLOp *op = (SSLOp*)opptr;
  SSLSocket *socket = (SSLSocket*)opptr->object;

  if (op->state == sslStInitalize) {
    size_t bytes = 0;
    op->state = sslStProcessing;
    int writeResult = SSL_write(socket->ssl, op->buffer, (int)op->transactionSize);
    if (writeResult <= 0 && op->transactionSize) {
      // Same as the sync path: nothing entered the TLS stream
      int sslError = SSL_get_error(socket->ssl, writeResult);
      return sslError == SSL_ERROR_ZERO_RETURN ? aosDisconnected : aosUnknownError;
    }
    size_t writeSize = copyFromOut(socket);
    asyncOpRoot *writeOp = implWrite(socket->object, socket->sslWriteBuffer, writeSize, afWaitAll, 0, sslWriteWriteCb, op, &bytes);
    if (writeOp) {
      combinerPushOperation(writeOp);
      return aosPending;
    }
    op->bytesTransferred = op->transactionSize;
    return aosSuccess;
  } else {
    op->bytesTransferred = op->transactionSize;
    return aosSuccess;
  }
}

asyncOpRoot *implSslWrite(SSLSocket *socket,
                          const void *buffer,
                          size_t size,
                          AsyncFlags flags,
                          uint64_t usTimeout,
                          sslCb callback,
                          void *arg)
{
  int writeResult = SSL_write(socket->ssl, buffer, (int)size);
  if (writeResult <= 0 && size) {
    // Mirror of the read path: a fatal TLS error is sticky, and WANT_READ
    // here means the handshake is not complete - either way SSL_write
    // accepted nothing, and silently flushing bioOut would report the
    // payload as sent; report the error through an already finished
    // operation instead
    int sslError = SSL_get_error(socket->ssl, writeResult);
    struct Context context;
    fillContext(&context, writeProc, rwFinish, (void*)(uintptr_t)buffer, size);
    SSLOp *sslOp = (SSLOp*)newWriteAsyncOp(&socket->root, flags, usTimeout, (void*)callback, arg, sslOpWrite, &context);
    opForceStatus(&sslOp->root, sslError == SSL_ERROR_ZERO_RETURN ? aosDisconnected : aosUnknownError);
    return &sslOp->root;
  }

  size_t writeSize = copyFromOut(socket);
  size_t bytes = 0;
  asyncOpRoot *op = implWrite(socket->object, socket->sslWriteBuffer, writeSize, afWaitAll, 0, sslWriteWriteCb, 0, &bytes);
  if (!op) {
    return 0;
  } else {
    struct Context context;
    fillContext(&context, writeProc, rwFinish, (void*)(uintptr_t)buffer, size);
    SSLOp *sslOp = (SSLOp*)newWriteAsyncOp(&socket->root, flags | afRunning, usTimeout, (void*)callback, arg, sslOpWrite, &context);
    sslOp->state = sslStProcessing;
    op->arg = sslOp;
    combinerPushOperation(op);
    return &sslOp->root;
  }
}

static asyncOpRoot *implSslWriteProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  return implSslWrite((SSLSocket*)object, context->Buffer, context->TransactionSize, flags, usTimeout, (sslCb*)callback, arg);
}

ssize_t aioSslWrite(SSLSocket *socket,
                   const void *buffer,
                   size_t size,
                   AsyncFlags flags,
                   uint64_t usTimeout,
                   sslCb callback,
                   void *arg)
{
  struct Context context;
  fillContext(&context, writeProc, rwFinish, (void*)(uintptr_t)buffer, size);
  runAioOperation(&socket->root, newWriteAsyncOp, implSslWriteProxy, makeResult, initOp, flags, usTimeout, (void*)callback, arg, sslOpWrite, &context);
  return context.Result;
}

int ioSslConnect(SSLSocket *socket, const HostAddress *address, const char *tlsextHostName, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, connectProc, 0, (void*)(uintptr_t)tlsextHostName, tlsextHostName ? strlen(tlsextHostName)+1 : 0);
  SSLOp *op = (SSLOp*)newWriteAsyncOp(&socket->root, afCoroutine, usTimeout, 0, 0, sslOpConnect, &context);
  op->address = *address;
  if (!__uintptr_atomic_compare_and_swap(&socket->root.initializationOp,
                                         0,
                                         (uintptr_t)&op->root,
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(&op->root, aosUnknownError);
    addToGlobalQueue(&op->root);
  } else {
    SSL_set_connect_state(socket->ssl);
    if (tlsextHostName)
      SSL_set_tlsext_host_name(socket->ssl, op->buffer);
    combinerPushOperation(&op->root);
  }
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -status;
}

ssize_t ioSslRead(SSLSocket *socket, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, readProc, 0, buffer, size);
  asyncOpRoot *op = runIoOperation(&socket->root, newReadAsyncOp, implSslReadProxy, initOp, flags, usTimeout, sslOpRead, &context);
  return op ? coroutineRwFinish((SSLOp*)op, socket) : (ssize_t)context.BytesTransferred;
}

ssize_t ioSslWrite(SSLSocket *socket, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, writeProc, 0, (void*)(uintptr_t)buffer, size);
  asyncOpRoot *op = runIoOperation(&socket->root, newWriteAsyncOp, implSslWriteProxy, initOp, flags, usTimeout, sslOpWrite, &context);
  return op ? coroutineRwFinish((SSLOp*)op, socket) : (ssize_t)context.BytesTransferred;
}
