#include "asyncio/http.h"

#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include <string.h>

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;
static ConcurrentQueue objectPool;

typedef enum {
  httpOpConnect = 0,
  httpOpRequest
} HttpOpTy;

static AsyncOpStatus httpParseStart(asyncOpRoot *opptr);

static int cancel(asyncOpRoot *opptr)
{
  HTTPClient *client = (HTTPClient*)opptr->object;
  cancelIo(client->isHttps ? (aioObjectRoot*)client->sslSocket : (aioObjectRoot*)client->plainSocket);
  return 0;
}

static void connectFinish(asyncOpRoot *opptr)
{
  ((httpConnectCb*)opptr->callback)(opGetStatus(opptr), (HTTPClient*)opptr->object, opptr->arg);
}

static void requestFinish(asyncOpRoot *opptr)
{
  HTTPClient *client = (HTTPClient*)opptr->object;
  ((httpRequestCb*)opptr->callback)(opGetStatus(opptr), client, opptr->arg);
}

static void httpResumeProc(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  HTTPClient *client = (HTTPClient*)((HTTPOp*)arg)->root.object;
  client->requestBytesSent += transferred;
  resumeParent((asyncOpRoot*)arg, status);
}

static void httpsResumeProc(AsyncOpStatus status, SSLSocket *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  HTTPClient *client = (HTTPClient*)((HTTPOp*)arg)->root.object;
  client->requestBytesSent += transferred;
  resumeParent((asyncOpRoot*)arg, status);
}


static void httpRequestProc(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  asyncOpRoot *opptr = (asyncOpRoot*)arg;
  HTTPClient *client = (HTTPClient*)opptr->object;
  httpSetBuffer(&client->state, client->inBuffer, client->inBufferOffset+transferred);
  resumeParent(opptr, status);
}

static void httpsRequestProc(AsyncOpStatus status, SSLSocket *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  asyncOpRoot *opptr = (asyncOpRoot*)arg;
  HTTPClient *client = (HTTPClient*)opptr->object;
  httpSetBuffer(&client->state, client->inBuffer, client->inBufferOffset+transferred);
  resumeParent(opptr, status);
}

static void httpConnectProc(AsyncOpStatus status, aioObject *object, void *arg)
{
  __UNUSED(object);
  resumeParent((asyncOpRoot*)arg, status);
}

static void httpsConnectProc(AsyncOpStatus status, SSLSocket *object, void *arg)
{
  __UNUSED(object);
  resumeParent((asyncOpRoot*)arg, status);
}

static AsyncOpStatus httpConnectStart(asyncOpRoot *opptr)
{
  HTTPOp *op = (HTTPOp*)opptr;
  HTTPClient *client = (HTTPClient*)opptr->object;
  if (op->state == 0) {
    op->state = 1;
    if (client->isHttps)
      aioSslConnect(client->sslSocket, &op->address, op->dataSize ? (char*)op->internalBuffer : 0, 0, httpsConnectProc, op);
    else
      aioConnect(client->plainSocket, &op->address, 0, httpConnectProc, op);
    return aosPending;
  } else {
    return aosSuccess;
  }
}

static AsyncOpStatus httpParseStart(asyncOpRoot *opptr)
{
  HTTPOp *op = (HTTPOp*)opptr;
  HTTPClient *client = (HTTPClient*)op->root.object;

  if (op->state == 0) {
    client->requestBytesSent = 0;
    httpInit(&client->state);

    HttpComponent component;
    component.type = httpDtInitialize;
    op->parseCallback(&component, op->parseArg);

    size_t bytesTransferred = 0;
    op->state = 1;
    asyncOpRoot *childOp = client->isHttps ?
      implSslWrite(client->sslSocket, op->internalBuffer, op->dataSize, afWaitAll, 0, httpsResumeProc, op) :
      implWrite(client->plainSocket, op->internalBuffer, op->dataSize, afWaitAll, 0, httpResumeProc, op, &bytesTransferred);
    if (childOp) {
      combinerPushOperation(childOp);
      return aosPending;
    }
    // sync completion — callback not invoked, all bytes sent (afWaitAll)
    client->requestBytesSent = op->dataSize;
  }

  for (;;) {
    switch (httpParse(&client->state, op->parseCallback, op->parseArg)) {
      case ParserResultOk : {
        HttpComponent component;
        component.type = httpDtFinalize;
        op->parseCallback(&component, op->parseArg);
        return aosSuccess;  
      }
      case ParserResultNeedMoreData : {
        size_t offset = httpDataRemaining(&client->state);
        if (offset == client->inBufferSize) {
          // a message element larger than the buffer can never complete;
          // reading 0 bytes below would spin this loop forever
          return aosBufferTooSmall;
        }

        // move 'tail' to begin of buffer; the regions overlap when the parser
        // consumed less than half of the buffered data
        if (offset)
          memmove(client->inBuffer, httpDataPtr(&client->state), offset);

        asyncOpRoot *readOp;
        size_t bytesTransferred = 0;
        if (client->isHttps)
          readOp = implSslRead(client->sslSocket,
                               client->inBuffer+offset,
                               client->inBufferSize-offset,
                               afNone,
                               0,
                               httpsRequestProc,
                               op,
                               &bytesTransferred);
        else
          readOp = implRead(client->plainSocket,
                            client->inBuffer+offset,
                            client->inBufferSize-offset,
                            afNone,
                            0,
                            httpRequestProc,
                            op,
                            &bytesTransferred);

        client->inBufferOffset = offset;
        if (readOp) {
          combinerPushOperation(readOp);
          return aosPending;
        } else {
          httpSetBuffer(&client->state, client->inBuffer, client->inBufferOffset+bytesTransferred);
        }
        break;
      }

      case ParserResultCancelled :
      case ParserResultError :
        return aosUnknownError;
    }
  }
}

static void releaseProc(asyncOpRoot *opptr)
{
  HTTPOp *op = (HTTPOp*)opptr;
  if (op->internalBuffer) {
    free(op->internalBuffer);
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }
}

static HTTPOp *allocHttpOp(aioExecuteProc executeProc,
                           aioFinishProc finishProc,
                           HTTPClient *client,
                           int type,
                           httpParseCb parseCallback,
                           void *parseArg,
                           void *callback,
                           void *arg,
                           AsyncFlags flags,
                           uint64_t timeout)
{
  HTTPOp *op = 0;
  if (asyncOpAlloc(client->root.header.base, sizeof(HTTPOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = 0;
    op->dataSize = 0;
    op->internalBufferSize = 0;
  }

  initAsyncOpRoot(&op->root, executeProc, cancel, finishProc, releaseProc, &client->root, callback, arg, flags, type, timeout);
  op->parseCallback = parseCallback;
  op->parseArg = parseArg;
  op->state = 0;
  return op;
}

void httpParseDefaultInit(HTTPParseDefaultContext *context)
{
  dynamicBufferInit(&context->buffer, 65536);
}

void httpParseDefault(HttpComponent *component, void *arg)
{
  HTTPParseDefaultContext *context = (HTTPParseDefaultContext*)arg;
  switch (component->type) {
    case httpDtInitialize : {
      context->contentTypeOffset = 0;
      context->bodyOffset = 0;
      context->contentType.data = 0;
      context->contentType.size = 0;
      context->body.data = 0;
      context->body.size = 0;
      context->resultCode = 0;
      dynamicBufferClear(&context->buffer);
      break;
    }

    case httpDtStartLine : {
      context->resultCode = component->startLine.code;
      break;
    }

    case httpDtHeaderEntry : {
      switch (component->header.entryType) {
        case hhContentType : {
          context->contentTypeOffset = context->buffer.offset;
          char *out = (char*)dynamicBufferAlloc(&context->buffer, component->header.stringValue.size+1);
          memcpy(out, component->header.stringValue.data, component->header.stringValue.size);
          out[component->header.stringValue.size] = 0;
          context->contentType.size = component->header.stringValue.size;
          break;
        }
      }

      break;
    }

    case httpDtData :
    case httpDtDataFragment : {
      if (context->bodyOffset == 0)
          context->bodyOffset = context->buffer.offset;

      char *out = (char*)dynamicBufferAlloc(&context->buffer, component->data.size);
      memcpy(out, component->data.data, component->data.size);
      break;
    }

    case httpDtFinalize : {
      *(char*)dynamicBufferAlloc(&context->buffer, 1) = 0;
      if (context->contentTypeOffset)
        context->contentType.data = (char*)context->buffer.data + context->contentTypeOffset;
      context->body.data = (char*)context->buffer.data + context->bodyOffset;
      context->body.size = context->buffer.size - context->bodyOffset - 1;
      break;
    }
  }
}

static void httpClientDestructor(aioObjectRoot *root)
{
  HTTPClient *client = (HTTPClient*)root;
  if (client->isHttps)
    sslSocketDelete(client->sslSocket);
  else
    deleteAioObject(client->plainSocket);
  objectFree(&objectPool, client, sizeof(HTTPClient));
}

HTTPClient *httpClientNew(asyncBase *base, aioObject *socket)
{
  if (!socket)
    return 0;
  HTTPClient *client = (HTTPClient*)objectAlloc(&objectPool,
                                                sizeof(HTTPClient),
                                                16);
  if (!client)
    return 0;
  if (!client->inBuffer) {
    client->inBuffer = (uint8_t*)malloc(65536);
    if (!client->inBuffer) {
      objectFree(&objectPool, client, sizeof(HTTPClient));
      return 0;
    }
    client->inBufferSize = 65536;
  }

  initObjectRoot(&client->root, base, ioObjectUserDefined, httpClientDestructor);
  client->isHttps = 0;
  client->inBufferOffset = 0;
  client->requestBytesSent = 0;
  httpSetBuffer(&client->state, client->inBuffer, 0);
  client->plainSocket = socket;
  return client;
}

HTTPClient *httpsClientNew(asyncBase *base, SSLSocket *socket)
{
  if (!socket)
    return 0;
  HTTPClient *client = (HTTPClient*)objectAlloc(&objectPool,
                                                sizeof(HTTPClient),
                                                16);
  if (!client)
    return 0;
  if (!client->inBuffer) {
    client->inBuffer = (uint8_t*)malloc(65536);
    if (!client->inBuffer) {
      objectFree(&objectPool, client, sizeof(HTTPClient));
      return 0;
    }
    client->inBufferSize = 65536;
  }

  initObjectRoot(&client->root, base, ioObjectUserDefined, httpClientDestructor);
  client->isHttps = 1;
  client->inBufferOffset = 0;
  client->requestBytesSent = 0;
  httpSetBuffer(&client->state, client->inBuffer, 0);
  client->sslSocket = socket;
  return client;
}

void httpClientDelete(HTTPClient *client)
{
  objectDelete(&client->root);
}

void aioHttpConnect(HTTPClient *client,
                    const HostAddress *address,
                    const char *tlsextHostName,
                    uint64_t usTimeout,
                    httpConnectCb callback,
                    void *arg)
{
  HTTPOp *op = allocHttpOp(httpConnectStart, connectFinish, client, httpOpConnect, 0, 0, (void*)callback, arg, afNone, usTimeout);
  op->address = *address;

  if (tlsextHostName) {
    size_t tlsextHostNameSize = strlen(tlsextHostName) + 1;
    if (op->internalBufferSize < tlsextHostNameSize) {
      op->internalBuffer = realloc(op->internalBuffer, tlsextHostNameSize);
      op->internalBufferSize = tlsextHostNameSize;
    }

    op->dataSize = tlsextHostNameSize;
    memcpy(op->internalBuffer, tlsextHostName, tlsextHostNameSize);
  } else {
    op->dataSize = 0;
  }

  combinerPushOperation(&op->root);
}

void aioHttpRequest(HTTPClient *client,
                    const char *request,
                    size_t requestSize,
                    uint64_t usTimeout,
                    httpParseCb parseCallback,
                    void *parseArg,
                    httpRequestCb callback,
                    void *arg)
{
  HTTPOp *op = allocHttpOp(httpParseStart, requestFinish, client, httpOpConnect, parseCallback, parseArg, (void*)callback, arg, afNone, usTimeout);
  if (op->internalBufferSize >= requestSize) {
    op->dataSize = requestSize;
    memcpy(op->internalBuffer, request, requestSize);
  } else {
    op->internalBuffer = realloc(op->internalBuffer, requestSize);
    op->internalBufferSize = requestSize;
    op->dataSize = requestSize;
    memcpy(op->internalBuffer, request, requestSize);
  }

  combinerPushOperation(&op->root);
}


int ioHttpConnect(HTTPClient *client, const HostAddress *address, const char *tlsextHostName, uint64_t usTimeout)
{
  HTTPOp *op = allocHttpOp(httpConnectStart, 0, client, httpOpConnect, 0, 0, 0, 0, afCoroutine, usTimeout);
  op->address = *address;

  if (tlsextHostName) {
    size_t tlsextHostNameSize = strlen(tlsextHostName) + 1;
    if (op->internalBufferSize < tlsextHostNameSize) {
      op->internalBuffer = realloc(op->internalBuffer, tlsextHostNameSize);
      op->internalBufferSize = tlsextHostNameSize;
    }

    op->dataSize = tlsextHostNameSize;
    memcpy(op->internalBuffer, tlsextHostName, tlsextHostNameSize);
  } else {
    op->dataSize = 0;
  }

  combinerPushOperation(&op->root);
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -(int)status;
}

AsyncOpStatus ioHttpRequest(HTTPClient *client,
                            const char *request,
                            size_t requestSize,
                            uint64_t usTimeout,
                            httpParseCb parseCallback,
                            void *parseArg)
{
  HTTPOp *op = allocHttpOp(httpParseStart, 0, client, httpOpConnect, parseCallback, parseArg, 0, 0, afCoroutine, usTimeout);
  if (op->internalBufferSize >= requestSize) {
    op->dataSize = requestSize;
    memcpy(op->internalBuffer, request, requestSize);
  } else {
    op->internalBuffer = realloc(op->internalBuffer, requestSize);
    op->internalBufferSize = requestSize;
    op->dataSize = requestSize;
    memcpy(op->internalBuffer, request, requestSize);
  }

  combinerPushOperation(&op->root);
  coroutineYield();

  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status;
}
