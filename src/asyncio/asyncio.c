#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include "asyncio/device.h"
#include "asyncio/socket.h"
#include "asyncioImpl.h"
#include "atomic.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;
static ConcurrentQueue eventPool;

#ifdef OS_WINDOWS
asyncBase *iocpNewAsyncBase();
#endif
#ifdef OS_LINUX
asyncBase *epollNewAsyncBase(void);
#endif
#if defined(OS_DARWIN) || defined (OS_FREEBSD)
asyncBase *kqueueNewAsyncBase(void);
#endif

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

static void connectFinish(asyncOpRoot* opptr)
{
  ((aioConnectCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, opptr->arg);
}

static void acceptFinish(asyncOpRoot* opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  ((aioAcceptCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, op->host, op->acceptSocket, opptr->arg);
}

static void rwFinish(asyncOpRoot* opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  ((aioCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, op->bytesTransferred, opptr->arg);
}

static void readMsgFinish(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  ((aioReadMsgCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, op->host, op->bytesTransferred, opptr->arg);
}

static void eventFinish(asyncOpRoot *root)
{
  if (root->callback)
    ((aioEventCb*)root->callback)((aioUserEvent*)root, root->arg);
  else
    ((aioUserEvent*)root)->pendingActivations++;
}

static void releaseOp(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  if (op->internalBuffer) {
    free(op->internalBuffer);
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }
}

static asyncOpRoot *newAsyncOp(aioObjectRoot *object,
                               AsyncFlags flags,
                               uint64_t usTimeout,
                               void *callback,
                               void *arg,
                               int opCode,
                               void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  asyncOp *op = (asyncOp*)object->base->methodImpl.newAsyncOp(object->base, flags & afRealtime, &opPool, &opTimerPool);
  initAsyncOpRoot(&op->root, context->StartProc, object->base->methodImpl.cancelAsyncOp, context->FinishProc, releaseOp, object, callback, arg, flags, opCode, usTimeout);

  op->state = 0;
  op->transactionSize = context->TransactionSize;
  op->bytesTransferred = 0;
  if (context->TransactionSize && (opCode & OPCODE_WRITE) && !(flags & afNoCopy)) {
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

  return &op->root;
}

static void coroutineEventCb(aioObject *event, void *arg)
{
  __UNUSED(event);
  coroutineTy *coroutine = (coroutineTy*)arg;
  assert(coroutineIsMain() && "no main coroutine!\n");
  coroutineCall(coroutine);
}

static ssize_t coroutineRwFinish(asyncOp *op, aioObject *object)
{
  __UNUSED(object);
  AsyncOpStatus status = opGetStatus(&op->root);
  size_t bytesTransferred = op->bytesTransferred;
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? (ssize_t)bytesTransferred : -(int)status;
}

socketTy aioObjectSocket(aioObject *object)
{
  return object->hSocket;
}

iodevTy aioObjectDevice(aioObject *object)
{
  return object->hDevice;
}

aioObjectRoot *aioObjectHandle(aioObject *object)
{
  return &object->root;
}

asyncBase *createAsyncBase(AsyncMethod method)
{
  asyncBase *base = 0;
  switch (method) {
#if defined(OS_WINDOWS)
    case amIOCP :
      base = iocpNewAsyncBase();
      break;
#elif defined(OS_LINUX)
    case amEPoll :
      base = epollNewAsyncBase();
      break;
#elif defined(OS_DARWIN) || defined(OS_FREEBSD)
   case amKQueue :
      base = kqueueNewAsyncBase();
     break;
#endif
    case amOSDefault :
    default:
#if defined(OS_WINDOWS)
      base = iocpNewAsyncBase();
#elif defined(OS_LINUX)
      base = epollNewAsyncBase();
#elif defined(OS_DARWIN) || defined(OS_FREEBSD)
      base = kqueueNewAsyncBase();
#else
#error "Unsupported OS: no I/O multiplexor backend available"
#endif
      break;
  }

#ifndef NDEBUG
  base->opsCount = 0;
#endif
  pageMapInit(&base->timerMap);
  memset(&base->globalQueue, 0, sizeof(base->globalQueue));
  base->timerMapLock = 0;
  base->lastCheckPoint = time(0);
  base->messageLoopThreadCounter = 0;
  return base;
}

void asyncLoop(asyncBase *base)
{
  base->methodImpl.nextFinishedOperation(base);
}


void postQuitOperation(asyncBase *base)
{
  base->methodImpl.postEmptyOperation(base);
}

void setSocketBuffer(aioObject *socket, size_t bufferSize)
{
  if (bufferSize > socket->buffer.totalSize) {
    socket->buffer.ptr= realloc(socket->buffer.ptr, bufferSize);
    socket->buffer.totalSize = bufferSize;
  }
}

aioUserEvent *newUserEvent(asyncBase *base, int isSemaphore, aioEventCb callback, void *arg)
{
  // TODO: use malloc allocator for aioUserEvent
  aioUserEvent *event = 0;
  asyncOpAlloc(base, sizeof(aioUserEvent), 1, 0, &eventPool, (asyncOpRoot**)&event);
  event->root.opCode = actUserEvent;
  event->root.finishMethod = eventFinish;
  event->root.callback = (void*)callback;
  event->root.releaseMethod = 0;
  event->root.arg = arg;
  event->root.tag = ((opGetGeneration(&event->root)+1) << TAG_STATUS_SIZE) | aosPending;
  event->base = base;
  event->isSemaphore = isSemaphore;
  event->counter = 0;
  event->tag = 1;
  event->pendingActivations = 0;
  event->destructorCb = 0;
  event->destructorCbArg = 0;
  return event;
}


aioObject *newSocketIo(asyncBase *base, socketTy hSocket)
{
  return base->methodImpl.newAioObject(base, ioObjectSocket, &hSocket);
}

aioObject *newDeviceIo(asyncBase *base, iodevTy hDevice)
{
  return base->methodImpl.newAioObject(base, ioObjectDevice, &hDevice);
}

void deleteAioObject(aioObject *object)
{
  objectDelete(&object->root);
}

asyncBase *aioGetBase(aioObject *object)
{
  return object->root.base;
}

void userEventStartTimer(aioUserEvent *event, uint64_t usTimeout, int counter)
{
  event->counter = counter;
  event->root.timeout = usTimeout;
  event->base->methodImpl.startTimer(&event->root);
}


void userEventStopTimer(aioUserEvent *event)
{
  event->counter = 0;
  event->base->methodImpl.stopTimer(&event->root);
}

void userEventActivate(aioUserEvent *event)
{
  if (eventTryActivate(event))
    event->base->methodImpl.activate(event);
}

void deleteUserEvent(aioUserEvent *event)
{
  event->base->methodImpl.stopTimer(&event->root);
  eventDecrementReference(event, 1 - TAG_EVENT_DELETE);
}

asyncOpRoot *implRead(aioObject *object,
                      void *buffer,
                      size_t size,
                      AsyncFlags flags,
                      uint64_t usTimeout,
                      aioCb callback,
                      void *arg,
                      size_t *bytesTransferred)
{
  *bytesTransferred = 0;
  struct ioBuffer *sb = &object->buffer;
#ifdef OS_WINDOWS
  AsyncFlags extraFlags = afNone;
#else
  AsyncFlags extraFlags = afRunning;
#endif

  if (copyFromBuffer(buffer, bytesTransferred, sb, size))
    return 0;

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.read, rwFinish, buffer, size);
  if (size < sb->totalSize) {
    size_t bytes;
    while (*bytesTransferred <= size) {
      int result = object->root.type == ioObjectSocket ?
        socketSyncRead(object->hSocket, sb->ptr, sb->totalSize, 0, &bytes) :
        deviceSyncRead(object->hDevice, sb->ptr, sb->totalSize, 0, &bytes);
      if (result) {
        sb->dataSize = bytes;
        if (copyFromBuffer(buffer, bytesTransferred, sb, size) || !(flags & afWaitAll))
          break;
      } else {
        asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | extraFlags, usTimeout, (void*)callback, arg, actRead, &context);
        op->bytesTransferred = *bytesTransferred;
        return &op->root;
      }
    }

    return 0;
  } else {
    size_t bytes = 0;
    int result = object->root.type == ioObjectSocket ?
      socketSyncRead(object->hSocket, (uint8_t*)buffer+*bytesTransferred, size-*bytesTransferred, flags & afWaitAll, &bytes) :
      deviceSyncRead(object->hDevice, (uint8_t*)buffer+*bytesTransferred, size-*bytesTransferred, flags & afWaitAll, &bytes);
    *bytesTransferred += bytes;
    if (result) {
      return 0;
    } else {
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | extraFlags, usTimeout, (void*)callback, arg, actRead, &context);
      op->bytesTransferred = *bytesTransferred;
      return &op->root;
    }
  }
}

void implReadModify(asyncOpRoot *opptr, void *buffer, size_t size)
{
  asyncOp *op = (asyncOp*)opptr;
  op->buffer = buffer;
  op->transactionSize = size;
}

asyncOpRoot *implWrite(aioObject *object,
                       const void *buffer,
                       size_t size,
                       AsyncFlags flags,
                       uint64_t usTimeout,
                       aioCb callback,
                       void *arg,
                       size_t *bytesTransferred)
{
#ifdef OS_WINDOWS
  AsyncFlags extraFlags = afNone;
#else
  AsyncFlags extraFlags = afRunning;
#endif
  size_t bytes = 0;
  int result = object->root.type == ioObjectSocket ?
    socketSyncWrite(object->hSocket, buffer, size, flags & afWaitAll, &bytes) :
    deviceSyncWrite(object->hDevice, buffer, size, flags & afWaitAll, &bytes);
  if (result) {
    *bytesTransferred = bytes;
    return 0;
  } else {
    struct Context context;
    fillContext(&context, object->root.base->methodImpl.write, rwFinish, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | extraFlags, usTimeout, (void*)callback, arg, actWrite, &context);
    op->bytesTransferred = bytes;
    return &op->root;
  }
}

static asyncOpRoot *implReadProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  return implRead((aioObject*)object, context->Buffer, context->TransactionSize, flags, usTimeout, (aioCb*)callback, arg, &context->BytesTransferred);
}

static asyncOpRoot *implWriteProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  return implWrite((aioObject*)object, context->Buffer, context->TransactionSize, flags, usTimeout, (aioCb*)callback, arg, &context->BytesTransferred);
}

void aioConnect(aioObject *object,
                const HostAddress *address,
                uint64_t usTimeout,
                aioConnectCb callback,
                void *arg)
{
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.connect, connectFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, afNone, usTimeout, (void*)callback, arg, actConnect, &context);
  op->host = *address;
  combinerPushOperation(&op->root, aaStart);
}


void aioAccept(aioObject *object,
               uint64_t usTimeout,
               aioAcceptCb callback,
               void *arg)
{
#ifdef OS_WINDOWS
  AsyncFlags flags = afNone;
#else
  AsyncFlags flags = afRunning;
#endif
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.accept, acceptFinish, 0, 0);
  asyncOpRoot *op = newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actAccept, &context);
  combinerPushOperation(op, aaStart);
}

static void makeResult(void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  context->Result = (ssize_t)context->BytesTransferred;
}

static void initOp(asyncOpRoot *op, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  ((asyncOp*)op)->bytesTransferred = context->BytesTransferred;
}

ssize_t aioRead(aioObject *object,
                void *buffer,
                size_t size,
                AsyncFlags flags,
                uint64_t usTimeout,
                aioCb callback,
                void *arg)
{
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.read, rwFinish, buffer, size);
  runAioOperation(&object->root, newAsyncOp, implReadProxy, makeResult, initOp, flags, usTimeout, (void*)callback, arg, actRead, &context);
  return context.Result;
}

ssize_t aioWrite(aioObject *object,
                 const void *buffer,
                 size_t size,
                 AsyncFlags flags,
                 uint64_t usTimeout,
                 aioCb callback,
                 void *arg)
{
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.write, rwFinish, (void*)((uintptr_t)buffer), size);
  runAioOperation(&object->root, newAsyncOp, implWriteProxy, makeResult, initOp, flags, usTimeout, (void*)callback, arg, actWrite, &context);
  return context.Result;
}

ssize_t aioReadMsg(aioObject *object,
                   void *buffer,
                   size_t size,
                   AsyncFlags flags,
                   uint64_t usTimeout,
                   aioReadMsgCb callback,
                   void *arg)
{
  struct sockaddr_in source;
  socketLenTy addrlen = sizeof(source);
#ifdef OS_WINDOWS
  ssize_t result = recvfrom(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)&source, &addrlen);
#else
  ssize_t result = recvfrom(object->hSocket, buffer, size, 0, (struct sockaddr*)&source, &addrlen);
#endif

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.readMsg, readMsgFinish, buffer, size);
  if (result >= 0) {
    // Data received synchronously
    HostAddress host;
    host.family = 0;
    host.ipv4 = source.sin_addr.s_addr;
    host.port = source.sin_port;
    currentFinishedSync++;
    if (++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION && (callback == 0 || flags & afActiveOnce)) {
      return result;
    } else {
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
      op->bytesTransferred = (size_t)result;
      op->host = host;
      opForceStatus(&op->root, aosSuccess);
      addToGlobalQueue(&op->root);
    }
  } else {
    asyncOpRoot *op = newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
    combinerPushOperation(op, aaStart);
  }

  return -(ssize_t)aosPending;
}



ssize_t aioWriteMsg(aioObject *object,
                    const HostAddress *address,
                    const void *buffer,
                    size_t size,
                    AsyncFlags flags,
                    uint64_t usTimeout,
                    aioCb callback,
                    void *arg)
{
  // Datagram socket can be accessed by multiple threads without lock
  struct sockaddr_in remoteAddress;
  remoteAddress.sin_family = address->family;
  remoteAddress.sin_addr.s_addr = address->ipv4;
  remoteAddress.sin_port = address->port;
#ifdef OS_WINDOWS
  ssize_t result = sendto(object->hSocket, buffer, (int)size, 0, (struct sockaddr *)&remoteAddress, sizeof(remoteAddress));
#else
  ssize_t result = sendto(object->hSocket, buffer, size, 0, (struct sockaddr *)&remoteAddress, sizeof(remoteAddress));
#endif

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.writeMsg, rwFinish, (void*)((uintptr_t)buffer), size);
  if (result >= 0) {
    currentFinishedSync++;
    if (++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION && (callback == 0 || flags & afActiveOnce)) {
      return result;
    } else {
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actWriteMsg, &context);
      op->bytesTransferred = (size_t)result;
      opForceStatus(&op->root, aosSuccess);
      addToGlobalQueue(&op->root);
    }
  } else {
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actWriteMsg, &context);
    op->host = *address;
    combinerPushOperation(&op->root, aaStart);
  }

  return -(ssize_t)aosPending;
}


int ioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.connect, connectFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, afCoroutine, usTimeout, 0, 0, actConnect, &context);
  op->host = *address;
  combinerPushOperation(&op->root, aaStart);
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -status;
}


socketTy ioAccept(aioObject *object, uint64_t usTimeout)
{
#ifdef OS_WINDOWS
  AsyncFlags flags = afNone;
#else
  AsyncFlags flags = afRunning;
#endif
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.accept, acceptFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actAccept, &context);
  combinerPushOperation(&op->root, aaStart);

  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  socketTy acceptSocket = op->acceptSocket;
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? acceptSocket : -(int)status;
}


ssize_t ioRead(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.read, 0, buffer, size);
  asyncOpRoot *op = runIoOperation(&object->root, newAsyncOp, implReadProxy, initOp, flags, usTimeout, actRead, &context);
  return op ? coroutineRwFinish((asyncOp*)op, object) : (ssize_t)context.BytesTransferred;
}


ssize_t ioWrite(aioObject *object, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.write, 0, (void*)((uintptr_t)buffer), size);
  asyncOpRoot *op = runIoOperation(&object->root, newAsyncOp, implWriteProxy, initOp, flags, usTimeout, actWrite, &context);
  return op ? coroutineRwFinish((asyncOp*)op, object) : (ssize_t)context.BytesTransferred;
}

ssize_t ioReadMsg(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  // Datagram socket can be accessed by multiple threads without lock
  struct sockaddr_in source;
  socketLenTy addrlen = sizeof(source);
#ifdef OS_WINDOWS
  ssize_t result = recvfrom(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)&source, &addrlen);
#else
  ssize_t result = recvfrom(object->hSocket, buffer, size, 0, (struct sockaddr*)&source, &addrlen);
#endif

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.readMsg, 0, buffer, size);
  if (result >= 0) {
    // Data received synchronously
    if (++currentFinishedSync >= MAX_SYNCHRONOUS_FINISHED_OPERATION) {
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actReadMsg, &context);
      op->bytesTransferred = (size_t)result;
      opForceStatus(&op->root, aosSuccess);
      addToGlobalQueue(&op->root);
      coroutineYield();
      return coroutineRwFinish(op, object);
    } else {
      return result;
    }
  }

  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actReadMsg, &context);
  combinerPushOperation(&op->root, aaStart);
  coroutineYield();
  return coroutineRwFinish(op, object);
}

ssize_t ioWriteMsg(aioObject *object, const HostAddress *address, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  // Datagram socket can be accessed by multiple threads without lock
  struct sockaddr_in remoteAddress;
  remoteAddress.sin_family = address->family;
  remoteAddress.sin_addr.s_addr = address->ipv4;
  remoteAddress.sin_port = address->port;
#ifdef OS_WINDOWS
  ssize_t result = sendto(object->hSocket, buffer, (int)size, 0, (struct sockaddr *)&remoteAddress, sizeof(remoteAddress));
#else
  ssize_t result = sendto(object->hSocket, buffer, size, 0, (struct sockaddr *)&remoteAddress, sizeof(remoteAddress));
#endif

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.writeMsg, 0, (void*)((uintptr_t)buffer), size);
  if (result != -1) {
    // Data received synchronously
    if (++currentFinishedSync >= MAX_SYNCHRONOUS_FINISHED_OPERATION) {
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actWriteMsg, &context);
      op->host = *address;
      opForceStatus(&op->root, aosSuccess);
      addToGlobalQueue(&op->root);
      coroutineYield();
      return coroutineRwFinish(op, object);
    } else {
      return result;
    }
  }

  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actWriteMsg, &context);
  op->host = *address;
  combinerPushOperation(&op->root, aaStart);
  coroutineYield();
  return coroutineRwFinish(op, object);
}


void ioSleep(aioUserEvent *event, uint64_t usTimeout)
{
  if (event->pendingActivations > 0) {
    event->pendingActivations--;
    return;
  }
  event->root.callback = (void*)coroutineEventCb;
  event->root.arg = coroutineCurrent();
  event->root.timeout = usTimeout;
  event->counter = 1;
  event->base->methodImpl.startTimer(&event->root);
  coroutineYield();
  event->root.callback = 0;
  event->root.arg = 0;
}

void ioWaitUserEvent(aioUserEvent *event)
{
  if (event->pendingActivations > 0) {
    event->pendingActivations--;
    return;
  }
  event->root.callback = (void*)coroutineEventCb;
  event->root.arg = coroutineCurrent();
  coroutineYield();
  event->root.callback = 0;
  event->root.arg = 0;
}
