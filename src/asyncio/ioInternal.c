#include "ioInternal.h"
#include "asyncio/coroutine.h"
#include "asyncio/device.h"
#include "asyncio/socket.h"
#include "atomic.h"
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#ifndef OS_WINDOWS
#include <fcntl.h>
#include <sys/stat.h>
#endif

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;

// Reactor operations park as already running: kernel readiness drives them from submission on. The IOCP proactor must reach executeMethod to
// post the overlapped I/O first, so its operations park as waiting until then.
#ifdef OS_WINDOWS
static const AsyncFlags afSyncStarted = afNone;
#else
static const AsyncFlags afSyncStarted = afRunning;
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

static void connectFinish(asyncOpRoot *opptr)
{
  ((aioConnectCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, opptr->arg);
}

static void acceptFinish(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  ((aioAcceptCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, op->host, op->acceptSocket, opptr->arg);
}

static void rwFinish(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  ((aioCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, op->bytesTransferred, opptr->arg);
}

static void readMsgFinish(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  ((aioReadMsgCb*)opptr->callback)(opGetStatus(opptr), (aioObject*)opptr->object, op->host, op->bytesTransferred, opptr->arg);
}

static void releaseOp(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  // Pool-sized scratch stays with the pooled operation for the next parked write or accept; only oversized captures (large payloads under
  // backpressure) go back to the allocator, so a pooled operation retains at most the default buffer size
  if (op->internalBufferSize > DEFAULT_SOCKET_BUFFER_SIZE) {
    free(op->internalBuffer);
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }
}

static void releaseAcceptOp(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  // Ownership leaves the operation through the success callback (aio flavor) or through the resumed coroutine reading the fields (io flavor);
  // anything still owned here - a failed accept, or a callback-less aioAccept that nobody can receive - is closed
  if (op->acceptSocket != INVALID_SOCKET && (opGetStatus(opptr) != aosSuccess || (opptr->callback == 0 && !(opptr->flags & afCoroutine)))) {
    socketClose(op->acceptSocket);
    op->acceptSocket = INVALID_SOCKET;
  }
  releaseOp(opptr);
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
  asyncBase *base = object->header.base;
  asyncOp *op = (asyncOp*)base->methodImpl.newAsyncOp(base, flags & afRealtime, &opPool, &opTimerPool);
  initAsyncOpRoot(&op->root,
                  context->StartProc,
                  base->methodImpl.cancelAsyncOp,
                  context->FinishProc,
                  opCode == actAccept ? releaseAcceptOp : releaseOp,
                  object,
                  callback,
                  arg,
                  flags,
                  opCode,
                  usTimeout);

  op->state = 0;
  op->transactionSize = context->TransactionSize;
  op->bytesTransferred = 0;
  if (opCode == actAccept)
    op->acceptSocket = INVALID_SOCKET;
  if (opCode == actAccept || opCode == actReadMsg)
    memset(&op->host, 0, sizeof(op->host));
  if (context->TransactionSize && (opCode & OPCODE_WRITE) && !(flags & afNoCopy)) {
    asyncOpEnsureInternalBuffer(&op->internalBuffer, &op->internalBufferSize, context->TransactionSize);
    memcpy(op->internalBuffer, context->Buffer, context->TransactionSize);
    op->buffer = op->internalBuffer;
  } else {
    op->buffer = context->Buffer;
  }

  return &op->root;
}

static ssize_t coroutineRwFinish(asyncOp *op)
{
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

void setSocketBuffer(aioObject *socket, size_t bufferSize)
{
  ioBufferEnsureCapacity(&socket->buffer, bufferSize);
}

int copyFromBuffer(void *dst, size_t *offset, struct ioBuffer *src, size_t size)
{
  size_t needRead = size - *offset;
  size_t remaining = src->dataSize - src->offset;
  if (needRead <= remaining) {
    memcpy((uint8_t*)dst + *offset, (uint8_t*)src->ptr + src->offset, needRead);
    *offset += needRead;
    src->offset += needRead;
    return 1;
  } else {
    memcpy((uint8_t*)dst + *offset, (uint8_t*)src->ptr + src->offset, remaining);
    *offset += remaining;
    src->offset = 0;
    src->dataSize = 0;
    return 0;
  }
}

aioObject *newSocketIo(asyncBase *base, socketTy hSocket)
{
#ifdef SO_NOSIGPIPE
  // Accepted sockets never pass through socketCreate, and Darwin/BSD have no MSG_NOSIGNAL, so SIGPIPE suppression must be a property of the
  // descriptor itself, set at the point every socket enters the async machinery.
  int optval = 1;
  setsockopt(hSocket, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof(optval));
#endif
  aioObject *object = base->methodImpl.newAioObject(base, ioObjectSocket, &hSocket);
  if (!object)
    return 0;
  object->needSigpipeGuard = 0;
  return object;
}

aioObject *newDeviceIo(asyncBase *base, iodevTy hDevice)
{
  int needGuard = 0;
#ifndef OS_WINDOWS
  // write() has no MSG_NOSIGNAL equivalent, so descriptors that can raise SIGPIPE need protection: pipes, plus sockets handed here by mistake.
  // Character devices (serial ports, ttys) cannot raise it and stay guard-free.
  struct stat deviceStat;
  if (fstat(hDevice, &deviceStat) == 0 && (S_ISFIFO(deviceStat.st_mode) || S_ISSOCK(deviceStat.st_mode)))
    needGuard = 1;
#ifdef F_SETNOSIGPIPE
  // Per-fd suppression (Darwin/NetBSD) makes per-write masking unnecessary.
  if (needGuard && fcntl(hDevice, F_SETNOSIGPIPE, 1) == 0)
    needGuard = 0;
#endif
#endif
  aioObject *object = base->methodImpl.newAioObject(base, ioObjectDevice, &hDevice);
  if (!object)
    return 0;
  object->needSigpipeGuard = needGuard;
  return object;
}

void deleteAioObject(aioObject *object)
{
  objectDelete(&object->root);
}

asyncBase *aioGetBase(aioObject *object)
{
  return object->root.header.base;
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

  if (copyFromBuffer(buffer, bytesTransferred, sb, size))
    return 0;
  // A partial hit from the read-ahead buffer completes a non-afWaitAll read (the reactor executors and iocp share this contract): one more
  // syscall could hit EAGAIN and park the already delivered bytes behind the timeout
  if (*bytesTransferred != 0 && !(flags & afWaitAll))
    return 0;

  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.read, rwFinish, buffer, size);
  if (size < sb->totalSize) {
    size_t bytes;
    while (*bytesTransferred <= size) {
      int result = object->root.header.objectType == ioObjectSocket ? socketSyncRead(object->hSocket, sb->ptr, sb->totalSize, 0, &bytes)
                                                                    : deviceSyncRead(object->hDevice, sb->ptr, sb->totalSize, 0, &bytes);
      if (result) {
        sb->dataSize = bytes;
        if (copyFromBuffer(buffer, bytesTransferred, sb, size) || !(flags & afWaitAll))
          break;
        // A short refill means the queue is drained right now: park without paying for the guaranteed-EAGAIN syscall
        if (bytes == sb->totalSize)
          continue;
      }
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afSyncStarted, usTimeout, (void*)callback, arg, actRead, &context);
      op->bytesTransferred = *bytesTransferred;
      return &op->root;
    }

    return 0;
  } else {
    size_t bytes = 0;
    int result =
        object->root.header.objectType == ioObjectSocket
            ? socketSyncRead(object->hSocket, (uint8_t*)buffer + *bytesTransferred, size - *bytesTransferred, flags & afWaitAll, &bytes)
            : deviceSyncRead(object->hDevice, (uint8_t*)buffer + *bytesTransferred, size - *bytesTransferred, flags & afWaitAll, &bytes);
    *bytesTransferred += bytes;
    if (result) {
      return 0;
    } else {
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afSyncStarted, usTimeout, (void*)callback, arg, actRead, &context);
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
  if (size == 0) {
    *bytesTransferred = 0;
    return 0;
  }

  size_t bytes = 0;
  int result;
  if (object->root.header.objectType == ioObjectSocket) {
    result = socketSyncWrite(object->hSocket, buffer, size, flags & afWaitAll, &bytes);
  }
#ifndef OS_WINDOWS
  else if (object->needSigpipeGuard && !sigpipeIgnored) {
    struct SigpipeGuard guard;
    sigpipeGuardEnter(&guard);
    result = deviceSyncWrite(object->hDevice, buffer, size, flags & afWaitAll, &bytes);
    sigpipeGuardLeave(&guard, !result && errno == EPIPE);
  }
#endif
  else {
    result = deviceSyncWrite(object->hDevice, buffer, size, flags & afWaitAll, &bytes);
  }
  if (result) {
    *bytesTransferred = bytes;
    return 0;
  } else {
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.write, rwFinish, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afSyncStarted, usTimeout, (void*)callback, arg, actWrite, &context);
    op->bytesTransferred = bytes;
    return &op->root;
  }
}

static asyncOpRoot *implReadProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  return implRead((aioObject*)object,
                  context->Buffer,
                  context->TransactionSize,
                  flags,
                  usTimeout,
                  (aioCb*)callback,
                  arg,
                  &context->BytesTransferred);
}

static asyncOpRoot *implWriteProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  struct Context *context = (struct Context*)contextPtr;
  return implWrite((aioObject*)object,
                   context->Buffer,
                   context->TransactionSize,
                   flags,
                   usTimeout,
                   (aioCb*)callback,
                   arg,
                   &context->BytesTransferred);
}

void aioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout, aioConnectCb callback, void *arg)
{
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.connect, connectFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, afNone, usTimeout, (void*)callback, arg, actConnect, &context);
  op->host = *address;
  combinerPushOperation(&op->root);
}

void aioAccept(aioObject *object, uint64_t usTimeout, aioAcceptCb callback, void *arg)
{
  assert(callback && "aioAccept requires a callback: it is the only channel that can receive the accepted socket");
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.accept, acceptFinish, 0, 0);
  asyncOpRoot *op = newAsyncOp(&object->root, afSyncStarted, usTimeout, (void*)callback, arg, actAccept, &context);
  combinerPushOperation(op);
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

ssize_t aioRead(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout, aioCb callback, void *arg)
{
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.read, rwFinish, buffer, size);
  runAioOperation(&object->root, newAsyncOp, implReadProxy, makeResult, initOp, flags, usTimeout, (void*)callback, arg, actRead, &context);
  return context.Result;
}

ssize_t aioWrite(aioObject *object, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout, aioCb callback, void *arg)
{
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.write, rwFinish, (void*)((uintptr_t)buffer), size);
  runAioOperation(&object->root, newAsyncOp, implWriteProxy, makeResult, initOp, flags, usTimeout, (void*)callback, arg, actWrite, &context);
  return context.Result;
}

// One receive syscall and status/truncation oracle for both datagram read paths: Winsock consumes an oversized datagram and reports
// WSAEMSGSIZE, POSIX returns the clipped payload flagged MSG_TRUNC - either way the datagram is gone and must complete as aosBufferTooSmall,
// never be retried.
static inline int messageSizeIsSupported(size_t size)
{
  return size <= (size_t)INT_MAX;
}

static ssize_t readMsgSyscall(aioObject *object, void *buffer, size_t size, struct sockaddr_storage *source, AsyncOpStatus *status)
{
  assert(messageSizeIsSupported(size));
#ifdef OS_WINDOWS
  socketLenTy addrlen = sizeof(*source);
  ssize_t result = recvfrom(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)source, &addrlen);
  if (result >= 0) {
    *status = aosSuccess;
  } else {
    int error = WSAGetLastError();
    if (error == WSAEMSGSIZE)
      *status = aosBufferTooSmall;
    else if (error == WSAEWOULDBLOCK)
      *status = aosPending;
    else if (error == WSAECONNRESET)
      *status = aosDisconnected;
    else if (error == WSAENOTCONN)
      *status = aosNotConnected;
    else
      *status = aosUnknownError;
  }
#else
  struct iovec iov;
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  iov.iov_base = buffer;
  iov.iov_len = size;
  msg.msg_name = source;
  msg.msg_namelen = sizeof(*source);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  ssize_t result = recvmsg(object->hSocket, &msg, 0);
  if (result < 0)
    *status = socketStatusFromErrno(errno);
  else
    *status = (msg.msg_flags & MSG_TRUNC) ? aosBufferTooSmall : aosSuccess;
#endif
  return result;
}

static ssize_t writeMsgSyscall(aioObject *object, const HostAddress *address, const void *buffer, size_t size)
{
  assert(messageSizeIsSupported(size));
  struct sockaddr_storage remoteAddress;
  socketLenTy addrlen = hostAddressToSockaddr(address, &remoteAddress);
#ifdef OS_WINDOWS
  return sendto(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)&remoteAddress, addrlen);
#else
  return sendto(object->hSocket, buffer, size, ASYNCIO_MSG_NOSIGNAL, (struct sockaddr*)&remoteAddress, addrlen);
#endif
}

// Datagram fast paths run their syscall without entering the combiner, so the sticky delete sweep cannot stop them; the callers gate on
// DeletePending before touching the socket - after objectDelete an incoming flood would otherwise keep the path succeeding and teardown would
// never finish. Fire-and-forget learns the rejection inline; a callback completes with aosCanceled through the global queue.
static ssize_t datagramRejectClosing(aioObject *object,
                                     AsyncFlags flags,
                                     uint64_t usTimeout,
                                     void *callback,
                                     void *arg,
                                     int opCode,
                                     struct Context *context)
{
  if (callback == 0)
    return -(ssize_t)aosCanceled;
  // The rejected operation only delivers aosCanceled; nothing ever reads a write payload from it, so skip the capture copy. This also keeps the
  // op pools clean: this op bypasses opRelease, so a captured buffer would ride into the pool around the releaseMethod size cap
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afNoCopy, usTimeout, callback, arg, opCode, context);
  if (opCode == actReadMsg)
    memset(&op->host, 0, sizeof(op->host));
  opForceStatus(&op->root, aosCanceled);
  addToGlobalQueue(&op->root);
  return -(ssize_t)aosPending;
}

ssize_t aioReadMsg(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout, aioReadMsgCb callback, void *arg)
{
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed)) {
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.readMsg, readMsgFinish, buffer, size);
    return datagramRejectClosing(object, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
  }

  struct sockaddr_storage source;
  AsyncOpStatus status;
  ssize_t result;
  if (messageSizeIsSupported(size)) {
    result = readMsgSyscall(object, buffer, size, &source, &status);
  } else {
    result = -1;
    status = aosUnknownError;
  }

  if (status != aosPending) {
    // Data, truncation and fatal socket errors are all terminal: only would-block may be retried by a parked operation. In particular a pending
    // UDP error is consumed by this syscall and cannot be rediscovered.
    if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION))
      return status == aosSuccess ? result : -(ssize_t)status;

    if (flags & afActiveOnce)
      currentFinishedSync = 0;
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.readMsg, readMsgFinish, buffer, size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
    op->bytesTransferred = status == aosBufferTooSmall ? size : status == aosSuccess ? (size_t)result : 0;
    if (status == aosSuccess || status == aosBufferTooSmall)
      sockaddrToHostAddress(&source, &op->host);
    else
      memset(&op->host, 0, sizeof(op->host));
    opForceStatus(&op->root, status);
    addToGlobalQueue(&op->root);
  } else {
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.readMsg, readMsgFinish, buffer, size);
    asyncOpRoot *op = newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
    combinerPushOperation(op);
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
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed)) {
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.writeMsg, rwFinish, (void*)((uintptr_t)buffer), size);
    return datagramRejectClosing(object, flags, usTimeout, (void*)callback, arg, actWriteMsg, &context);
  }

  // Datagram socket can be accessed by multiple threads without lock
  int sizeIsSupported = messageSizeIsSupported(size);
  ssize_t result = sizeIsSupported ? writeMsgSyscall(object, address, buffer, size) : -1;

  if (result >= 0 || !sizeIsSupported) {
    AsyncOpStatus status = result >= 0 ? aosSuccess : aosUnknownError;
    if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION))
      return status == aosSuccess ? result : -(ssize_t)status;

    if (flags & afActiveOnce)
      currentFinishedSync = 0;
    // The operation is already terminal; the op only carries its completion, so the payload capture copy is skipped (afNoCopy)
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.writeMsg, rwFinish, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afNoCopy, usTimeout, (void*)callback, arg, actWriteMsg, &context);
    op->bytesTransferred = status == aosSuccess ? (size_t)result : 0;
    opForceStatus(&op->root, status);
    addToGlobalQueue(&op->root);
  } else {
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.writeMsg, rwFinish, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actWriteMsg, &context);
    op->host = *address;
    combinerPushOperation(&op->root);
  }

  return -(ssize_t)aosPending;
}

int ioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.connect, connectFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, afCoroutine, usTimeout, 0, 0, actConnect, &context);
  op->host = *address;
  combinerPushOperation(&op->root);
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -status;
}

int ioAccept(aioObject *object, socketTy *acceptedSocket, HostAddress *remoteAddress, uint64_t usTimeout)
{
  *acceptedSocket = INVALID_SOCKET;
  if (remoteAddress)
    memset(remoteAddress, 0, sizeof(*remoteAddress));

  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.accept, acceptFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, afSyncStarted | afCoroutine, usTimeout, 0, 0, actAccept, &context);
  combinerPushOperation(&op->root);

  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  if (status == aosSuccess) {
    *acceptedSocket = op->acceptSocket;
    if (remoteAddress)
      *remoteAddress = op->host;
  }
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -(int)status;
}

ssize_t ioRead(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.read, 0, buffer, size);
  asyncOpRoot *op = runIoOperation(&object->root, newAsyncOp, implReadProxy, initOp, flags, usTimeout, actRead, &context);
  return op ? coroutineRwFinish((asyncOp*)op) : (ssize_t)context.BytesTransferred;
}

ssize_t ioWrite(aioObject *object, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.write, 0, (void*)((uintptr_t)buffer), size);
  asyncOpRoot *op = runIoOperation(&object->root, newAsyncOp, implWriteProxy, initOp, flags, usTimeout, actWrite, &context);
  return op ? coroutineRwFinish((asyncOp*)op) : (ssize_t)context.BytesTransferred;
}

ssize_t ioReadMsg(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  // See datagramRejectClosing: reject a closing object before the syscall
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed))
    return -(ssize_t)aosCanceled;

  // Datagram socket can be accessed by multiple threads without lock
  struct sockaddr_storage source;
  AsyncOpStatus status;
  ssize_t result;
  if (messageSizeIsSupported(size)) {
    result = readMsgSyscall(object, buffer, size, &source, &status);
  } else {
    result = -1;
    status = aosUnknownError;
  }

  if (status != aosPending) {
    // Preserve the fairness budget for every terminal inline result. Fatal errors cannot be retried: recvmsg/recvfrom may have consumed socket
    // state.
    if (++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION)
      return status == aosSuccess ? result : -(ssize_t)status;

    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.readMsg, 0, buffer, size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actReadMsg, &context);
    op->bytesTransferred = status == aosBufferTooSmall ? size : status == aosSuccess ? (size_t)result : 0;
    opForceStatus(&op->root, status);
    addToGlobalQueue(&op->root);
    coroutineYield();
    return coroutineRwFinish(op);
  }

  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.readMsg, 0, buffer, size);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actReadMsg, &context);
  combinerPushOperation(&op->root);
  coroutineYield();
  return coroutineRwFinish(op);
}

ssize_t ioWriteMsg(aioObject *object, const HostAddress *address, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  // See datagramRejectClosing: reject a closing object before the syscall
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed))
    return -(ssize_t)aosCanceled;

  // Datagram socket can be accessed by multiple threads without lock
  int sizeIsSupported = messageSizeIsSupported(size);
  ssize_t result = sizeIsSupported ? writeMsgSyscall(object, address, buffer, size) : -1;

  if (result != -1 || !sizeIsSupported) {
    AsyncOpStatus status = result != -1 ? aosSuccess : aosUnknownError;
    if (++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION)
      return status == aosSuccess ? result : -(ssize_t)status;

    // The operation is already terminal; the op only carries its completion, so the payload capture copy is skipped (afNoCopy)
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.writeMsg, 0, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine | afNoCopy, usTimeout, 0, 0, actWriteMsg, &context);
    if (status == aosSuccess)
      op->host = *address;
    op->bytesTransferred = status == aosSuccess ? (size_t)result : 0;
    opForceStatus(&op->root, status);
    addToGlobalQueue(&op->root);
    coroutineYield();
    return coroutineRwFinish(op);
  }

  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.writeMsg, 0, (void*)((uintptr_t)buffer), size);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actWriteMsg, &context);
  op->host = *address;
  combinerPushOperation(&op->root);
  coroutineYield();
  return coroutineRwFinish(op);
}
