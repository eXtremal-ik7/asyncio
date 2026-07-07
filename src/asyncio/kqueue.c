#include "asyncioImpl.h"
#include "atomic.h"
#include "asyncio/ringBuffer.h"

#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static ConcurrentQueue objectPool;

#define MAX_EVENTS 256

typedef struct kqueueBase {
  asyncBase B;
  int kqueueFd;
  intptr_t timerIdCounter;
} kqueueBase;

typedef struct KQueueObject {
  aioObject Object;
  uint32_t ReadEvents;
  uint32_t WriteEvents;
} KQueueObject;

typedef struct aioTimer {
  aioObjectRoot root;
  intptr_t fd;
  asyncOpRoot *op;
} aioTimer;

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, AsyncOpActionTy opMethod);
void kqueueEnqueue(asyncBase *base, asyncOpRoot *op);
void kqueuePostEmptyOperation(asyncBase *base);
void kqueueNextFinishedOperation(asyncBase *base);
aioObject *kqueueNewAioObject(asyncBase *base, IoObjectTy type, void *data);
asyncOpRoot *kqueueNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool);
int kqueueCancelAsyncOp(asyncOpRoot *opptr);
void kqueueDeleteObject(aioObject *object);
void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op);
void kqueueStartTimer(asyncOpRoot *op);
void kqueueStopTimer(asyncOpRoot *op);
void kqueueDeleteTimer(asyncOpRoot *op);
void kqueueActivate(aioUserEvent *op);
AsyncOpStatus kqueueAsyncConnect(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncAccept(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncRead(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncWrite(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncReadMsg(asyncOpRoot *op);
AsyncOpStatus kqueueAsyncWriteMsg(asyncOpRoot *op);

static struct asyncImpl kqueueImpl = {
  combinerTaskHandler,
  kqueueEnqueue,
  kqueuePostEmptyOperation,
  kqueueNextFinishedOperation,
  kqueueNewAioObject,
  kqueueNewAsyncOp,
  kqueueCancelAsyncOp,
  kqueueDeleteObject,
  kqueueInitializeTimer,
  kqueueStartTimer,
  kqueueStopTimer,
  kqueueDeleteTimer,
  kqueueActivate,
  kqueueAsyncConnect,
  kqueueAsyncAccept,
  kqueueAsyncRead,
  kqueueAsyncWrite,
  kqueueAsyncReadMsg,
  kqueueAsyncWriteMsg
};

static void kqueueControl(int kqueueFd, uint16_t flags, int16_t filter, int fd, void *ptr)
{
  struct kevent event;
  EV_SET(&event, fd, filter, flags, 0, 0, ptr);
  if (kevent(kqueueFd, &event, 1, 0, 0, 0) == -1)
    fprintf(stderr, "kqueue event error, errno: %s\n", strerror(errno));
}

static int getFd(aioObject *object)
{
  switch (object->root.type) {
    case ioObjectDevice :
      return object->hDevice;
    case ioObjectSocket :
      return object->hSocket;
    default :
      return -1;
  }
}

asyncBase *kqueueNewAsyncBase()
{
  kqueueBase *base = malloc(sizeof(kqueueBase));
  if (base) {
    base->B.methodImpl = kqueueImpl;
    base->kqueueFd = kqueue();
    if (base->kqueueFd == -1) {
      fprintf(stderr, " * kqueueNewAsyncBase: kqueue_create failed\n");
    }

    base->timerIdCounter = 1;
    kqueueControl(base->kqueueFd, EV_ADD | EV_CLEAR, EVFILT_USER, 1, 0);
  }

  return (asyncBase *)base;
}

void kqueueEnqueue(asyncBase *base, asyncOpRoot *op)
{
  kqueueBase *localBase = (kqueueBase*)base;
  struct kevent ev;
  concurrentQueuePush(&base->globalQueue, op);
  EV_SET(&ev, 1, EVFILT_USER, 0, NOTE_TRIGGER, 0, 0);
  kevent(localBase->kqueueFd, &ev, 1, 0, 0, 0);
}

void kqueuePostEmptyOperation(asyncBase *base)
{
  kqueueEnqueue(base, 0);
}

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, AsyncOpActionTy opMethod)
{
  kqueueBase *base = (kqueueBase*)object->base;
  KQueueObject *fdObject = (object->type == ioObjectDevice || object->type == ioObjectSocket) ? (KQueueObject*)object : 0;
  uint32_t readEvents = fdObject ? fdObject->ReadEvents : 0;
  uint32_t writeEvents = fdObject ? fdObject->WriteEvents : 0;

  asyncOpRoot *connectOp = (asyncOpRoot*)object->exclusiveOp;
  int wasConnecting = connectOp && connectOp->running == arRunning;
  int hasReadOp = object->readQueue.head != 0;
  int hasWriteOp = object->writeQueue.head != 0;

  uint32_t needStart = readEvents | writeEvents;

  // Process the current action before completing the exclusive connect from
  // the accumulated fd event: an aaStart delivered together with the event
  // must enter its queue first, otherwise a failed connect cancels the queues
  // without it and the operation would start on the dead socket afterwards
  if (op)
    processAction(op, opMethod, &needStart);

  // A parked connect (the object's exclusive operation) is driven by fd
  // events directly, not through a queue; its completion is signaled by
  // writability or an error. Drive it before the disconnect sweep so that
  // operations queued behind it are cancelled with the connect status, not
  // aosDisconnected
  if (wasConnecting && ((readEvents | writeEvents) & (IO_EVENT_WRITE | IO_EVENT_ERROR)))
    processExclusiveOp(object, &needStart);

  if ((readEvents | writeEvents) & IO_EVENT_ERROR) {
    // EV_EOF mapped to TAG_ERROR, cancel all operations with aosDisconnected status
    int available;
    int fd = getFd((aioObject*)object);
    ioctl(fd, FIONREAD, &available);
    if (available == 0)
      cancelOperationList(&object->readQueue, aosDisconnected);
    cancelOperationList(&object->writeQueue, aosDisconnected);
  }

  if (needStart & IO_EVENT_READ)
    executeOperationList(&object->readQueue);
  if (needStart & IO_EVENT_WRITE)
    executeOperationList(&object->writeQueue);

  if (fdObject) {
    int fd = getFd((aioObject*)object);

    if (readEvents)
      fdObject->ReadEvents = 0;
    if (writeEvents)
      fdObject->WriteEvents = 0;

    unsigned readEventActivated = hasReadOp && !(readEvents & IO_EVENT_READ);
    unsigned writeEventActivated = (hasWriteOp || wasConnecting) && !(writeEvents & IO_EVENT_WRITE);

    asyncOpRoot *connectOpNow = (asyncOpRoot*)object->exclusiveOp;
    int connectingNow = connectOpNow && connectOpNow->running == arRunning;
    int wantWrite = object->writeQueue.head != 0 || connectingNow;

    if (object->readQueue.head && !readEventActivated)
        kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_READ, fd, object);
    if (!object->readQueue.head && readEventActivated)
        kqueueControl(base->kqueueFd, EV_DELETE| EV_ONESHOT | EV_EOF, EVFILT_READ, fd, object);
    if (wantWrite && !writeEventActivated)
        kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, object);
    if (!wantWrite && writeEventActivated)
        kqueueControl(base->kqueueFd, EV_DELETE | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, object);
  }
}

void kqueueNextFinishedOperation(asyncBase *base)
{
  int nfds, n;
  struct kevent events[MAX_EVENTS];
  kqueueBase *localBase = (kqueueBase *)base;
  messageLoopThreadId = __sync_fetch_and_add(&base->messageLoopThreadCounter, 1);

  while (1) {
    do {
      if (!executeGlobalQueue(base)) {
        // Found quit marker
        unsigned threadsRunning = __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, 0u-1) - 1;
        if (threadsRunning)
          kqueueEnqueue(base, 0);
        return;
      }

      struct timespec timeout;
      timeout.tv_sec = 1;
      timeout.tv_nsec = 0;
      nfds = kevent(localBase->kqueueFd, 0, 0, events, MAX_EVENTS, &timeout);

      time_t currentTime = time(0);
      if (currentTime % base->messageLoopThreadCounter == messageLoopThreadId)
        processTimeoutQueue(base, currentTime);
    } while (nfds <= 0 && errno == EINTR);

    for (n = 0; n < nfds; n++) {
      uintptr_t timerId;
      aioObjectRoot *object;
      __tagged_pointer_decode(events[n].udata, (void**)&object, &timerId);
      if (object == 0) {
        // EVFILT_USER with EV_CLEAR stays registered, no re-add needed
      } else if (object->type == ioObjectTimer) {
        aioTimer *timer = (aioTimer*)object;
        asyncOpRoot *op = timer->op;
        if (op->opCode == actUserEvent) {
          aioUserEvent *event = (aioUserEvent*)op;
          if (eventTryActivate(event)) {
            // TODO: compare timer and event tag
            if (event->counter > 0 && --event->counter == 0)
              kqueueStopTimer(op);

            eventDeactivate(event);
            op->finishMethod(op);
            eventDecrementReference(event, 1);
          }
        } else {
          opCancel(op, opEncodeTag(op, timerId), aosTimeout);
        }
      } else {
        uint32_t eventMask = (events[n].flags & EV_EOF) ? IO_EVENT_ERROR : 0;
        if (events[n].filter == EVFILT_READ) {
          ((KQueueObject*)object)->ReadEvents = eventMask | IO_EVENT_READ;
          combinerPushCounter(object, COMBINER_TAG_ACCESS);
        } else if (events[n].filter == EVFILT_WRITE) {
          ((KQueueObject*)object)->WriteEvents = eventMask | IO_EVENT_WRITE;
          combinerPushCounter(object, COMBINER_TAG_ACCESS);
        }
      }
    }
  }
}


aioObject *kqueueNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  KQueueObject *object = 0;
  if (!objectPoolGet(&objectPool, (void**)&object)) {
    object = alignedMalloc(sizeof(KQueueObject), TAGGED_POINTER_ALIGNMENT);
    object->Object.buffer.ptr = 0;
    object->Object.buffer.totalSize = 0;
  }

  initObjectRoot(&object->Object.root, base, type, (aioObjectDestructor*)kqueueDeleteObject);
  switch (type) {
    case ioObjectDevice :
      object->Object.hDevice = *(iodevTy *)data;
      break;
    case ioObjectSocket :
      object->Object.hSocket = *(socketTy *)data;
      break;
    default :
      break;
  }

  object->ReadEvents = 0;
  object->WriteEvents = 0;
  object->Object.buffer.offset = 0;
  object->Object.buffer.dataSize = 0;
  return &object->Object;
}

asyncOpRoot *kqueueNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool)
{
  asyncOp *op = 0;
  if (asyncOpAlloc(base, sizeof(asyncOp), isRealTime, objectPool, objectTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }

  return &op->root;
}

int kqueueCancelAsyncOp(asyncOpRoot *opptr)
{
  __UNUSED(opptr);
  return 1;
}

void kqueueDeleteObject(aioObject *object)
{
  switch (object->root.type) {
    case ioObjectDevice :
      close(object->hDevice);
      object->hDevice = -1;
      break;
    case ioObjectSocket :
      close(object->hSocket);
      object->hSocket = -1;
      break;
    default :
      break;
  }

  if (!objectPoolPut(&objectPool, object)) {
    free(object->buffer.ptr);
    alignedFree(object);
  }
}

void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  kqueueBase *localBase = (kqueueBase*)base;
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  timer->root.base = base;
  timer->root.type = ioObjectTimer;
  timer->fd = __sync_fetch_and_add(&localBase->timerIdCounter, 1);
  timer->op = op;
  op->timerId = timer;
}

void kqueueStartTimer(asyncOpRoot *op)
{
  struct kevent event;
  int periodic = op->opCode == actUserEvent;  
  aioTimer *timer = (aioTimer*)op->timerId;
  EV_SET(&event,
         timer->fd,
         EVFILT_TIMER,
         EV_ADD | EV_ENABLE | (periodic ? 0 : EV_ONESHOT),
         NOTE_USECONDS,
         op->timeout,
         __tagged_pointer_make(timer, opGetGeneration(op)));
  if (kevent(((kqueueBase*)timer->root.base)->kqueueFd, &event, 1, 0, 0, 0) == -1)
    fprintf(stderr, "kqueueStartTimer: %s\n", strerror(errno));
}


void kqueueStopTimer(asyncOpRoot *op)
{
  struct kevent event;
  aioTimer *timer = (aioTimer*)op->timerId;
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DELETE, 0, 0, 0);
  if (kevent(((kqueueBase*)timer->root.base)->kqueueFd, &event, 1, 0, 0, 0) == -1)
    fprintf(stderr, "kqueueStopTimer: %s\n", strerror(errno));
}

void kqueueDeleteTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  free(timer);
}

void kqueueActivate(aioUserEvent *op)
{
  kqueueEnqueue(op->base, &op->root);
}


AsyncOpStatus kqueueAsyncConnect(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);
  if (op->state == 0) {
    op->state = 1;
    struct sockaddr_storage sa;
    socklen_t saLen = hostAddressToSockaddr(&op->host, &sa);
    int result = connect(fd, (struct sockaddr *)&sa, saLen);
    if (result == -1 && errno != EINPROGRESS)
      return aosUnknownError;
    else
      return aosPending;
  } else {
    int error;
    socklen_t size = sizeof(error);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size);
    return (error == 0) ? aosSuccess : aosUnknownError;
  }
}


AsyncOpStatus kqueueAsyncAccept(asyncOpRoot *opptr)
{
  struct sockaddr_storage clientAddr;
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);
  socklen_t clientAddrSize = sizeof(clientAddr);
  op->acceptSocket =
    accept(fd, (struct sockaddr *)&clientAddr, &clientAddrSize);

  if (op->acceptSocket != -1) {
    int current = fcntl(op->acceptSocket, F_GETFL);
    fcntl(op->acceptSocket, F_SETFL, O_NONBLOCK | current);
    sockaddrToHostAddress(&clientAddr, &op->host);
    return aosSuccess;
  } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED || errno == EPROTO || errno == EINTR) {
    // The connection can be gone from the backlog by the time accept runs
    // (stolen by another thread, aborted by the peer): wait for the next one.
    // Resource exhaustion (EMFILE/ENFILE/ENOBUFS) must NOT retry: the
    // backlog stays readable and the retry loop would spin hot
    return aosPending;
  } else {
    return aosUnknownError;
  }
}


AsyncOpStatus kqueueAsyncRead(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  aioObject *object = (aioObject*)op->root.object;
  struct ioBuffer *sb = &object->buffer;
  int fd = getFd(object);

  if (copyFromBuffer(op->buffer, &op->bytesTransferred, sb, op->transactionSize))
    return aosSuccess;

  if (op->transactionSize <= object->buffer.totalSize) {
    while (op->bytesTransferred < op->transactionSize) {
      ssize_t bytesRead = read(fd, sb->ptr, sb->totalSize);
      if (bytesRead == 0)
        return aosDisconnected;
      else if (bytesRead < 0)
        return errno == EAGAIN ? aosPending : aosUnknownError;
      sb->dataSize = (size_t)bytesRead;

      if (copyFromBuffer(op->buffer, &op->bytesTransferred, sb, op->transactionSize) || !(opptr->flags & afWaitAll))
        break;
    }

    return aosSuccess;
  } else {
    ssize_t bytesRead = read(fd,
                             (uint8_t *)op->buffer + op->bytesTransferred,
                             op->transactionSize - op->bytesTransferred);

    if (bytesRead > 0) {
      op->bytesTransferred += (size_t)bytesRead;
      if (op->root.flags & afWaitAll && op->bytesTransferred < op->transactionSize)
        return aosPending;
      else
        return aosSuccess;
    } else if (bytesRead == 0) {
      return op->transactionSize - op->bytesTransferred > 0 ? aosDisconnected : aosSuccess;
    } else {
      return errno == EAGAIN ? aosPending : aosUnknownError;
    }
  }
}


AsyncOpStatus kqueueAsyncWrite(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  aioObject *object = (aioObject*)op->root.object;
  int fd = getFd(object);

  // Sockets are covered by SO_NOSIGPIPE from newSocketIo, and on Darwin/NetBSD
  // pipes are covered by F_SETNOSIGPIPE (needSigpipeGuard stays zero); the
  // masked branch is only reachable for pipes on FreeBSD.
  ssize_t bytesWritten;
  if (object->needSigpipeGuard && !sigpipeIgnored) {
    struct SigpipeGuard guard;
    sigpipeGuardEnter(&guard);
    bytesWritten = write(fd, (uint8_t *)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
    sigpipeGuardLeave(&guard, bytesWritten == -1 && errno == EPIPE);
  } else {
    bytesWritten = write(fd, (uint8_t *)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
  }
  if (bytesWritten > 0) {
    op->bytesTransferred += bytesWritten;
    if (op->root.flags & afWaitAll && op->bytesTransferred < op->transactionSize)
      return aosPending;
    else
      return aosSuccess;
  } else if (bytesWritten == 0) {
    return op->transactionSize - op->bytesTransferred > 0 ? aosDisconnected : aosSuccess;
  } else {
    return errno == EAGAIN ? aosPending : aosUnknownError;
  }
}


AsyncOpStatus kqueueAsyncReadMsg(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);

  struct sockaddr_storage source;
  struct iovec iov;
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  iov.iov_base = op->buffer;
  iov.iov_len = op->transactionSize;
  msg.msg_name = &source;
  msg.msg_namelen = sizeof(source);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  ssize_t result = recvmsg(fd, &msg, 0);
  if (result != -1) {
    sockaddrToHostAddress(&source, &op->host);
    op->bytesTransferred = (size_t)result;
    // MSG_TRUNC: the datagram did not fit, the kernel dropped its tail
    return (msg.msg_flags & MSG_TRUNC) ? aosBufferTooSmall : aosSuccess;
  } else {
    return errno == EAGAIN ? aosPending : aosUnknownError;
  }
}


AsyncOpStatus kqueueAsyncWriteMsg(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);

  struct sockaddr_storage remoteAddress;
  socklen_t addrLen = hostAddressToSockaddr(&op->host, &remoteAddress);
  ssize_t result = sendto(fd, op->buffer, op->transactionSize, 0, (struct sockaddr *)&remoteAddress, addrLen);
  if (result != -1) {
    return aosSuccess;
  }

  return errno == EAGAIN ? aosPending : aosUnknownError;
}
