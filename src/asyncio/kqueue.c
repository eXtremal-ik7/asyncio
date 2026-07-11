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

// Discriminator bits carried in the udata pointer tag. The event decode path
// runs concurrently with the owning thread initializing the pointed-to object
// (the kernel round-trip is not an ordering edge), so the branch decision must
// not read the object's memory: fd objects carry no bits, timers are marked
// explicitly, user-event timers additionally so
enum {
  udataTimer = 1,
  udataUserEvent = 2
};

typedef struct aioTimer {
  aioObjectRoot root;
  // Full generation of the armed operation; 0 = disarmed. Written with release
  // at arm time, read with acquire at event delivery: the kernel round-trip
  // carries no memory ordering, this field is the synchronization edge. A stale
  // event delivered after stop/rearm reads either 0 or a generation that loses
  // the status CAS in opCancel().
  uintptr_t tag;
  intptr_t fd;
  asyncOpRoot *op;
} aioTimer;

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig);
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
  if (kevent(kqueueFd, &event, 1, 0, 0, 0) == -1) {
    // fprintf(stderr, "kqueue event error, errno: %s\n", strerror(errno));
  }
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
      // fprintf(stderr, " * kqueueNewAsyncBase: kqueue_create failed\n");
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

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig)
{
  kqueueBase *base = (kqueueBase*)object->base;
  aioObject *fdObject = (object->type == ioObjectDevice || object->type == ioObjectSocket) ? (aioObject*)object : 0;
  uint32_t oldIoEvents = fdObject ? combinerActiveIoEvents(object) : 0;
  uint32_t progress = sig & COMBINER_TAG_PROGRESS_MASK;
  uint32_t ioEvents = fdObject
    ? progress | ((sig & COMBINER_TAG_ERROR) ? IO_EVENT_ERROR : 0)
    : 0;

  // A dying object gets no fd readiness processing: its operations are being
  // cancelled wholesale anyway, and for a stale batch entry landing here
  // within the grace period the descriptor is already closed - the error
  // path ioctl and the rearm kevent would run on a dead or reused fd
  if (fdObject && __uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    ioEvents = 0;
    progress = 0;
  }

  // READ/WRITE tag values deliberately match IO_EVENT_READ/WRITE.
  uint32_t needStart = progress;

  // Start a submitted operation before completing initialization from the
  // accumulated event: a start delivered together with the event must
  // enter its queue first, otherwise a failed connect cancels the queues
  // without it and the operation would start on the dead socket afterwards
  if (op)
    startOperation(op, &needStart);

  // By contract initialization precedes ordinary I/O, so any progress while
  // its slot is occupied belongs to it. Drive it before the disconnect sweep
  // so queued operations inherit a connect failure, not aosDisconnected.
  if (progress && __uintptr_atomic_load(&object->initializationOp, amoRelaxed))
    processInitializationOp(object, &needStart);

  // CANCEL: a timeout/opCancel/cancelIo set the status and asked for a scan
  if (sig & COMBINER_TAG_CANCEL)
    reapObject(object, &needStart);

  if (ioEvents & IO_EVENT_ERROR) {
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

  if (fdObject && !__uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    int fd = getFd(fdObject);
    uint32_t newIoEvents = combinerActiveIoEvents(object);
    unsigned readEventActivated = (oldIoEvents & IO_EVENT_READ) && !(ioEvents & IO_EVENT_READ);
    unsigned writeEventActivated = (oldIoEvents & IO_EVENT_WRITE) && !(ioEvents & IO_EVENT_WRITE);

    if ((newIoEvents & IO_EVENT_READ) && !readEventActivated)
        kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_READ, fd, object);
    if (!(newIoEvents & IO_EVENT_READ) && readEventActivated)
        kqueueControl(base->kqueueFd, EV_DELETE| EV_ONESHOT | EV_EOF, EVFILT_READ, fd, object);
    if ((newIoEvents & IO_EVENT_WRITE) && !writeEventActivated)
        kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, object);
    if (!(newIoEvents & IO_EVENT_WRITE) && writeEventActivated)
        kqueueControl(base->kqueueFd, EV_DELETE | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, object);
  }
}

void kqueueNextFinishedOperation(asyncBase *base)
{
  int nfds, n;
  struct kevent events[MAX_EVENTS];
  kqueueBase *localBase = (kqueueBase *)base;
  messageLoopThreadId = __sync_fetch_and_add(&base->messageLoopThreadCounter, 1);
  graceThreadEnter(base);

  while (1) {
    do {
      if (!executeGlobalQueue(base)) {
        // Found quit marker. Stamp the slot out before the counter drops:
        // once it does, a future loop thread may adopt this id, and a stale
        // stamp would shield its batches from the grace period. The last
        // thread out drains the limbo list
        if (messageLoopThreadId < base->graceSlotLimit)
          __uintptr_atomic_store(&base->graceSeen[messageLoopThreadId].seen,
                                 UINTPTR_MAX,
                                 amoRelease);
        graceReclaim(base);
        unsigned threadsRunning = __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, 0u-1) - 1;
        if (threadsRunning)
          kqueueEnqueue(base, 0);
        return;
      }

      struct timespec timeout;
      timeout.tv_sec = 1;
      timeout.tv_nsec = 0;
      graceQuiesce(base);
      nfds = kevent(localBase->kqueueFd, 0, 0, events, MAX_EVENTS, &timeout);

      uint64_t currentTime = getMonotonicSeconds();
      unsigned loopThreadCount = __uint_atomic_load(&base->messageLoopThreadCounter, amoRelaxed);
      if (currentTime % loopThreadCount == messageLoopThreadId)
        processTimeoutQueue(base, currentTime);
    } while (nfds <= 0 && errno == EINTR);

    for (n = 0; n < nfds; n++) {
      uintptr_t timerId;
      aioObjectRoot *object;
      __tagged_pointer_decode(events[n].udata, (void**)&object, &timerId);
      if (object == 0) {
        // EVFILT_USER with EV_CLEAR stays registered, no re-add needed
      } else if (timerId & udataTimer) {
        // The acquire load pairs with the release store in kqueueStartTimer and
        // is the only ordering edge with the arming thread; timer->op and the
        // fields behind it may only be read after it. 0 = a stale doorbell
        // delivered after stop, nothing may be touched. The user-event bit
        // rides in the udata tag: op->opCode belongs to the operation and
        // cannot be read before its generation is validated by the status CAS
        aioTimer *timer = (aioTimer*)object;
        uintptr_t armedTag = __uintptr_atomic_load(&timer->tag, amoAcquire);
        if (armedTag == 0) {
          // stale doorbell, timer disarmed
        } else if (timerId & udataUserEvent) {
          aioUserEvent *event = (aioUserEvent*)timer->op;
          if (eventTryActivate(event)) {
            if (event->counter > 0 && --event->counter == 0)
              kqueueStopTimer(&event->root);

            eventDeactivate(event);
            event->root.finishMethod(&event->root);
            eventDecrementReference(event, 1);
          }
        } else {
          opCancel(timer->op, armedTag, aosTimeout);
        }
      } else {
        uint32_t bits = (events[n].flags & EV_EOF) ? COMBINER_TAG_ERROR : 0;
        if (events[n].filter == EVFILT_READ) {
          bits |= COMBINER_TAG_PROGRESS_READ;
        } else if (events[n].filter == EVFILT_WRITE) {
          bits |= COMBINER_TAG_PROGRESS_WRITE;
        }
        if (bits & COMBINER_TAG_PROGRESS_MASK)
          combinerPushCounter(object, bits);
      }
    }
  }
}


aioObject *kqueueNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  aioObject *object = 0;
  if (!objectPoolGet(&objectPool, (void**)&object, sizeof(aioObject))) {
    object = alignedMalloc(sizeof(aioObject), TAGGED_POINTER_ALIGNMENT);
    object->buffer.ptr = 0;
    object->buffer.totalSize = 0;
  }

  initObjectRoot(&object->root, base, type, (aioObjectDestructor*)kqueueDeleteObject);
  switch (type) {
    case ioObjectDevice :
      object->hDevice = *(iodevTy *)data;
      break;
    case ioObjectSocket :
      object->hSocket = *(socketTy *)data;
      break;
    default :
      break;
  }

  object->buffer.offset = 0;
  object->buffer.dataSize = 0;
  return object;
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

// Memory half of the object destructor, runs via graceRetire once no loop
// thread can hold the object in an already-harvested event batch
static void kqueueReleaseObject(aioObject *object)
{
  objectPoolPut(&objectPool, object, sizeof(aioObject));
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

  // close() detached the knotes (descriptors are never dup()ed here), so
  // batches harvested from now on cannot reference the object; batches
  // already in loop thread buffers gate the memory release through the
  // grace period
  graceRetire(object->root.base, &object->root, (aioObjectDestructor*)kqueueReleaseObject);
}

void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  kqueueBase *localBase = (kqueueBase*)base;
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  timer->root.base = base;
  timer->root.type = ioObjectTimer;
  timer->tag = 0;
  timer->fd = __sync_fetch_and_add(&localBase->timerIdCounter, 1);
  timer->op = op;
  op->timerId = timer;
}

void kqueueStartTimer(asyncOpRoot *op)
{
  struct kevent event;
  int periodic = op->opCode == actUserEvent;
  aioTimer *timer = (aioTimer*)op->timerId;
  __uintptr_atomic_store(&timer->tag, opGetGeneration(op), amoRelease);
  EV_SET(&event,
         timer->fd,
         EVFILT_TIMER,
         EV_ADD | EV_ENABLE | (periodic ? 0 : EV_ONESHOT),
         NOTE_USECONDS,
         op->timeout,
         __tagged_pointer_make(timer, udataTimer | (periodic ? udataUserEvent : 0)));
  if (kevent(((kqueueBase*)timer->root.base)->kqueueFd, &event, 1, 0, 0, 0) == -1) {
    // fprintf(stderr, "kqueueStartTimer: %s\n", strerror(errno));
  }
}


void kqueueStopTimer(asyncOpRoot *op)
{
  struct kevent event;
  aioTimer *timer = (aioTimer*)op->timerId;
  __uintptr_atomic_store(&timer->tag, 0, amoRelaxed);
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DELETE, 0, 0, 0);
  if (kevent(((kqueueBase*)timer->root.base)->kqueueFd, &event, 1, 0, 0, 0) == -1) {
    // fprintf(stderr, "kqueueStopTimer: %s\n", strerror(errno));
  }
}

// Memory half only: the timer pointer is published to the kernel as udata and
// may still sit in another loop thread's harvested batch, so the release goes
// through the grace period like any other kernel-published object
static void kqueueReleaseTimer(aioObjectRoot *object)
{
  free(object);
}

void kqueueDeleteTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  graceRetire(timer->root.base, &timer->root, kqueueReleaseTimer);
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
