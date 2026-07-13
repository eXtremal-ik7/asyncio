#include "asyncioImpl.h"
#include "reactorTimer.h"
#include "atomic.h"
#include "asyncio/ringBuffer.h"

#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static ConcurrentQueue objectPool;

#define MAX_EVENTS 256

typedef struct kqueueBase {
  asyncBase B;
  int kqueueFd;
} kqueueBase;

// Base part of the composite kevent ident (reactorTimer.h). Process-global
// like the op pools themselves: a pooled op slot - and its timer - can be
// recycled into any base, so per-base counters could hand two timers equal
// bases and let their idents collide inside one kqueue
static volatile intptr_t timerIdCounter;

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig);
void kqueueEnqueue(asyncBase *base, asyncOpRoot *op);
void kqueuePostEmptyOperation(asyncBase *base);
void kqueueWakeupLoop(asyncBase *base);
void kqueueNextFinishedOperation(asyncBase *base);
aioObject *kqueueNewAioObject(asyncBase *base, IoObjectTy type, void *data);
asyncOpRoot *kqueueNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool);
int kqueueCancelAsyncOp(asyncOpRoot *opptr);
void kqueueDeleteObject(aioObject *object);
void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op);
void kqueueStartTimer(asyncOpRoot *op);
void kqueueStopTimer(asyncOpRoot *op);
int kqueueActivate(aioUserEvent *op);
int kqueueUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uintptr_t generation, uint64_t period);
void kqueueReleaseUserEvent(aioUserEvent *event);
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
  kqueueActivate,
  kqueueAsyncConnect,
  kqueueAsyncAccept,
  kqueueAsyncRead,
  kqueueAsyncWrite,
  kqueueAsyncReadMsg,
  kqueueAsyncWriteMsg,
  kqueueWakeupLoop,
  kqueueUpdateEventTimer,
  kqueueReleaseUserEvent
};

static int kqueueControl(int kqueueFd, uint16_t flags, int16_t filter, int fd, void *ptr)
{
  struct kevent event;
  EV_SET(&event, fd, filter, flags, 0, 0, ptr);
  int result;
  do {
    result = kevent(kqueueFd, &event, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  return result;
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
      // Descriptor exhaustion. Without the kqueue fd every kevent call fails
      // and the message loop would spin hot on EBADF: fail creation instead
      free(base);
      return 0;
    }

    if (kqueueControl(base->kqueueFd,
                      EV_ADD | EV_CLEAR,
                      EVFILT_USER,
                      1,
                      0) == -1) {
      close(base->kqueueFd);
      free(base);
      return 0;
    }
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

void kqueueWakeupLoop(asyncBase *base)
{
  // Pure kick: trigger the user event without a queue node (an empty node is
  // the quit marker). One sleeper wakes; the loop's udata==0 branch is a
  // no-op and EV_CLEAR resets the event on delivery
  struct kevent ev;
  EV_SET(&ev, 1, EVFILT_USER, 0, NOTE_TRIGGER, 0, 0);
  kevent(((kqueueBase*)base)->kqueueFd, &ev, 1, 0, 0, 0);
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
  // cancelled wholesale anyway, and its descriptor may already be closed;
  // the error path ioctl and a rearm kevent must not run on a reused fd.
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

  // CANCEL/CANCELIO: a timeout/opCancel/cancelIo set the status and asked for
  // a scan; the CANCELIO position additionally drives the CancelIoFlag sweep
  if (sig & (COMBINER_TAG_CANCEL | COMBINER_TAG_CANCELIO))
    reapObject(object, sig, &needStart);

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
    uint64_t encoded = 0;
    if (((newIoEvents & IO_EVENT_READ) && !readEventActivated) ||
        ((newIoEvents & IO_EVENT_WRITE) && !writeEventActivated))
      encoded = objectHandleEncodeKnown(object);

    if ((newIoEvents & IO_EVENT_READ) && !readEventActivated)
        kqueueControl(base->kqueueFd,
                      EV_ADD | EV_ONESHOT | EV_EOF,
                      EVFILT_READ,
                      fd,
                      (void*)(uintptr_t)encoded);
    if (!(newIoEvents & IO_EVENT_READ) && readEventActivated)
        kqueueControl(base->kqueueFd, EV_DELETE| EV_ONESHOT | EV_EOF, EVFILT_READ, fd, object);
    if ((newIoEvents & IO_EVENT_WRITE) && !writeEventActivated)
        kqueueControl(base->kqueueFd,
                      EV_ADD | EV_ONESHOT | EV_EOF,
                      EVFILT_WRITE,
                      fd,
                      (void*)(uintptr_t)encoded);
    if (!(newIoEvents & IO_EVENT_WRITE) && writeEventActivated)
        kqueueControl(base->kqueueFd, EV_DELETE | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, object);
  }
}

void kqueueNextFinishedOperation(asyncBase *base)
{
  int nfds, n;
  struct kevent events[MAX_EVENTS];
  kqueueBase *localBase = (kqueueBase *)base;
  if (!loopThreadEnter(base))
    return;

  while (1) {
    do {
      if (!executeGlobalQueue(base)) {
        unsigned threadsRunning = loopThreadExit(base);
        if (threadsRunning)
          kqueueEnqueue(base, 0);
        return;
      }

      // UINT32_MAX = wait with no timeout: an idle base blocks until queue
      // traffic, a timer-arm kick or kernel readiness supplies a doorbell.
      uint32_t sleepMs = timerLoopPrepareSleep(base, messageLoopThreadId, getMonotonicTicks(), 1000);
      struct timespec timeout;
      timeout.tv_sec = sleepMs / 1000;
      timeout.tv_nsec = (long)(sleepMs % 1000) * 1000000;
      nfds = kevent(localBase->kqueueFd, 0, 0, events, MAX_EVENTS,
                    sleepMs == UINT32_MAX ? 0 : &timeout);
      timerLoopCancelSleep(base, messageLoopThreadId);

      // Unconditional sweep (the modulo election is gone): an idle pass costs
      // one relaxed load, and the wakeup handshake relies on whichever thread
      // the kick lands on doing the sweep itself
      processTimeoutQueue(base, getMonotonicTicks());
    } while (nfds <= 0 && errno == EINTR);

    for (n = 0; n < nfds; n++) {
      uint64_t envelope = (uint64_t)(uintptr_t)events[n].udata;
      if (envelope == 0) {
        // EVFILT_USER with EV_CLEAR stays registered, no re-add needed
      } else if (envelope & udataTimer) {
        uintptr_t timerId;
        aioObjectRoot *decoded;
        __tagged_pointer_decode(events[n].udata, (void**)&decoded, &timerId);
        aioTimer *timer = (aioTimer*)decoded;
        uintptr_t armedGeneration = 0;
        aioObjectRoot *armedObject = 0;
        uintptr_t armedObjectGeneration = 0;
        uintptr_t armedEventIncarnation = 0;
        switch (reactorTimerDecodeIdentHandle(timer,
                                              timerId,
                                              (uintptr_t)events[n].ident,
                                              &armedGeneration,
                                              &armedObject,
                                              &armedObjectGeneration,
                                              &armedEventIncarnation)) {
          case rteIgnore:
            // stale doorbell: a previous arming's ident, timer disarmed
            break;

          case rteUserEvent: {
            aioUserEvent *event = (aioUserEvent*)timer->op;
            eventTimerSignalTick(event,
                                 armedGeneration,
                                 armedEventIncarnation);
            break;
          }

          case rteExpireOperation:
            if (!opCancel(timer->op,
                          armedGeneration,
                          aosTimeout,
                          armedObject,
                          armedObjectGeneration))
              recordStaleHandleDrop(base);
            break;
        }
      } else {
        aioObjectRoot *object;
        uintptr_t carriedGen;
        if (!objectHandleDecode(envelope, &object, &carriedGen))
          continue;
        uint32_t bits = (events[n].flags & EV_EOF) ? COMBINER_TAG_ERROR : 0;
        if (events[n].filter == EVFILT_READ) {
          bits |= COMBINER_TAG_PROGRESS_READ;
        } else if (events[n].filter == EVFILT_WRITE) {
          bits |= COMBINER_TAG_PROGRESS_WRITE;
        }
        if ((bits & COMBINER_TAG_PROGRESS_MASK) &&
            !combinerPushValidated(object,
                                   carriedGen,
                                   bits))
          recordStaleHandleDrop(base);
      }
    }
  }
}


aioObject *kqueueNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  aioObject *object = (aioObject*)objectAlloc(&objectPool,
                                               sizeof(aioObject),
                                               TAGGED_POINTER_ALIGNMENT);
  if (!object)
    return 0;
  uint64_t encoded;
  if (!objectHandleTryEncode(&object->root, &encoded)) {
    // Every pooled fd cell was encodable when first published, keeps the same
    // address, and generation-range exhaustion is tombstoned instead of pooled.
    // Failure therefore proves this is a fresh, unpublished allocation.
    alignedFree(object);
    return 0;
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

// Return the generation-headed cell immediately. Harvested events validate
// only Head and cannot enter the combiner after the generation bump.
static void kqueueReleaseObject(aioObject *object)
{
  if (__uintptr_atomic_load(&object->root.Head.gen, amoRelaxed) >
      objectHandleGenMask()) {
    free(object->buffer.ptr);
    object->buffer.ptr = 0;
    object->buffer.totalSize = 0;
    ASAN_POISON_MEMORY_REGION((uint8_t*)object + sizeof(AsyncObjectHead),
                              sizeof(aioObject) - sizeof(AsyncObjectHead));
    return; // permanent tombstone: exhausted generation is never published
  }
  objectFree(&objectPool, object, sizeof(aioObject));
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

  // Already-harvested kevents carry the old generation and may touch only
  // Head. The rest of the cell can return to its type-stable pool immediately.
  kqueueReleaseObject(object);
}

void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  op->timerId = 0;
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  if (!timer)
    return;
  timer->base = base;
  reactorTimerInitializeSharedState(timer);
  // Seed the permanent base into the high half; the low half is the arm
  // sequence, advanced by reactorTimerArmIdent with each arming
  timer->fd = (intptr_t)((uintptr_t)__sync_fetch_and_add(&timerIdCounter, 1) << rtIdentSeqBits);
  timer->op = op;
  timer->registeredBase = 0;
  timer->kind = rtkUnknown;
  timer->broken = 0;
  op->timerId = timer;
}

static aioTimer *kqueuePrepareTimer(asyncOpRoot *op,
                                    asyncBase *base,
                                    ReactorTimerKind kind)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (!timer) {
    kqueueInitializeTimer(base, op);
    timer = (aioTimer*)op->timerId;
  }
  if (!timer || !reactorTimerBindKind(timer, kind))
    return 0;

  uintptr_t seqMask = (((uintptr_t)1) << rtIdentSeqBits) - 1;
  if (!timer->broken && ((uintptr_t)timer->fd & seqMask) != seqMask)
    return timer;

  // Allocate and initialize the replacement before tombstoning the old cell.
  // On OOM the old disarmed cell remains attached and can be retried safely.
  aioTimer *replacement = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  if (!replacement)
    return 0;
  replacement->base = base;
  reactorTimerInitializeSharedState(replacement);
  replacement->fd =
    (intptr_t)((uintptr_t)__sync_fetch_and_add(&timerIdCounter, 1) << rtIdentSeqBits);
  replacement->op = op;
  replacement->registeredBase = 0;
  replacement->kind = timer->kind;
  replacement->broken = 0;

  reactorTimerDisarm(timer);
  timer->broken = 1; // permanent tombstone: a stale kevent may retain its pointer
  op->timerId = replacement;
  return replacement;
}

static int kqueueArmTimer(asyncOpRoot *op,
                          asyncBase *target,
                          ReactorTimerKind kind,
                          uintptr_t generation,
                          uint64_t period)
{
  struct kevent event;
  aioTimer *timer = kqueuePrepareTimer(op, target, kind);
  if (!timer)
    return 0;
  void *udata = reactorTimerArmIdentGeneration(timer, op, generation);
  EV_SET(&event,
         timer->fd,
         EVFILT_TIMER,
         EV_ADD | EV_ENABLE | (kind == rtkUserEvent ? 0 : EV_ONESHOT),
         NOTE_USECONDS,
         period,
         udata);
  int result;
  do {
    result = kevent(((kqueueBase*)target)->kqueueFd, &event, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    reactorTimerDisarm(timer);
    return 0;
  }
  timer->base = target;
  return 1;
}

void kqueueStartTimer(asyncOpRoot *op)
{
  if (!kqueueArmTimer(op,
                      op->object->base,
                      rtkOperation,
                      opGetGeneration(op),
                      op->timeout))
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
}

static int kqueueStopTimerInternal(asyncOpRoot *op)
{
  struct kevent event;
  aioTimer *timer = (aioTimer*)op->timerId;
  if (!timer)
    return 1;
  reactorTimerDisarm(timer);
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DELETE, 0, 0, 0);
  // ENOENT is the normal outcome whenever the oneshot already fired and was
  // harvested (every expiry-driven stop) or the timer was never armed
  int result;
  do {
    result = kevent(((kqueueBase*)timer->base)->kqueueFd,
                    &event,
                    1,
                    0,
                    0,
                    0);
  } while (result == -1 && errno == EINTR);
  if (result == 0 || errno == ENOENT)
    return 1;

  // Do not recycle a cell whose periodic knote could still be live. A best-
  // effort disable prevents a wake storm; either way the next arm rotates to
  // a fresh physical cell and stale deliveries see this cell's zero tag.
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DISABLE, 0, 0, 0);
  do {
    result = kevent(((kqueueBase*)timer->base)->kqueueFd,
                    &event,
                    1,
                    0,
                    0,
                    0);
  } while (result == -1 && errno == EINTR);
  timer->broken = 1;
  return 1;
}

void kqueueStopTimer(asyncOpRoot *op)
{
  (void)kqueueStopTimerInternal(op);
}

int kqueueActivate(aioUserEvent *op)
{
  kqueueEnqueue(op->base, &op->root);
  return 1;
}

int kqueueUpdateEventTimer(aioUserEvent *event,
                           EventTimerUpdate update,
                           uintptr_t generation,
                           uint64_t period)
{
  switch (update) {
    case etuStart:
      return kqueueArmTimer(&event->root,
                            event->base,
                            rtkUserEvent,
                            generation,
                            period);
    case etuStop:
      return kqueueStopTimerInternal(&event->root);
    case etuRearm:
      // EVFILT_TIMER remains active after a periodic delivery.
      break;
    case etuConsume:
      break;
  }
  return 1;
}

void kqueueReleaseUserEvent(aioUserEvent *event)
{
  aioTimer *timer = (aioTimer*)event->root.timerId;
  if (!timer) {
    if (__uintptr_atomic_load(&event->incarnation, amoRelaxed) != 0)
      concurrentQueuePush(event->root.objectPool, event);
    return;
  }
  assert(__uintptr_atomic_load(&timer->tag, amoAcquire) == 0 &&
         "Recycling an armed user-event timer");
  // EV_DELETE/expiry has made the knote inert, but harvested kevents may
  // retain timer's address indefinitely. Park the immutable event/timer pair
  // together; the next incarnation continues the timer's ident sequence.
  if (__uintptr_atomic_load(&event->incarnation, amoRelaxed) != 0)
    concurrentQueuePush(event->root.objectPool, event);
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
