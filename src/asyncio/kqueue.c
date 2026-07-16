#include "asyncioImpl.h"
#include "reactor.h"
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

// Base part of the composite kevent ident (reactor.h). Process-global
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
int kqueueInitializeUserEvent(aioUserEvent *event);
int kqueueActivate(aioUserEvent *op);
int kqueueUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period);
uint64_t kqueueConsumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period);
void kqueueReleaseUserEvent(aioUserEvent *event);
AsyncOpStatus kqueueAsyncConnect(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncAccept(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncRead(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncWrite(asyncOpRoot *opptr);
AsyncOpStatus kqueueAsyncReadMsg(asyncOpRoot *op);
AsyncOpStatus kqueueAsyncWriteMsg(asyncOpRoot *op);

static struct asyncImpl kqueueImpl = {combinerTaskHandler,    kqueueEnqueue,         kqueuePostEmptyOperation, kqueueNextFinishedOperation,
                                      kqueueNewAioObject,     kqueueNewAsyncOp,      kqueueCancelAsyncOp,      kqueueDeleteObject,
                                      kqueueInitializeTimer,  kqueueStartTimer,      kqueueStopTimer,          kqueueInitializeUserEvent,
                                      kqueueActivate,         kqueueAsyncConnect,    kqueueAsyncAccept,        kqueueAsyncRead,
                                      kqueueAsyncWrite,       kqueueAsyncReadMsg,    kqueueAsyncWriteMsg,      kqueueWakeupLoop,
                                      kqueueUpdateEventTimer, kqueueConsumeEventTimerTick, kqueueReleaseUserEvent};

static int kqueueControl(int kqueueFd, uint16_t flags, int16_t filter, uintptr_t ident, void *ptr)
{
  struct kevent event;
  EV_SET(&event, ident, filter, flags, 0, 0, ptr);
  int result;
  do {
    result = kevent(kqueueFd, &event, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  return result;
}

static int getFd(aioObject *object)
{
  switch (object->root.header.objectType) {
    case ioObjectDevice: return object->hDevice;
    case ioObjectSocket: return object->hSocket;
    default: return -1;
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

    if (kqueueControl(base->kqueueFd, EV_ADD | EV_CLEAR, EVFILT_USER, 1, 0) == -1) {
      close(base->kqueueFd);
      free(base);
      return 0;
    }
  }

  return (asyncBase*)base;
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
  kqueueBase *base = (kqueueBase*)object->header.base;
  aioObject *fdObject = (object->header.objectType == ioObjectDevice || object->header.objectType == ioObjectSocket) ? (aioObject*)object : 0;
  uint32_t oldIoEvents = fdObject ? combinerActiveIoEvents(object) : 0;
  uint32_t progress = sig & COMBINER_TAG_PROGRESS_MASK;
  uint32_t ioEvents = fdObject ? progress | ((sig & COMBINER_TAG_ERROR) ? IO_EVENT_ERROR : 0) : 0;

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
    if (((newIoEvents & IO_EVENT_READ) && !readEventActivated) || ((newIoEvents & IO_EVENT_WRITE) && !writeEventActivated))
      encoded = reactorHandleEncode(&fdObject->root.header);

    if ((newIoEvents & IO_EVENT_READ) && !readEventActivated)
      kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_READ, fd, (void*)(uintptr_t)encoded);
    if (!(newIoEvents & IO_EVENT_READ) && readEventActivated)
      kqueueControl(base->kqueueFd, EV_DELETE | EV_ONESHOT | EV_EOF, EVFILT_READ, fd, object);
    if ((newIoEvents & IO_EVENT_WRITE) && !writeEventActivated)
      kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, (void*)(uintptr_t)encoded);
    if (!(newIoEvents & IO_EVENT_WRITE) && writeEventActivated)
      kqueueControl(base->kqueueFd, EV_DELETE | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, object);
  }
}

void kqueueNextFinishedOperation(asyncBase *base)
{
  int nfds, n;
  struct kevent events[MAX_EVENTS];
  kqueueBase *localBase = (kqueueBase*)base;
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
      nfds = kevent(localBase->kqueueFd, 0, 0, events, MAX_EVENTS, sleepMs == UINT32_MAX ? 0 : &timeout);
      timerLoopCancelSleep(base, messageLoopThreadId);

      // Unconditional sweep (the modulo election is gone): an idle pass costs
      // one relaxed load, and the wakeup handshake relies on whichever thread
      // the kick lands on doing the sweep itself
      processTimeoutQueue(base, getMonotonicTicks());
    } while (nfds <= 0 && errno == EINTR);

    for (n = 0; n < nfds; n++) {
      uint64_t envelope = (uint64_t)(uintptr_t)events[n].udata;
      // EVFILT_USER with EV_CLEAR stays registered, no re-add needed
      if (envelope == 0)
        continue;

      objectHeader *header;
      uint64_t generation;
      header = reactorHandleDecode(envelope, &generation);
      switch (objectHeaderGetType(header)) {
        case ohtUserEvent: {
          aioUserEvent *event = (aioUserEvent*)header;
          if (!eventTimerTryClaimReference(event, generation))
            break;
          eventManualReady(event);
          eventDecrementReference(event, 1);
          break;
        }

        case ohtTimer: {
          aioTimer *timer = (aioTimer*)header;
          uint64_t armedGeneration = 0;
          aioObjectRoot *armedObject = 0;
          uint64_t armedObjectGeneration = 0;
          uint64_t armedEventGeneration = 0;
          switch (reactorTimerDecodeIdentHandle(timer,
                                                generation,
                                                (uint64_t)events[n].ident,
                                                &armedGeneration,
                                                &armedObject,
                                                &armedObjectGeneration,
                                                &armedEventGeneration)) {
            case rteIgnore: break;

            case rteUserEvent:
              eventTimerSignal((aioUserEvent*)timer->target, armedGeneration, armedEventGeneration, (uint64_t)events[n].data);
              break;

            case rteExpireOperation:
              (void)opCancel((asyncOpRoot*)timer->target, armedGeneration, aosTimeout, armedObject, armedObjectGeneration);
              break;
          }
          break;
        }

        case ohtObject: {
          aioObject *object = (aioObject*)header;
          uint32_t bits = (events[n].flags & EV_EOF) ? COMBINER_TAG_ERROR : 0;
          if (events[n].filter == EVFILT_READ)
            bits |= COMBINER_TAG_PROGRESS_READ;
          else if (events[n].filter == EVFILT_WRITE)
            bits |= COMBINER_TAG_PROGRESS_WRITE;
          if (bits & COMBINER_TAG_PROGRESS_MASK)
            (void)combinerPushValidated(&object->root, generation, bits);
          break;
        }
      }
    }
  }
}

aioObject *kqueueNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  aioObject *object = (aioObject*)objectAlloc(&objectPool, sizeof(aioObject), TAGGED_POINTER_ALIGNMENT);
  if (!object)
    return 0;

  initObjectRoot(&object->root, base, type, (aioObjectDestructor*)kqueueDeleteObject);
  switch (type) {
    case ioObjectDevice: object->hDevice = *(iodevTy*)data; break;
    case ioObjectSocket: object->hSocket = *(socketTy*)data; break;
    default: break;
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

void kqueueDeleteObject(aioObject *object)
{
  switch (object->root.header.objectType) {
    case ioObjectDevice:
      close(object->hDevice);
      object->hDevice = -1;
      break;
    case ioObjectSocket:
      close(object->hSocket);
      object->hSocket = -1;
      break;
    default: break;
  }

  objectFree(&objectPool, object, sizeof(aioObject));
}

static aioTimer *kqueueNewTimer(asyncBase *base, void *target, ReactorTimerKind kind)
{
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  if (!timer)
    return 0;
  reactorTimerInitializeSharedState(timer);
  timer->header.base = base;
  // Seed the permanent base into the high half; the low half is the arm
  // sequence, advanced by reactorTimerArmIdent with each arming
  timer->fd = (intptr_t)((uintptr_t)__sync_fetch_and_add(&timerIdCounter, 1) << rtIdentSeqBits);
  timer->target = target;
  timer->header.timer.kind = (uint8_t)kind;
  return timer;
}

void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  op->timerId = kqueueNewTimer(base, op, rtkOperation);
}

static aioTimer *kqueuePrepareTimer(asyncOpRoot *op, asyncBase *base)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (!timer) {
    kqueueInitializeTimer(base, op);
    timer = (aioTimer*)op->timerId;
  }
  if (!timer)
    return 0;
  assert(timer->header.timer.kind == rtkOperation);

  uintptr_t seqMask = (((uintptr_t)1) << rtIdentSeqBits) - 1;
  if (!timer->header.timer.broken && ((uintptr_t)timer->fd & seqMask) != seqMask)
    return timer;

  // Allocate and initialize the replacement before tombstoning the old cell.
  // On OOM the old disarmed cell remains attached and can be retried safely.
  aioTimer *replacement = kqueueNewTimer(base, op, rtkOperation);
  if (!replacement)
    return 0;

  reactorTimerDisarm(timer);
  // Permanent tombstone: a stale kevent may retain this timer's pointer.
  timer->header.timer.broken = 1;
  op->timerId = replacement;
  return replacement;
}

static int kqueueArmTimer(asyncOpRoot *op)
{
  asyncBase *target = op->object->header.base;
  struct kevent event;
  aioTimer *timer = kqueuePrepareTimer(op, target);
  if (!timer)
    return 0;
  void *udata = reactorTimerArmIdentGeneration(timer, op, opGetGeneration(op));
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_ADD | EV_ENABLE | EV_ONESHOT, NOTE_USECONDS, op->timeout, udata);
  int result;
  do {
    result = kevent(((kqueueBase*)target)->kqueueFd, &event, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    reactorTimerDisarm(timer);
    return 0;
  }
  timer->header.base = target;
  return 1;
}

void kqueueStartTimer(asyncOpRoot *op)
{
  if (!kqueueArmTimer(op))
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
}

static int kqueueDisarmTimer(aioTimer *timer)
{
  struct kevent event;
  reactorTimerDisarm(timer);
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DELETE, 0, 0, 0);
  // ENOENT is the normal outcome whenever the oneshot already fired and was
  // harvested (every expiry-driven stop) or the timer was never armed
  int result;
  do {
    result = kevent(((kqueueBase*)timer->header.base)->kqueueFd, &event, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  if (result == 0 || errno == ENOENT)
    return 1;

  // Do not recycle a cell whose knote could still be live. A best-effort
  // disable prevents a wake storm; either way the next arm rotates to a fresh
  // physical cell and stale deliveries see this cell's zero tag.
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DISABLE, 0, 0, 0);
  do {
    result = kevent(((kqueueBase*)timer->header.base)->kqueueFd, &event, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  timer->header.timer.broken = 1;
  return 1;
}

void kqueueStopTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (timer)
    (void)kqueueDisarmTimer(timer);
}

int kqueueInitializeUserEvent(aioUserEvent *event)
{
  uint64_t encoded = reactorHandleEncode(&event->header);
  event->activationId = (uint64_t)(uintptr_t)event;
  void *udata = (void*)(uintptr_t)encoded;
  if (kqueueControl(((kqueueBase*)event->header.base)->kqueueFd, EV_ADD | EV_CLEAR, EVFILT_USER, (uintptr_t)event, udata) == 0)
    return 1;
  event->activationId = UINT64_MAX;
  return 0;
}

int kqueueActivate(aioUserEvent *event)
{
  struct kevent kernelEvent;
  EV_SET(&kernelEvent, event->activationId, EVFILT_USER, 0, NOTE_TRIGGER, 0, (void*)(uintptr_t)reactorHandleEncode(&event->header));
  int result;
  do {
    result = kevent(((kqueueBase*)event->header.base)->kqueueFd, &kernelEvent, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  return result == 0;
}

static aioTimer *kqueuePrepareEventTimer(aioUserEvent *event)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  if (!timer) {
    timer = kqueueNewTimer(event->header.base, event, rtkUserEvent);
    if (!timer)
      return 0;
    eventTimerStore(event, timer, amoRelaxed);
  }

  uintptr_t seqMask = (((uintptr_t)1) << rtIdentSeqBits) - 1;
  if (!timer->header.timer.broken && ((uintptr_t)timer->fd & seqMask) != seqMask)
    return timer;

  aioTimer *replacement = kqueueNewTimer(event->header.base, event, rtkUserEvent);
  if (!replacement)
    return 0;

  reactorTimerDisarm(timer);
  timer->header.timer.broken = 1;
  eventTimerStore(event, replacement, amoRelaxed);
  return replacement;
}

static int kqueueArmEventTimer(aioUserEvent *event, uint32_t generation, uint64_t period)
{
  struct kevent kernelEvent;
  aioTimer *timer = kqueuePrepareEventTimer(event);
  if (!timer)
    return 0;
  void *udata = reactorTimerArmEventIdentGeneration(timer, event, generation);
  EV_SET(&kernelEvent, timer->fd, EVFILT_TIMER, EV_ADD | EV_ENABLE, NOTE_USECONDS, period, udata);
  int result;
  do {
    result = kevent(((kqueueBase*)event->header.base)->kqueueFd, &kernelEvent, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  if (result == -1) {
    reactorTimerDisarm(timer);
    return 0;
  }
  timer->header.base = event->header.base;
  return 1;
}

int kqueueUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period)
{
  switch (update) {
    case etuStart: return kqueueArmEventTimer(event, generation, period);
    case etuStop: {
      aioTimer *timer = eventTimerLoad(event, amoRelaxed);
      assert(timer && "Stopping a user-event timer which was never armed");
      return kqueueDisarmTimer(timer);
    }
  }
  return 1;
}

uint64_t kqueueConsumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period)
{
  __UNUSED(event);
  __UNUSED(generation);
  __UNUSED(period);
  // EVFILT_TIMER is periodic and kevent.data already carried the exact batch.
  return published;
}

void kqueueReleaseUserEvent(aioUserEvent *event)
{
  if (event->activationId != UINT64_MAX) {
    int result = kqueueControl(((kqueueBase*)event->header.base)->kqueueFd, EV_DELETE, EVFILT_USER, event->activationId, 0);
    assert((result == 0 || errno == ENOENT) && "Failed to remove a user-event knote");
    (void)result;
    event->activationId = UINT64_MAX;
  }
#ifndef NDEBUG
  aioTimer *timer = eventTimerLoad(event, amoAcquire);
  if (timer)
    assert(__uint64_atomic_load(&timer->header.tag.low, amoAcquire) == 0 && "Recycling an armed user-event timer");
#endif
  // EV_DELETE/expiry has made the knote inert, but harvested kevents may
  // retain timer's address indefinitely. Park the immutable event/timer pair
  // together; the next incarnation continues the timer's ident sequence.
  objectFree(&event->header.base->eventPool, event, sizeof(aioUserEvent));
}

AsyncOpStatus kqueueAsyncConnect(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);
  if (op->state == 0) {
    op->state = 1;
    struct sockaddr_storage sa;
    socklen_t saLen = hostAddressToSockaddr(&op->host, &sa);
    int result = connect(fd, (struct sockaddr*)&sa, saLen);
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
  op->acceptSocket = accept(fd, (struct sockaddr*)&clientAddr, &clientAddrSize);

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
        return socketStatusFromErrno(errno);
      sb->dataSize = (size_t)bytesRead;

      if (copyFromBuffer(op->buffer, &op->bytesTransferred, sb, op->transactionSize) || !(opptr->flags & afWaitAll))
        break;
    }

    return aosSuccess;
  } else {
    ssize_t bytesRead = read(fd, (uint8_t*)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);

    if (bytesRead > 0) {
      op->bytesTransferred += (size_t)bytesRead;
      if (op->root.flags & afWaitAll && op->bytesTransferred < op->transactionSize)
        return aosPending;
      else
        return aosSuccess;
    } else if (bytesRead == 0) {
      return op->transactionSize - op->bytesTransferred > 0 ? aosDisconnected : aosSuccess;
    } else {
      return socketStatusFromErrno(errno);
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
    bytesWritten = write(fd, (uint8_t*)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
    sigpipeGuardLeave(&guard, bytesWritten == -1 && errno == EPIPE);
  } else {
    bytesWritten = write(fd, (uint8_t*)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
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
    return socketStatusFromErrno(errno);
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
    return socketStatusFromErrno(errno);
  }
}

AsyncOpStatus kqueueAsyncWriteMsg(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);

  struct sockaddr_storage remoteAddress;
  socklen_t addrLen = hostAddressToSockaddr(&op->host, &remoteAddress);
  ssize_t result = sendto(fd, op->buffer, op->transactionSize, 0, (struct sockaddr*)&remoteAddress, addrLen);
  if (result != -1) {
    return aosSuccess;
  }

  return socketStatusFromErrno(errno);
}
