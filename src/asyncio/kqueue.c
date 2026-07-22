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
static volatile uint64_t timerIdCounter;

enum {
  timerIdentSeqBits = (int)(sizeof(uint64_t) * 4)
};

static uint64_t timerNewIdentBase(void)
{
  return __uint64_atomic_fetch_and_add(&timerIdCounter, 1, amoRelaxed) << timerIdentSeqBits;
}

static uint64_t timerNextIdent(aioTimer *timer)
{
  uint64_t seqMask = (1ULL << timerIdentSeqBits) - 1;
  uint64_t ident = (uint64_t)timer->fd + 1;
  return (ident & seqMask) ? ident : (timerNewIdentBase() | 1);
}

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig);
void kqueueEnqueue(asyncBase *base, asyncOpRoot *op);
void kqueueWakeupLoop(asyncBase *base);
void kqueueNextFinishedOperation(asyncBase *base);
aioObject *kqueueNewAioObject(asyncBase *base, IoObjectTy type, void *data);
void kqueueDeleteObject(aioObject *object);
void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op);
void kqueueStartTimer(asyncOpRoot *op);
void kqueueStopTimer(asyncOpRoot *op);
int kqueueInitializeUserEvent(aioUserEvent *event);
int kqueueActivate(aioUserEvent *op);
int kqueueUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period);
uint64_t kqueueConsumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period);
void kqueueReleaseUserEvent(aioUserEvent *event);
AsyncOpStatus kqueueAsyncWrite(asyncOpRoot *opptr);

static struct asyncImpl kqueueImpl = {
  combinerTaskHandler,
  kqueueEnqueue,
  kqueueNextFinishedOperation,
  kqueueNewAioObject,
  newAsyncOp,
  cancelAsyncOp,
  kqueueInitializeTimer,
  kqueueStartTimer,
  kqueueStopTimer,
  kqueueInitializeUserEvent,
  kqueueActivate,
  connectSyscall,
  acceptSyscall,
  readSyscall,
  kqueueAsyncWrite,
  readMsgSyscall,
  writeMsgSyscall,
  kqueueWakeupLoop,
  kqueueUpdateEventTimer,
  kqueueConsumeEventTimerTick,
  kqueueReleaseUserEvent
};

// Single-change submission on the configuration plane; every such kevent
// call shares this EINTR retry. errno of the final attempt is preserved.
static int kqueueSubmit(int kqueueFd, const struct kevent *event)
{
  int result;
  do {
    result = kevent(kqueueFd, event, 1, 0, 0, 0);
  } while (result == -1 && errno == EINTR);
  return result;
}

static int kqueueControl(int kqueueFd, uint16_t flags, int16_t filter, uintptr_t ident, uint64_t envelope)
{
  struct kevent event;
  EV_SET(&event, ident, filter, flags, 0, 0, (void*)(uintptr_t)envelope);
  return kqueueSubmit(kqueueFd, &event);
}

// Triggers the base's own EVFILT_USER doorbell knote. Intentionally a single
// kevent call with no EINTR retry: doorbell posts are fire-and-forget.
static void kqueueDoorbell(kqueueBase *base)
{
  struct kevent ev;
  EV_SET(&ev, 1, EVFILT_USER, 0, NOTE_TRIGGER, 0, 0);
  kevent(base->kqueueFd, &ev, 1, 0, 0, 0);
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
  concurrentQueuePush(&base->globalQueue, op);
  if (enqueueNeedsDoorbell(base, op))
    kqueueDoorbell((kqueueBase*)base);
}

void kqueueWakeupLoop(asyncBase *base)
{
  // Pure kick: trigger the user event without a queue node. One sleeper
  // wakes; the loop's udata==0 branch is a no-op and EV_CLEAR resets the
  // event on delivery
  kqueueDoorbell((kqueueBase*)base);
}

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig)
{
  kqueueBase *base = (kqueueBase*)object->header.base;
  aioObject *fdObject = (object->header.objectType == ioObjectDevice || object->header.objectType == ioObjectSocket) ? (aioObject*)object : 0;
  CombinerPassEvents pass = reactorCombinerCore(object, fdObject, op, sig);

  if (fdObject && !__uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    int fd = getFd(fdObject);
    uint32_t newIoEvents = combinerActiveIoEvents(object);
    unsigned readEventActivated = (pass.oldIoEvents & IO_EVENT_READ) && !(pass.ioEvents & IO_EVENT_READ);
    unsigned writeEventActivated = (pass.oldIoEvents & IO_EVENT_WRITE) && !(pass.ioEvents & IO_EVENT_WRITE);
    uint64_t encoded = 0;
    if (((newIoEvents & IO_EVENT_READ) && !readEventActivated) || ((newIoEvents & IO_EVENT_WRITE) && !writeEventActivated))
      encoded = kernelHandleEncode(&fdObject->root.header);

    if ((newIoEvents & IO_EVENT_READ) && !readEventActivated)
      kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_READ, fd, encoded);
    if (!(newIoEvents & IO_EVENT_READ) && readEventActivated)
      kqueueControl(base->kqueueFd, EV_DELETE | EV_ONESHOT | EV_EOF, EVFILT_READ, fd, 0);
    if ((newIoEvents & IO_EVENT_WRITE) && !writeEventActivated)
      kqueueControl(base->kqueueFd, EV_ADD | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, encoded);
    if (!(newIoEvents & IO_EVENT_WRITE) && writeEventActivated)
      kqueueControl(base->kqueueFd, EV_DELETE | EV_ONESHOT | EV_EOF, EVFILT_WRITE, fd, 0);
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
      executeGlobalQueue(base);

      // Checked after the drain (already-queued callbacks run) and before
      // every kernel wait; the exit re-rings the doorbell while other threads
      // remain registered, so one postQuitOperation reaches every sleeper
      if (__uintptr_atomic_load(&base->quitRequested, amoAcquire)) {
        if (loopThreadExit(base))
          kqueueWakeupLoop(base);
        return;
      }

      // UINT32_MAX = wait with no timeout: an idle base blocks until queue
      // traffic, a timer-arm kick or kernel readiness supplies a doorbell.
      uint64_t sleepFrom = getMonotonicTicks();
      uint32_t sleepMs = timerSleepShrinkElapsed(sleepFrom,
        timerLoopPrepareSleep(base, messageLoopThreadId, sleepFrom, 1000));
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
      uint64_t envelopeGeneration;
      header = kernelHandleDecode(envelope, &envelopeGeneration);
      switch (objectHeaderGetType(header)) {
        case ohtUserEvent: {
          aioUserEvent *event = (aioUserEvent*)header;
          if (!eventTimerTryClaimReference(event, envelopeGeneration))
            break;
          eventManualReady(event);
          eventDecrementReference(event, 1);
          break;
        }

        case ohtTimer: {
          aioTimer *timer = (aioTimer*)header;
          uint64_t eventIdent = (uint64_t)events[n].ident;

          // Safely loading timer config
          int isStale = __uint64_atomic_load(&timer->header.tag.low, amoAcquire) != eventIdent;
          uint64_t generation;
          aioObjectRoot *object = 0;
          uint64_t objectGeneration = 0;
          uint64_t eventGeneration = 0;
          generation = __uint64_atomic_load(&timer->header.tag.high, amoAcquire);
          if (timer->header.timer.kind == tkUserEvent)
            eventGeneration = __uint64_atomic_load(&timer->event.generation, amoAcquire);
          else {
            object = (aioObjectRoot*)__pointer_atomic_load((void *volatile*)&timer->operation.object, amoRelaxed);
            objectGeneration = __uint64_atomic_load(&timer->operation.objectGeneration, amoRelaxed);
          }
          isStale |= __uint64_atomic_load(&timer->header.tag.low, amoRelaxed) != eventIdent;
          if (isStale)
            break;

          if (timer->header.timer.kind == tkUserEvent)
            eventTimerSignal(timer->event.userEvent, generation, eventGeneration, (uint64_t)events[n].data);
          else
            (void)opCancel(timer->operation.op, generation, aosTimeout, object, objectGeneration);
          break;
        }

        case ohtObject: {
          aioObject *object = (aioObject*)header;
          // EV_EOF raises the error sweep only from the read filter, where it
          // means the peer half-closed. On EVFILT_WRITE it means our transmit
          // path is dead: plain write progress lets the parked ops fail on
          // the syscall per-op instead of a wrong-direction sweep.
          uint32_t bits = 0;
          if (events[n].filter == EVFILT_READ)
            bits = COMBINER_TAG_PROGRESS_READ | ((events[n].flags & EV_EOF) ? COMBINER_TAG_ERROR : 0);
          else if (events[n].filter == EVFILT_WRITE)
            bits = COMBINER_TAG_PROGRESS_WRITE;
          if (bits & COMBINER_TAG_PROGRESS_MASK)
            (void)combinerPushValidated(&object->root, envelopeGeneration, bits);
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
  ioBufferEnsureCapacity(&object->buffer, DEFAULT_SOCKET_BUFFER_SIZE);
  return object;
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

static aioTimer *kqueueNewTimer(asyncBase *base, TimerKind kind)
{
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  if (!timer)
    return 0;
  timerInitialize(timer);
  timer->header.base = base;
  // Seed the high half; the low half advances on each arm.
  timer->fd = (intptr_t)timerNewIdentBase();
  timer->header.timer.kind = (uint8_t)kind;
  return timer;
}

void kqueueInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  aioTimer *timer = kqueueNewTimer(base, tkOperation);
  if (timer)
    timer->operation.op = op;
  op->timerId = timer;
}

static aioTimer *kqueuePrepareTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)opEnsureTimerCell(op);
  if (!timer)
    return 0;
  assert(timer->header.timer.kind == tkOperation);
  return timer;
}

static int kqueueArmTimer(asyncOpRoot *op)
{
  asyncBase *target = op->object->header.base;
  struct kevent event;
  aioTimer *timer = kqueuePrepareTimer(op);
  if (!timer)
    return 0;
  uint64_t ident = timerNextIdent(timer);
  aioObjectRoot *object = op->object;

  timerPublishBegin(timer);
    timer->fd = (intptr_t)ident;
    __pointer_atomic_store((void *volatile*)&timer->operation.object, object, amoRelaxed);
    __uint64_atomic_store(&timer->operation.objectGeneration, objectHeaderGeneration(&object->header), amoRelaxed);
  timerPublishEnd(timer, opGetGeneration(op), ident);

  uint64_t envelope = kernelHandleEncode(&timer->header);
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_ADD | EV_ENABLE | EV_ONESHOT, NOTE_USECONDS, op->timeout, (void*)(uintptr_t)envelope);
  if (kqueueSubmit(((kqueueBase*)target)->kqueueFd, &event) == -1) {
    timerUnpublish(timer);
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
  timerUnpublish(timer);
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DELETE, 0, 0, 0);
  // ENOENT is the normal outcome whenever the oneshot already fired and was
  // harvested (every expiry-driven stop) or the timer was never armed
  if (kqueueSubmit(((kqueueBase*)timer->header.base)->kqueueFd, &event) == 0 || errno == ENOENT)
    return 1;

  // Keep a failed-delete knote quiet. Its old ident cannot match a later arm
  // of the same physical timer cell.
  EV_SET(&event, timer->fd, EVFILT_TIMER, EV_DISABLE, 0, 0, 0);
  (void)kqueueSubmit(((kqueueBase*)timer->header.base)->kqueueFd, &event);
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
  uint64_t encoded = kernelHandleEncode(&event->header);
  event->activationId = (uint64_t)(uintptr_t)event;
  if (kqueueControl(((kqueueBase*)event->header.base)->kqueueFd, EV_ADD | EV_CLEAR, EVFILT_USER, (uintptr_t)event, encoded) == 0)
    return 1;
  event->activationId = UINT64_MAX;
  return 0;
}

int kqueueActivate(aioUserEvent *event)
{
  struct kevent kernelEvent;
  EV_SET(&kernelEvent, event->activationId, EVFILT_USER, 0, NOTE_TRIGGER, 0, (void*)(uintptr_t)kernelHandleEncode(&event->header));
  return kqueueSubmit(((kqueueBase*)event->header.base)->kqueueFd, &kernelEvent) == 0;
}

static aioTimer *kqueuePrepareEventTimer(aioUserEvent *event)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  if (!timer) {
    timer = kqueueNewTimer(event->header.base, tkUserEvent);
    if (!timer)
      return 0;
    timer->event.userEvent = event;
    eventTimerStore(event, timer, amoRelaxed);
    poolCacheHandoff(timer);
  }

  return timer;
}

static int kqueueArmEventTimer(aioUserEvent *event, uint32_t generation, uint64_t period)
{
  struct kevent kernelEvent;
  aioTimer *timer = kqueuePrepareEventTimer(event);
  if (!timer)
    return 0;
  uint64_t ident = timerNextIdent(timer);

  timerPublishBegin(timer);
    timer->fd = (intptr_t)ident;
    __uint64_atomic_store(&timer->event.generation, eventHandleGeneration(event), amoRelease);
  timerPublishEnd(timer, generation, ident);

  void *envelope = (void*)kernelHandleEncode(&timer->header);
  EV_SET(&kernelEvent, timer->fd, EVFILT_TIMER, EV_ADD | EV_ENABLE, NOTE_USECONDS, period, envelope);
  if (kqueueSubmit(((kqueueBase*)event->header.base)->kqueueFd, &kernelEvent) == -1) {
    timerUnpublish(timer);
    return 0;
  }
  timer->header.base = event->header.base;
  return 1;
}

int kqueueUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period)
{
  switch (update) {
    case etuStart:
      return kqueueArmEventTimer(event, generation, period);
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
  // EVFILT_TIMER is periodic and the exact kevent batch arrived through the
  // 64-bit pending-tick word intact.
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
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  if (timer)
    assert(__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) == 0 && "Recycling an armed user-event timer");
#endif
  // EV_DELETE/expiry has made the knote inert, but harvested kevents may
  // retain timer's address indefinitely. Park the immutable event/timer pair
  // together; the next incarnation continues the timer's ident sequence.
  objectFree(&event->header.base->eventPool, event, sizeof(aioUserEvent));
}

AsyncOpStatus kqueueAsyncWrite(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  aioObject *object = (aioObject*)op->root.object;

  // Sockets are covered by SO_NOSIGPIPE from newSocketIo, and on Darwin/NetBSD
  // pipes are covered by F_SETNOSIGPIPE (needSigpipeGuard stays zero); the
  // masked branch is only reachable for pipes on FreeBSD.
  ssize_t bytesWritten = guardedWrite(object,
                                      getFd(object),
                                      (uint8_t*)op->buffer + op->bytesTransferred,
                                      op->transactionSize - op->bytesTransferred);
  return transferStatus(op, bytesWritten);
}
