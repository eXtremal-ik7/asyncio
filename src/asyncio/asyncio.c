#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include "asyncio/device.h"
#include "asyncio/socket.h"
#include "asyncioImpl.h"
#include "atomic.h"
#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef OS_WINDOWS
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#endif

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;

static void eventCoroutineDeliver(aioUserEvent *event);
static void eventTimerScheduleImpl(aioUserEvent *event,
                                   int enqueueOnly,
                                   int transferReference);
static void eventTimerSchedule(aioUserEvent *event, int enqueueOnly);
static void eventTimerProcessImpl(aioUserEvent *event, int releaseControlReference);

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
    eventCoroutineDeliver((aioUserEvent*)root);
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
  if (opCode == actAccept) {
    op->acceptSocket = INVALID_SOCKET;
    memset(&op->host, 0, sizeof(op->host));
  }
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

void initializeAsyncIo(AsyncInitFlags flags)
{
#ifdef OS_WINDOWS
  (void)flags;
  WSADATA wsadata;
  WSAStartup(MAKEWORD(2, 2), &wsadata);
#else
  if (flags & aiIgnoreSigpipe) {
    struct sigaction ignoreAction;
    memset(&ignoreAction, 0, sizeof(ignoreAction));
    ignoreAction.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &ignoreAction, 0);
    sigpipeIgnored = 1;
  }
#endif
}

asyncBase *createAsyncBase(AsyncMethod method, unsigned loopThreads)
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

  if (!base)
    return 0;

#ifndef NDEBUG
  base->opsCount = 0;
#endif
  timerWheelInit(base, getMonotonicTicks());
  memset(&base->globalQueue, 0, sizeof(base->globalQueue));
  memset(&base->eventPool, 0, sizeof(base->eventPool));
  base->messageLoopThreadCounter = 0;
  base->graceFrozen = 0;
  base->graceSlotCount = 0;
  base->graceSlotLimit = loopThreads ? loopThreads : 1;
  base->graceScanning = 0;
  base->graceLimbo = 0;
  base->gracePending = 0;
  base->gracePendingSlots = 0;
  base->gracePendingSeen = (uintptr_t*)malloc(sizeof(uintptr_t) * base->graceSlotLimit);
  base->graceSeen = (GraceSlot*)alignedMalloc(sizeof(GraceSlot) * base->graceSlotLimit, CACHE_LINE_SIZE);
  base->timerSleep = (TimerSleepSlot*)alignedMalloc(sizeof(TimerSleepSlot) * base->graceSlotLimit, CACHE_LINE_SIZE);
  for (unsigned i = 0; i < base->graceSlotLimit; i++) {
    base->graceSeen[i].seen = UINTPTR_MAX;   // empty slots never gate the limbo
    base->timerSleep[i].wakeTick = UINTPTR_MAX;  // nobody sleeps yet, kicks not needed
  }
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

static int eventTimerSnapshot(aioUserEvent *event,
                              uintptr_t *generation,
                              uint64_t *period,
                              int *counter)
{
  // Atomic payload fields make this a conforming single-writer seqlock without
  // seq-cst stores. Seeing a new period/counter through its acquire load also
  // acquires the preceding odd sequence store; coherence then prevents the
  // final recheck from reading the older even value. Conversely, acquiring a
  // new final even value publishes both payload stores. A reader never spins
  // on odd: the writer schedules a fresh pass after publishing matching even.
  uintptr_t first = __uintptr_atomic_load(&event->timerConfigSequence, amoAcquire);
  if (first & 1)
    return 0;
  uintptr_t snapshotPeriod = __uintptr_atomic_load(&event->timerConfigPeriod, amoAcquire);
  unsigned snapshotCounter = __uint_atomic_load(&event->timerConfigCounter, amoAcquire);
  uintptr_t second = __uintptr_atomic_load(&event->timerConfigSequence, amoRelaxed);
  if (first != second)
    return 0;
  *generation = first;
  *period = (uint64_t)snapshotPeriod;
  *counter = (int)snapshotCounter;
  return 1;
}

static uintptr_t eventTimerPublishConfig(aioUserEvent *event,
                                         uint64_t period,
                                         int counter,
                                         uintptr_t expectedGeneration)
{
  if (eventReferenceIsDeleting(event))
    return 0;

  // Timer control for one event is a single-controller API. The odd value is
  // therefore a diagnostic for a forbidden overlapping Start/Stop, not a
  // lock on which another caller waits.
  // Single-controller ordering (including hand-off between controller
  // threads) already happens outside this API; only the monotonic token is
  // needed here, so no payload acquire belongs on the writer path.
  uintptr_t old = __uintptr_atomic_load(&event->timerConfigSequence, amoRelaxed);
  if ((old & 1) || (expectedGeneration != UINTPTR_MAX && old != expectedGeneration)) {
    assert(!(old & 1) && "Overlapping user-event timer control calls");
    return 0;
  }
  // epoll stores (generation << 1) | armed in its publication word.
  assert(old <= (UINTPTR_MAX >> 1) - 2 && "User-event timer generation exhausted");
  // There is exactly one application-side writer by contract. Atomic stores
  // are needed for the concurrent applier reader, but writer arbitration is
  // not library work and must not add another locked RMW to every Start/Stop.
  __uintptr_atomic_store(&event->timerConfigSequence, old + 1, amoRelaxed);

  // A non-positive public counter means unlimited repetition and is encoded
  // as zero in the desired snapshot.
  unsigned finiteCounter = counter > 0 ? (unsigned)counter : 0;
  __uintptr_atomic_store(&event->timerConfigPeriod, (uintptr_t)period, amoRelease);
  __uint_atomic_store(&event->timerConfigCounter, finiteCounter, amoRelease);
  __uintptr_atomic_store(&event->timerConfigSequence, old + 2, amoRelease);
  eventTimerSchedule(event, 0);
  return old + 2;
}

static void eventTimerCancelGeneration(aioUserEvent *event, uintptr_t generation)
{
  if (generation)
    eventTimerPublishConfig(event, 0, 0, generation);
}

static void eventTimerScheduleImpl(aioUserEvent *event,
                                   int enqueueOnly,
                                   int transferReference)
{
  // RUNNING elects exactly one applier; DIRTY is its coalesced mailbox. Even
  // when an applier already exists, this release RMW publishes the preceding
  // config/tick/DELETE and prevents it from retiring as clean.
  unsigned old = __uint_atomic_fetch_or(&event->timerControlState,
                                              ecsRunning | ecsDirty,
                                              amoRelease);
  if (!(old & ecsRunning)) {
    if (enqueueOnly) {
      // Kernel signal producers transfer the reference claimed before their
      // generation CAS. Other producers retain here. In both cases the tagged
      // node is not made visible before its queued-control reference exists.
      if (!transferReference)
        eventIncrementReference(event, 1);
      event->base->methodImpl.enqueue(event->base, eventTimerControlNode(event));
    } else {
      assert(!transferReference);
      // Start/Stop/delete and coroutine delivery already own a reference for
      // the whole synchronous call. Do not add a second refcount round-trip
      // merely to run the applier on the same stack.
      eventTimerProcessImpl(event, 0);
    }
  } else if (transferReference) {
    // Another control reference (or the synchronous caller) already pins the
    // sole applier. DIRTY carries our publication, so our temporary kernel
    // publisher claim is no longer needed. RUNNING cannot retire underneath
    // this fetch_or: if its final CAS follows us, DIRTY makes that CAS fail.
    eventReferenceDropNonFinal(event, 1);
  }
}

static void eventTimerSchedule(aioUserEvent *event, int enqueueOnly)
{
  eventTimerScheduleImpl(event, enqueueOnly, 0);
}

// A decoded POSIX timer pointer is protected physically by the timer's grace
// period, but stop may release the last logical arm reference while this loop
// thread is between generation validation and mailbox publication. Claim a
// reference first. The zero-ref rollback is safe because that same grace
// period prevents storage reuse; on IOCP the callback rendezvous means the
// arm reference cannot reach zero until the callback returns.
static int eventTimerTryClaimPublisherReference(aioUserEvent *event)
{
  uintptr_t old = __uintptr_atomic_fetch_and_add(&event->tag, 1, amoRelaxed);
  uintptr_t refs = old & TAG_EVENT_REF_MASK;
  if (refs == 0) {
    __uintptr_atomic_fetch_and_add(&event->tag, (uintptr_t)0 - 1, amoRelaxed);
    return 0;
  }
  assert(refs != TAG_EVENT_REF_MASK && "Event reference overflow");
  return 1;
}

static int eventTimerPublishGenerationSignal(aioUserEvent *event,
                                             uintptr_t generation,
                                             int probe)
{
  volatile uint128Pair *bucket = &event->timerSignals;
  uint128Pair expected = __uint128_atomic_load_relaxed(bucket);
  if (generation == 0 || expected.high != (uint64_t)generation)
    return 0;
  if (!eventTimerTryClaimPublisherReference(event))
    return 0;

  for (;;) {
    // The applier changes the bucket generation before disarm/restart. A late
    // harvested tick may inspect the still-live storage, but can never publish
    // into a different schedule.
    if (expected.high != (uint64_t)generation) {
      eventDecrementReference(event, 1);
      return 0;
    }
    assert((expected.low == 0 || (int)(expected.low & 1) == probe) &&
           "Mixed user-event timer signal kinds");
    assert(expected.low <= UINT64_MAX - 2 && "User-event timer signal overflow");
    uint128Pair desired = {
      expected.low ? expected.low + 2 : (uint64_t)(2 | probe),
      expected.high
    };
    if (__uint128_atomic_compare_and_swap_relaxed(bucket, &expected, desired)) {
      // Transfer the publisher claim to a newly queued task, or release it if
      // an existing applier owns RUNNING and only needs our DIRTY bit.
      eventTimerScheduleImpl(event, 1, 1);
      return 1;
    }
  }
}

void eventTimerSignalTick(aioUserEvent *event, uintptr_t generation)
{
  eventTimerPublishGenerationSignal(event, generation, 0);
}

void eventTimerSignalProbe(aioUserEvent *event, uintptr_t generation)
{
  eventTimerPublishGenerationSignal(event, generation, 1);
}

static void eventCoroutineDeliver(aioUserEvent *event)
{
  uint128Pair expected = __uint128_atomic_load(&event->waiter);
  for (;;) {
    uint128Pair desired;
    if (expected.low) {
      desired.low = 0;
      desired.high = 0;
    } else {
      // An accepted delivery may finish after delete, but it must not create a
      // reusable credit in terminal state. An already installed waiter is
      // still completed exactly once by either this delivery or delete.
      if (eventReferenceIsDeleting(event))
        return;
      assert(expected.high != UINT64_MAX && "User-event coroutine credit overflow");
      desired.low = 0;
      desired.high = expected.high + 1;
    }
    if (__uint128_atomic_compare_and_swap(&event->waiter, &expected, desired)) {
      if (expected.low) {
        coroutineTy *coroutine = (coroutineTy*)(uintptr_t)expected.low;
        uintptr_t sleepGeneration = (uintptr_t)expected.high;
        if (sleepGeneration)
          eventTimerCancelGeneration(event, sleepGeneration);
        assert(coroutineIsMain() && "User-event callback must run in the main coroutine");
        coroutineCall(coroutine);
      }
      return;
    }
  }
}

static void eventCoroutineCancel(aioUserEvent *event)
{
  uint128Pair empty = { 0, 0 };
  uint128Pair old = __uint128_atomic_exchange(&event->waiter, empty);
  if (old.low) {
    coroutineTy *coroutine = (coroutineTy*)(uintptr_t)old.low;
    assert(coroutineIsMain() && "User-event cancellation must run in the main coroutine");
    coroutineCall(coroutine);
  }
}

static int eventTryConsumeCredit(aioUserEvent *event)
{
  if (eventReferenceIsDeleting(event))
    return 1;
  uint128Pair expected = __uint128_atomic_load(&event->waiter);
  for (;;) {
    assert(expected.low == 0 && "Only one coroutine may wait on a user event");
    if (expected.high == 0)
      return 0;
    uint128Pair desired = { 0, expected.high - 1 };
    if (__uint128_atomic_compare_and_swap(&event->waiter, &expected, desired))
      return 1;
  }
}

static int eventInstallWaiter(aioUserEvent *event, uintptr_t sleepGeneration)
{
  if (eventReferenceIsDeleting(event))
    return 0;

  // The waiter reference is released by the resumed coroutine, not by the
  // delivery/delete path that removes its pointer. This also covers the
  // activate-before-yield handshake implemented by coroutineCall/Yield.
  eventIncrementReference(event, 1);
  uint128Pair expected = __uint128_atomic_load(&event->waiter);
  for (;;) {
    if (eventReferenceIsDeleting(event)) {
      eventReferenceDropNonFinal(event, 1);
      return 0;
    }
    assert(expected.low == 0 && "Only one coroutine may wait on a user event");
    if (expected.high) {
      uint128Pair desiredCredit = { 0, expected.high - 1 };
      if (__uint128_atomic_compare_and_swap(&event->waiter, &expected, desiredCredit)) {
        eventReferenceDropNonFinal(event, 1);
        return 0;
      }
      continue;
    }

    uint128Pair desired = {
      (uint64_t)(uintptr_t)coroutineCurrent(),
      (uint64_t)sleepGeneration
    };
    if (__uint128_atomic_compare_and_swap(&event->waiter, &expected, desired)) {
      // Close may have won immediately after the pre-CAS check and already
      // drained an earlier control pass. Re-scheduling makes this publication
      // visible to the terminal waiter cancellation.
      if (eventReferenceIsDeleting(event))
        eventTimerSchedule(event, 1);
      return 1;
    }
  }
}

static int eventTimerStopApplied(aioUserEvent *event)
{
  if (!(event->timerState & etsArmed)) {
    event->timerState &= ~etsNeedsRearm;
    assert(!(event->timerState & etsHasReference));
    return 1;
  }

  uint128Pair noTicks = { 0, 0 };
  __uint128_atomic_exchange_relaxed(&event->timerSignals, noTicks);
  event->timerState &= ~etsNeedsRearm;
  if (!event->base->methodImpl.updateEventTimer(event,
                                                etuStop,
                                                event->timerAppliedGeneration,
                                                0))
    return 0;

  event->timerState &= ~etsArmed;
  if (event->timerState & etsHasReference) {
    event->timerState &= ~etsHasReference;
    // The RUNNING applier is independently pinned by its queued control
    // reference or by the synchronous caller.
    eventReferenceDropNonFinal(event, 1);
  }
  return 1;
}

static void eventTimerStartApplied(aioUserEvent *event)
{
  if (!event->root.timerId)
    event->base->methodImpl.initializeTimer(event->base, &event->root);
  if (!event->root.timerId)
    return;

  eventIncrementReference(event, 1); // kernel arming/callback reference
  event->timerState |= etsHasReference;
  uint128Pair ticks = { 0, (uint64_t)event->timerAppliedGeneration };
  __uint128_atomic_exchange_relaxed(&event->timerSignals, ticks);
  if (!event->base->methodImpl.updateEventTimer(event,
                                                etuStart,
                                                event->timerAppliedGeneration,
                                                event->timerAppliedPeriod)) {
    uint128Pair noTicks = { 0, 0 };
    __uint128_atomic_exchange_relaxed(&event->timerSignals, noTicks);
    event->timerState &= ~etsHasReference;
    eventReferenceDropNonFinal(event, 1);
    return;
  }
  event->timerState |= etsArmed;
}

static void eventTimerProcessImpl(aioUserEvent *event, int releaseControlReference)
{
  assert(__uint_atomic_load(&event->timerControlState, amoRelaxed) & ecsRunning);

  for (;;) {
    // Clear only DIRTY, retaining exclusive RUNNING ownership. Acquire pairs
    // with every producer's release fetch_or; a producer racing this pass
    // changes RUNNING back to RUNNING|DIRTY and makes the final CAS fail.
    __uint_atomic_fetch_and(&event->timerControlState,
                                  ~ecsDirty,
                                  amoAcquire);

    if (eventReferenceIsDeleting(event)) {
      if (!eventTimerStopApplied(event))
        continue;
      if (event->root.callback == 0 && !releaseControlReference) {
        // This synchronous applier may be an arbitrary Start/Stop thread.
        // Transfer its ownership to the loop before touching a coroutine
        // waiter; coroutineCall is only valid from an asyncLoop main context.
        eventIncrementReference(event, 1);
        event->base->methodImpl.enqueue(event->base, eventTimerControlNode(event));
        return;
      }
      eventCoroutineCancel(event);
    } else {
      uintptr_t desiredGeneration;
      uint64_t desiredPeriod;
      int desiredCounter;
      int stable = eventTimerSnapshot(event,
                                      &desiredGeneration,
                                      &desiredPeriod,
                                      &desiredCounter);
      if (stable && desiredGeneration != event->timerAppliedGeneration) {
        if (!eventTimerStopApplied(event))
          continue;
        event->timerAppliedGeneration = desiredGeneration;
        event->timerAppliedPeriod = desiredPeriod;
        event->timerRemaining = desiredCounter;
        if (desiredPeriod)
          eventTimerStartApplied(event);
      }

      if (stable && (event->timerState & etsArmed) &&
          desiredGeneration == event->timerAppliedGeneration) {
        uint128Pair empty = { 0, (uint64_t)event->timerAppliedGeneration };
        uint64_t confirmedTicks = 0;
        uint128Pair signals = __uint128_atomic_load_relaxed(&event->timerSignals);
        if (signals.low)
          signals = __uint128_atomic_exchange_relaxed(&event->timerSignals, empty);
        uint64_t signalCount = signals.low >> 1;
        int probes = (int)(signals.low & 1);
        if (signals.high == (uint64_t)event->timerAppliedGeneration && signalCount && probes) {
          // Each epoll envelope consumed an EPOLLONESHOT shot even when the
          // timerfd read says EAGAIN, so the current schedule must be rearmed.
          event->timerState |= etsNeedsRearm;
          for (uint64_t i = 0; i < signalCount; i++) {
            int consumed = event->base->methodImpl.updateEventTimer(
              event,
              etuConsume,
              event->timerAppliedGeneration,
              event->timerAppliedPeriod);
            if (consumed > 1)
              confirmedTicks++;
          }
        } else if (signals.high == (uint64_t)event->timerAppliedGeneration && !probes) {
          confirmedTicks = signalCount;
        }
        if (confirmedTicks) {
          event->timerState |= etsNeedsRearm;
          for (uint64_t i = 0; i < confirmedTicks; i++) {
            // The generation claim is the tick's linearization point against
            // Start/Stop. A later control publication is reconciled on the
            // next pass and cannot make this tick mutate its new counter.
            if (eventReferenceIsDeleting(event) ||
                __uintptr_atomic_load(&event->timerConfigSequence, amoAcquire) !=
                  event->timerAppliedGeneration)
              break;
            if (eventReferenceTryActivate(event)) {
#ifdef OS_WINDOWS
              if (!event->base->methodImpl.activate(event)) {
                eventReferenceDeactivate(event);
                eventReferenceDropNonFinal(event, 1);
                break;
              }
#else
              // Reactor enqueue is an infallible void publication. Keeping
              // IOCP's rollback branch out of this build removes a branch
              // after every POSIX timer activation.
              event->base->methodImpl.activate(event);
#endif
              if (event->timerRemaining > 0 && --event->timerRemaining == 0)
                break;
            }
          }

        }

        if (eventReferenceIsDeleting(event) ||
            __uintptr_atomic_load(&event->timerConfigSequence, amoAcquire) !=
              event->timerAppliedGeneration)
          continue;
        if (event->timerRemaining == 0 && desiredCounter > 0) {
          if (!eventTimerStopApplied(event))
            continue;
        } else if ((event->timerState & (etsArmed | etsNeedsRearm)) ==
                   (etsArmed | etsNeedsRearm)) {
          if (!event->base->methodImpl.updateEventTimer(event,
                                                        etuRearm,
                                                        event->timerAppliedGeneration,
                                                        event->timerAppliedPeriod))
            continue;
          event->timerState &= ~etsNeedsRearm;
        }
      }
    }

    // Retire only a clean owner. If a producer set DIRTY at any point after
    // the pass began, this CAS fails and the same owner drains another pass;
    // if it succeeds, a later producer observes no RUNNING bit and queues its
    // own referenced task. Release keeps all consumer work before hand-off.
    if (!__uint_atomic_compare_and_swap(&event->timerControlState,
                                              ecsRunning,
                                              0,
                                              amoRelease))
      continue;

    if (releaseControlReference)
      eventDecrementReference(event, 1);
    return; // the release may have recycled event
  }
}

void eventTimerProcess(aioUserEvent *event)
{
  eventTimerProcessImpl(event, 1);
}

aioUserEvent *newUserEvent(asyncBase *base, int isSemaphore, aioEventCb callback, void *arg)
{
  aioUserEvent *event = 0;
  asyncOpAlloc(base, sizeof(aioUserEvent), 0, &base->eventPool, 0, (asyncOpRoot**)&event);
  // releaseUserEvent returns a slot only after its timer grace/rendezvous and
  // every logical reference have completed, so the whole opaque object can be
  // initialized in one contiguous clear. This is both the complete reuse
  // contract and fewer stores than clearing root then every extension field.
  memset(event, 0, sizeof(*event));
  event->root.opCode = actUserEvent;
  event->root.finishMethod = eventFinish;
  event->root.callback = (void*)callback;
  event->root.arg = arg;
  event->root.objectPool = &base->eventPool;
  event->root.running = arWaiting;
  event->base = base;
  event->isSemaphore = isSemaphore;
  __uintptr_atomic_store(&event->tag, 1, amoRelaxed);
  return event;
}


aioObject *newSocketIo(asyncBase *base, socketTy hSocket)
{
#ifdef SO_NOSIGPIPE
  // Accepted sockets never pass through socketCreate, and Darwin/BSD have no
  // MSG_NOSIGNAL, so SIGPIPE suppression must be a property of the descriptor
  // itself, set at the point every socket enters the async machinery.
  int optval = 1;
  setsockopt(hSocket, SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof(optval));
#endif
  aioObject *object = base->methodImpl.newAioObject(base, ioObjectSocket, &hSocket);
  object->needSigpipeGuard = 0;
  return object;
}

aioObject *newDeviceIo(asyncBase *base, iodevTy hDevice)
{
  int needGuard = 0;
#ifndef OS_WINDOWS
  // write() has no MSG_NOSIGNAL equivalent, so descriptors that can raise
  // SIGPIPE need protection: pipes, plus sockets handed here by mistake.
  // Character devices (serial ports, ttys) cannot raise it and stay
  // guard-free.
  struct stat deviceStat;
  if (fstat(hDevice, &deviceStat) == 0 &&
      (S_ISFIFO(deviceStat.st_mode) || S_ISSOCK(deviceStat.st_mode)))
    needGuard = 1;
#ifdef F_SETNOSIGPIPE
  // Per-fd suppression (Darwin/NetBSD) makes per-write masking unnecessary.
  if (needGuard && fcntl(hDevice, F_SETNOSIGPIPE, 1) == 0)
    needGuard = 0;
#endif
#endif
  aioObject *object = base->methodImpl.newAioObject(base, ioObjectDevice, &hDevice);
  object->needSigpipeGuard = needGuard;
  return object;
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
  assert(usTimeout > 0 && "A user-event timer period must be non-zero");
  assert(usTimeout <= (uint64_t)INT64_MAX / 10 &&
         "A user-event timer period exceeds the supported range");
  eventTimerPublishConfig(event, usTimeout, counter, UINTPTR_MAX);
}


void userEventStopTimer(aioUserEvent *event)
{
  eventTimerPublishConfig(event, 0, 0, UINTPTR_MAX);
}

void userEventActivate(aioUserEvent *event)
{
  if (!eventReferenceTryActivate(event))
    return;
#ifdef OS_WINDOWS
  if (!event->base->methodImpl.activate(event)) {
    // The delivery was accepted atomically before backend publication. A
    // failed IOCP post must undo both pieces or the non-semaphore gate and its
    // delivery reference leak forever.
    eventReferenceDeactivate(event);
    eventReferenceDropNonFinal(event, 1);
  }
#else
  // kqueue/epoll enqueue cannot fail, and this final indirect call becomes a
  // tail call in the hot activation path.
  event->base->methodImpl.activate(event);
#endif
}

void deleteUserEvent(aioUserEvent *event)
{
  int firstDelete = eventReferenceMarkDeleting(event);
  assert(firstDelete && "deleteUserEvent may be called exactly once");
  if (!firstDelete)
    return;

  eventTimerSchedule(event, event->root.callback == 0);
  eventDecrementReference(event, 1);
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
  int result;
  if (object->root.type == ioObjectSocket) {
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
  if (!__uintptr_atomic_compare_and_swap(&object->root.initializationOp,
                                         0,
                                         (uintptr_t)&op->root,
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(&op->root, aosUnknownError);
    addToGlobalQueue(&op->root);
    return;
  }

  combinerPushOperation(&op->root);
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
  // Datagram fast path runs the syscall without entering the combiner, so
  // the sticky delete sweep cannot stop it - gate here: a call after
  // objectDelete must be rejected without touching the socket, otherwise an
  // incoming flood keeps this path succeeding and teardown never finishes
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed)) {
    if (callback == 0)
      return -(ssize_t)aosCanceled;
    struct Context context;
    fillContext(&context, object->root.base->methodImpl.readMsg, readMsgFinish, buffer, size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
    op->bytesTransferred = 0;
    memset(&op->host, 0, sizeof(op->host));
    opForceStatus(&op->root, aosCanceled);
    addToGlobalQueue(&op->root);
    return -(ssize_t)aosPending;
  }

  struct sockaddr_storage source;
  int truncated;
#ifdef OS_WINDOWS
  socketLenTy addrlen = sizeof(source);
  ssize_t result = recvfrom(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)&source, &addrlen);
  // Winsock consumes the datagram even when it does not fit into the buffer
  truncated = result == -1 && WSAGetLastError() == WSAEMSGSIZE;
#else
  struct iovec iov;
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  iov.iov_base = buffer;
  iov.iov_len = size;
  msg.msg_name = &source;
  msg.msg_namelen = sizeof(source);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  ssize_t result = recvmsg(object->hSocket, &msg, 0);
  truncated = result >= 0 && (msg.msg_flags & MSG_TRUNC);
#endif

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.readMsg, readMsgFinish, buffer, size);
  if (truncated) {
    // The datagram is already consumed and cut down to the buffer size:
    // parking the operation here would lose it with no completion at all
    HostAddress host;
    sockaddrToHostAddress(&source, &host);
    if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
      return -(ssize_t)aosBufferTooSmall;
    } else {
      if (flags & afActiveOnce)
        currentFinishedSync = 0;
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
      op->bytesTransferred = size;
      op->host = host;
      opForceStatus(&op->root, aosBufferTooSmall);
      addToGlobalQueue(&op->root);
    }
  } else if (result >= 0) {
    // Data received synchronously
    HostAddress host;
    sockaddrToHostAddress(&source, &host);
    if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
      return result;
    } else {
      if (flags & afActiveOnce)
        currentFinishedSync = 0;
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
      op->bytesTransferred = (size_t)result;
      op->host = host;
      opForceStatus(&op->root, aosSuccess);
      addToGlobalQueue(&op->root);
    }
  } else {
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
  // See aioReadMsg: reject a closing object before the syscall
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed)) {
    if (callback == 0)
      return -(ssize_t)aosCanceled;
    struct Context context;
    fillContext(&context, object->root.base->methodImpl.writeMsg, rwFinish, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actWriteMsg, &context);
    op->bytesTransferred = 0;
    opForceStatus(&op->root, aosCanceled);
    addToGlobalQueue(&op->root);
    return -(ssize_t)aosPending;
  }

  // Datagram socket can be accessed by multiple threads without lock
  struct sockaddr_storage remoteAddress;
  socketLenTy addrlen = hostAddressToSockaddr(address, &remoteAddress);
#ifdef OS_WINDOWS
  ssize_t result = sendto(object->hSocket, buffer, (int)size, 0, (struct sockaddr *)&remoteAddress, addrlen);
#else
  ssize_t result = sendto(object->hSocket, buffer, size, 0, (struct sockaddr *)&remoteAddress, addrlen);
#endif

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.writeMsg, rwFinish, (void*)((uintptr_t)buffer), size);
  if (result >= 0) {
    if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
      return result;
    } else {
      if (flags & afActiveOnce)
        currentFinishedSync = 0;
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actWriteMsg, &context);
      op->bytesTransferred = (size_t)result;
      opForceStatus(&op->root, aosSuccess);
      addToGlobalQueue(&op->root);
    }
  } else {
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actWriteMsg, &context);
    op->host = *address;
    combinerPushOperation(&op->root);
  }

  return -(ssize_t)aosPending;
}


int ioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout)
{
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.connect, connectFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, afCoroutine, usTimeout, 0, 0, actConnect, &context);
  op->host = *address;
  if (!__uintptr_atomic_compare_and_swap(&object->root.initializationOp,
                                         0,
                                         (uintptr_t)&op->root,
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(&op->root, aosUnknownError);
    addToGlobalQueue(&op->root);
  } else {
    combinerPushOperation(&op->root);
  }
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -status;
}


int ioAccept(aioObject *object,
             socketTy *acceptedSocket,
             HostAddress *remoteAddress,
             uint64_t usTimeout)
{
  *acceptedSocket = INVALID_SOCKET;
  if (remoteAddress)
    memset(remoteAddress, 0, sizeof(*remoteAddress));

#ifdef OS_WINDOWS
  AsyncFlags flags = afNone;
#else
  AsyncFlags flags = afRunning;
#endif
  struct Context context;
  fillContext(&context, object->root.base->methodImpl.accept, acceptFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actAccept, &context);
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
  // See aioReadMsg: reject a closing object before the syscall
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed))
    return -(ssize_t)aosCanceled;

  // Datagram socket can be accessed by multiple threads without lock
  struct sockaddr_storage source;
  int truncated;
#ifdef OS_WINDOWS
  socketLenTy addrlen = sizeof(source);
  ssize_t result = recvfrom(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)&source, &addrlen);
  // Winsock consumes the datagram even when it does not fit into the buffer
  truncated = result == -1 && WSAGetLastError() == WSAEMSGSIZE;
#else
  struct iovec iov;
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  iov.iov_base = buffer;
  iov.iov_len = size;
  msg.msg_name = &source;
  msg.msg_namelen = sizeof(source);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  ssize_t result = recvmsg(object->hSocket, &msg, 0);
  truncated = result >= 0 && (msg.msg_flags & MSG_TRUNC);
#endif

  struct Context context;
  fillContext(&context, object->root.base->methodImpl.readMsg, 0, buffer, size);
  if (truncated) {
    // The datagram is already consumed and cut down to the buffer size:
    // parking the operation here would lose it with no completion at all
    if (++currentFinishedSync >= MAX_SYNCHRONOUS_FINISHED_OPERATION) {
      asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actReadMsg, &context);
      op->bytesTransferred = size;
      opForceStatus(&op->root, aosBufferTooSmall);
      addToGlobalQueue(&op->root);
      coroutineYield();
      return coroutineRwFinish(op, object);
    } else {
      return -(ssize_t)aosBufferTooSmall;
    }
  }
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
  combinerPushOperation(&op->root);
  coroutineYield();
  return coroutineRwFinish(op, object);
}

ssize_t ioWriteMsg(aioObject *object, const HostAddress *address, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout)
{
  // See aioReadMsg: reject a closing object before the syscall
  if (__uint_atomic_load(&object->root.DeletePending, amoRelaxed))
    return -(ssize_t)aosCanceled;

  // Datagram socket can be accessed by multiple threads without lock
  struct sockaddr_storage remoteAddress;
  socketLenTy addrlen = hostAddressToSockaddr(address, &remoteAddress);
#ifdef OS_WINDOWS
  ssize_t result = sendto(object->hSocket, buffer, (int)size, 0, (struct sockaddr *)&remoteAddress, addrlen);
#else
  ssize_t result = sendto(object->hSocket, buffer, size, 0, (struct sockaddr *)&remoteAddress, addrlen);
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
  combinerPushOperation(&op->root);
  coroutineYield();
  return coroutineRwFinish(op, object);
}


void ioSleep(aioUserEvent *event, uint64_t usTimeout)
{
  if (eventTryConsumeCredit(event))
    return;

  assert(usTimeout > 0 && "ioSleep timeout must be non-zero");
  assert(usTimeout <= (uint64_t)INT64_MAX / 10 &&
         "ioSleep timeout exceeds the supported range");
  uintptr_t generation = eventTimerPublishConfig(event, usTimeout, 1, UINTPTR_MAX);
  if (!generation)
    return;
  if (!eventInstallWaiter(event, generation)) {
    eventTimerCancelGeneration(event, generation);
    return;
  }
  coroutineYield();
  eventTimerCancelGeneration(event, generation);
  eventDecrementReference(event, 1); // waiter reference
}

void ioWaitUserEvent(aioUserEvent *event)
{
  if (eventTryConsumeCredit(event))
    return;
  if (!eventInstallWaiter(event, 0))
    return;
  coroutineYield();
  eventDecrementReference(event, 1); // waiter reference
}
