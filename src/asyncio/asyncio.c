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

static void eventDeliverCallbacks(aioUserEvent *event, uintptr_t count);
static void eventCoroutineResume(aioUserEvent *event, uintptr_t count, int manual, uint32_t generation);
static void eventProcessCancellation(aioUserEvent *event);
static uint32_t eventTimerPublishConfig(aioUserEvent *event, uint64_t period, int counter, uint32_t requiredGeneration, int terminal);

#ifdef OS_WINDOWS
asyncBase *iocpNewAsyncBase();
#endif
#ifdef OS_LINUX
asyncBase *epollNewAsyncBase(void);
#endif
#if defined(OS_DARWIN) || defined(OS_FREEBSD)
asyncBase *kqueueNewAsyncBase(void);
#endif

// Reactor operations park as already running: kernel readiness drives them
// from submission on. The IOCP proactor must reach executeMethod to post the
// overlapped I/O first, so its operations park as waiting until then.
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

void eventExecuteQueuedTask(asyncOpRoot *node)
{
  aioUserEvent *event = eventQueueTaskEvent(node);
  currentFinishedSync = 0;
  eventProcessCancellation(event);
  eventDecrementReference(event, 1);
}

static void releaseOp(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  // Pool-sized scratch stays with the pooled operation for the next parked
  // write or accept; only oversized captures (large payloads under
  // backpressure) go back to the allocator, so a pooled operation retains
  // at most the default buffer size
  if (op->internalBufferSize > DEFAULT_SOCKET_BUFFER_SIZE) {
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
  asyncBase *base = object->header.base;
  asyncOp *op = (asyncOp*)base->methodImpl.newAsyncOp(base, flags & afRealtime, &opPool, &opTimerPool);
  initAsyncOpRoot(&op->root,
                  context->StartProc,
                  base->methodImpl.cancelAsyncOp,
                  context->FinishProc,
                  releaseOp,
                  object,
                  callback,
                  arg,
                  flags,
                  opCode,
                  usTimeout);

  op->state = 0;
  op->transactionSize = context->TransactionSize;
  op->bytesTransferred = 0;
  if (opCode == actAccept) {
    op->acceptSocket = INVALID_SOCKET;
    memset(&op->host, 0, sizeof(op->host));
  }
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
  unsigned loopThreadLimit = loopThreads ? loopThreads : 1;
  const size_t wordBits = sizeof(uintptr_t) * 8;
  size_t loopThreadSlotWords = ((size_t)loopThreadLimit + wordBits - 1) / wordBits;
  TimerSleepSlot *timerSleep = (TimerSleepSlot*)alignedMalloc(sizeof(TimerSleepSlot) * (size_t)loopThreadLimit, CACHE_LINE_SIZE);
  uintptr_t *loopThreadSlots = (uintptr_t*)calloc(loopThreadSlotWords, sizeof(uintptr_t));
  if (!timerSleep || !loopThreadSlots) {
    alignedFree(timerSleep);
    free(loopThreadSlots);
    return 0;
  }

  // Each supported OS has exactly one backend, so the method parameter cannot
  // select anything different from the OS default; it stays in the signature
  // for a platform with alternative backends.
  __UNUSED(method);
#if defined(OS_WINDOWS)
  asyncBase *base = iocpNewAsyncBase();
#elif defined(OS_LINUX)
  asyncBase *base = epollNewAsyncBase();
#elif defined(OS_DARWIN) || defined(OS_FREEBSD)
  asyncBase *base = kqueueNewAsyncBase();
#else
#error "Unsupported OS: no I/O multiplexor backend available"
#endif

  if (!base) {
    alignedFree(timerSleep);
    free(loopThreadSlots);
    return 0;
  }

#ifndef NDEBUG
  base->opsCount = 0;
#endif
  timerWheelInit(base, getMonotonicTicks());
  memset(&base->globalQueue, 0, sizeof(base->globalQueue));
  memset(&base->eventPool, 0, sizeof(base->eventPool));
  base->messageLoopThreadCounter = 0;
  base->quitRequested = 0;
  base->timerSleep = timerSleep;
  base->loopThreadSlots = loopThreadSlots;
  base->loopThreadSlotWords = (unsigned)loopThreadSlotWords;
  base->loopThreadLimit = loopThreadLimit;
  base->timerPreclearOverflow = 0;
  for (unsigned i = 0; i < loopThreadLimit; i++) {
    base->timerSleep[i].wakeTick = UINTPTR_MAX;
    base->timerSleep[i].preclearSequence = 0;
  }
  return base;
}

void asyncLoop(asyncBase *base)
{
  base->methodImpl.nextFinishedOperation(base);
}

void postQuitOperation(asyncBase *base)
{
  // Level, not edge: the sticky word stops the threads already looping, the
  // ones asleep in the kernel wait and the ones that have not entered
  // asyncLoop yet (see the litmus at asyncBase.quitRequested). One doorbell
  // is enough - each exiting thread re-rings while others remain. The
  // RMW-plus-write pair keeps this callable from a signal handler: no queue
  // traffic, no allocation, no lock
  __uintptr_atomic_fetch_or(&base->quitRequested, 1, amoSeqCst);
  base->methodImpl.wakeupLoop(base);
}

void resetQuitOperation(asyncBase *base)
{
  // Quiescent-only by contract: every asyncLoop returned and no concurrent
  // postQuitOperation/asyncLoop/reset. The store stays atomic, so a racing
  // quit is not UB - it lands on the old round (erased here) or on the next
  // one, unspecified which. Everything else the loops touched is already in
  // its entry state after a clean exit (slots and counter zero, horizons
  // parked at UINTPTR_MAX, leftover queue items and armed timers are next
  // round's legitimate work; a residual doorbell costs one spurious wakeup)
  assert(__uint_atomic_load(&base->messageLoopThreadCounter, amoSeqCst) == 0 &&
         "resetQuitOperation while asyncLoop threads are still registered");
#ifndef NDEBUG
  for (unsigned i = 0; i < base->loopThreadSlotWords; i++)
    assert(__uintptr_atomic_load(&base->loopThreadSlots[i], amoSeqCst) == 0 &&
           "resetQuitOperation while a loop slot is still owned");
#endif
  __uintptr_atomic_store(&base->quitRequested, 0, amoRelaxed);
}

void setSocketBuffer(aioObject *socket, size_t bufferSize)
{
  ioBufferEnsureCapacity(&socket->buffer, bufferSize);
}

static uint32_t eventTimerNextGeneration(uint32_t generation)
{
  generation++;
  return generation ? generation : 1;
}

static uint32_t eventTimerControlTickDelta(uint128 applied, uint128 current)
{
  return (uint32_t)(eventTimerControlCounter(current) - eventTimerControlCounter(applied));
}

static void eventTimerStop(aioUserEvent *event, aioTimer *timer)
{
  if (!timer)
    return;
  aioTimerUserEventState *state = eventTimerState(timer);
  if (!state->armed)
    return;

  (void)event->header.base->methodImpl.updateEventTimer(event, etuStop, 0, 0);
  state->armed = 0;
}

static aioTimer *eventTimerApplyConfig(aioUserEvent *event, aioTimer *timer, uint128 *applied, uint128 current)
{
  eventTimerStop(event, timer);

  *applied = current;
  uint64_t period = eventTimerControlPeriod(current);
  if (period && !(current.low & EVENT_TIMER_TERMINAL)) {
    uint32_t remaining = eventTimerControlIsFinite(current) ? eventTimerControlCounter(current) : 0;
    int armed = event->header.base->methodImpl.updateEventTimer(event, etuStart, eventTimerControlGeneration(current), period);
    timer = eventTimerLoad(event, amoRelaxed);
    if (timer) {
      aioTimerUserEventState *state = eventTimerState(timer);
      state->remaining = remaining;
      state->armed = (uint32_t)armed;
    }
  }
  return timer;
}

// One attempt of the owner's release CAS: drop the OWNER bit from the control
// word exactly as observed. Failure means a concurrent publication landed;
// *current is refreshed and the owner must reconcile it before retrying.
static inline int eventTimerTryRelease(aioUserEvent *event, uint128 *current)
{
  uint128 unlocked = *current;
  unlocked.low &= ~EVENT_TIMER_OWNER;
  uint128 expected = *current;
  if (__uint128_atomic_compare_and_swap(&event->timerControl, &expected, unlocked))
    return 1;
  *current = expected;
  return 0;
}

static void eventTimerProcessUserOwner(aioUserEvent *event, uint128 applied, uint128 current)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  aioTimerUserEventState *state = timer ? eventTimerState(timer) : 0;

  for (;;) {
    if (eventTimerControlGeneration(current) != eventTimerControlGeneration(applied)) {
      timer = eventTimerApplyConfig(event, timer, &applied, current);
      state = timer ? eventTimerState(timer) : 0;
    }

    uint32_t published = eventTimerControlTickDelta(applied, current);
    if (published) {
      // A user owner discards the raced input without spending the private
      // counter. Zero tells an IOCP backend to rearm even a one-shot last tick.
      applied.high = current.high;
      if (state && state->armed)
        (void)event->header.base->methodImpl.consumeEventTimerTick(event,
                                                                  0,
                                                                  eventTimerControlGeneration(applied),
                                                                  eventTimerControlPeriod(applied));
      continue;
    }

    if (eventTimerTryRelease(event, &current))
      return;
  }
}

static uintptr_t eventTimerProcessKernelOwner(aioUserEvent *event, uint128 applied, uint128 current, uint32_t *deliveryGeneration)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  aioTimerUserEventState *state = timer ? eventTimerState(timer) : 0;
  uintptr_t deliveryCount = 0;

  for (;;) {
    if (eventTimerControlGeneration(current) != eventTimerControlGeneration(applied)) {
      timer = eventTimerApplyConfig(event, timer, &applied, current);
      state = timer ? eventTimerState(timer) : 0;
    }

    uint32_t published = eventTimerControlTickDelta(applied, current);
    if (published) {
      uint32_t generation = eventTimerControlGeneration(applied);
      applied.high = current.high;
      if (!state || !state->armed)
        continue;

      uint64_t count = event->header.base->methodImpl.consumeEventTimerTick(event,
                                                                            published,
                                                                            generation,
                                                                            eventTimerControlPeriod(applied));
      if (eventTimerControlIsFinite(applied)) {
        if (count > state->remaining)
          count = state->remaining;
        state->remaining -= (uint32_t)count;
        if (!state->remaining)
          eventTimerStop(event, timer);
      }
      if (count) {
        if (!event->callback && *deliveryGeneration != generation)
          deliveryCount = 0;
        *deliveryGeneration = generation;
        deliveryCount += count;
      }
      continue;
    }

    if (eventTimerTryRelease(event, &current))
      return deliveryCount;
  }
}

static uint32_t eventTimerPublishConfig(aioUserEvent *event, uint64_t period, int counter, uint32_t requiredGeneration, int terminal)
{
  for (;;) {
    uint128 old = __uint128_atomic_load_relaxed(&event->timerControl);
    if ((old.low & EVENT_TIMER_TERMINAL) && !terminal)
      return 0;

    uint32_t generation = eventTimerControlGeneration(old);
    if (requiredGeneration && generation != requiredGeneration)
      return 0;

    uint32_t nextGeneration = eventTimerNextGeneration(generation);
    uint32_t finiteCounter = counter > 0 ? (uint32_t)counter : 0;
    uint128 desired = {period | EVENT_TIMER_OWNER | (terminal ? EVENT_TIMER_TERMINAL : 0) | (counter > 0 ? EVENT_TIMER_FINITE : 0),
                       nextGeneration | ((uint64_t)finiteCounter << EVENT_TIMER_COUNTER_SHIFT)};
    uint128 expected = old;
    if (!__uint128_atomic_compare_and_swap(&event->timerControl, &expected, desired))
      continue;

    if (!(old.low & EVENT_TIMER_OWNER))
      eventTimerProcessUserOwner(event, old, desired);
    return nextGeneration;
  }
}

static void eventTimerCancelGeneration(aioUserEvent *event, uint32_t generation)
{
  if (generation)
    eventTimerPublishConfig(event, 0, 0, generation, 0);
}

// Claims a kernel reference for a decoded full-generation handle. The
// successful DWCAS pins the complete Head before any tail access.
int eventTimerTryClaimReference(aioUserEvent *event, uint64_t eventGeneration)
{
  uint128 expected = {__uint64_atomic_load(&event->header.tag.low, amoRelaxed), eventGeneration};
  for (;;) {
    // The handle must still name a live, non-deleting event incarnation.
    if (expected.high != eventGeneration || !(expected.low & TAG_EVENT_REF_MASK) || (expected.low & TAG_EVENT_DELETE))
      return 0;
    uint128 desired = {expected.low + 1, expected.high};
    if (__uint128_atomic_compare_and_swap(&event->header.tag, &expected, desired))
      return 1;
  }
}

void eventTimerSignal(aioUserEvent *event, uint32_t timerGeneration, uint64_t eventGeneration, uint64_t tickCount)
{
  // A stale harvested timer may outlive logical release and may inspect only
  // Head while the rest of the pooled event is ASan-poisoned. The
  // cheap event-generation prefilter and the DWCAS claim must therefore
  // precede any access to the timer control word or event tail.
  if (timerGeneration == 0 || eventHandleGeneration(event) != eventGeneration)
    return;
  if (!eventTimerTryClaimReference(event, eventGeneration))
    return;

  for (;;) {
    uint128 old = __uint128_atomic_load_relaxed(&event->timerControl);
    if (eventTimerControlGeneration(old) != timerGeneration || !eventTimerControlPeriod(old) || (old.low & EVENT_TIMER_TERMINAL)) {
      eventDecrementReference(event, 1);
      return;
    }

    uint128 desired = old;
    desired.low |= EVENT_TIMER_OWNER;
    desired.high += (uint64_t)(uint32_t)tickCount << EVENT_TIMER_COUNTER_SHIFT;

    uint128 expected = old;
    if (!__uint128_atomic_compare_and_swap(&event->timerControl, &expected, desired)) {
      if (eventTimerControlGeneration(expected) != timerGeneration) {
        eventDecrementReference(event, 1);
        return;
      }
      continue;
    }

    if (!(old.low & EVENT_TIMER_OWNER)) {
      uint32_t deliveryGeneration = 0;
      uintptr_t deliveryCount = eventTimerProcessKernelOwner(event, old, desired, &deliveryGeneration);
      if (deliveryCount) {
        if (event->callback)
          eventDeliverCallbacks(event, deliveryCount);
        else
          eventCoroutineResume(event, deliveryCount, 0, deliveryGeneration);
      }
    }
    eventDecrementReference(event, 1);
    return;
  }
}

static void eventCoroutineResume(aioUserEvent *event, uintptr_t count, int manual, uint32_t generation)
{
  uintptr_t expected = __uintptr_atomic_load(&event->waiter, amoRelaxed);
  for (;;) {
    int kind = (int)(expected & ewtMask);
    if (kind == ewtDeleted)
      return;
    // A manual wake publishes cancellation before competing for an ioSleep
    // waiter. A timer validates after loading waiter, so a failed CAS observes
    // the winner before it can turn an old tick into a later credit.
    if (manual && kind == ewtSleep)
      eventTimerPublishConfig(event, 0, 0, 0, 0);
    else if (generation && eventTimerGeneration(event) != generation)
      return;
    uintptr_t desired = kind ? (count - 1) * ewtCreditUnit : expected + count * ewtCreditUnit;
    if (!__uintptr_atomic_compare_exchange(&event->waiter, &expected, desired, amoAcquire))
      continue;
    if (!kind)
      return;
    coroutineTy *coroutine = (coroutineTy*)(expected & ~(uintptr_t)ewtMask);
    assert(coroutineIsMain() && "User-event coroutine delivery must run in the main coroutine");
    coroutineCall(coroutine);
    return;
  }
}

static void eventCoroutineCancel(aioUserEvent *event)
{
  uintptr_t old = __uintptr_atomic_load(&event->waiter, amoRelaxed);
  for (;;) {
    unsigned kind = (unsigned)(old & ewtMask);
    if (kind == ewtDeleted)
      return;
    if (__uintptr_atomic_compare_exchange(&event->waiter, &old, ewtDeleted, amoAcquire)) {
      if (kind == ewtUser || kind == ewtSleep) {
        assert(coroutineIsMain() && "User-event coroutine cancellation must run in the main coroutine");
        coroutineCall((coroutineTy*)(old & ~(uintptr_t)ewtMask));
      }
      return;
    }
  }
}

static int eventInstallWaiter(aioUserEvent *event, unsigned waiterKind)
{
  uintptr_t expected = __uintptr_atomic_load(&event->waiter, amoRelaxed);
  if ((expected & ewtMask) == ewtDeleted)
    return 0;
  assert((expected & ewtMask) == ewtCredits && "Only one coroutine may wait on a user event");
  if (expected == 0) {
    uintptr_t coroutine = (uintptr_t)coroutineCurrent();
    assert(coroutine && !(coroutine & ewtMask) && "Coroutine pointer is not sufficiently aligned");
    uintptr_t desired = coroutine | waiterKind;
    if (__uintptr_atomic_compare_exchange(&event->waiter, &expected, desired, amoRelease)) {
      // The second same-word RMW against Delete schedules cancellation.
      uint64_t oldTag = __uint64_atomic_fetch_and_add(&event->header.tag.low, TAG_EVENT_WAITER_COMMITTED, amoRelease);
      if (oldTag & TAG_EVENT_DELETE) {
        eventIncrementReference(event, 1);
        event->header.base->methodImpl.enqueue(event->header.base, eventCancellationNode(event));
      }
      return 1;
    }
    if ((expected & ewtMask) == ewtDeleted)
      return 0;
  }

  (void)__uintptr_atomic_fetch_and_add(&event->waiter, (uintptr_t)0 - ewtCreditUnit, amoAcquire);
  return 0;
}

static void eventFinishWaiter(aioUserEvent *event)
{
  // Clear the rendezvous after Yield; this bit is not a reference.
  (void)__uint64_atomic_fetch_and_add(&event->header.tag.low, 0ULL - TAG_EVENT_WAITER_COMMITTED, amoRelaxed);
}

static void eventProcessCancellation(aioUserEvent *event)
{
  // The waiter sentinel stays terminal, so a late kernel owner cannot turn a
  // timer signal into a credit.
  __uintptr_atomic_store(&event->signalState, 0, amoRelaxed);
  eventCoroutineCancel(event);
}

static void eventDeliverCallbacks(aioUserEvent *event, uintptr_t count)
{
  // Admission owns a reference. Delete cannot retract this accepted batch.
  aioEventCb *callback = (aioEventCb*)event->callback;
  while (count--)
    callback(event, event->arg);
}

void eventManualReady(aioUserEvent *event)
{
  uintptr_t count = __uintptr_atomic_exchange(&event->signalState, 0, amoAcquire);
  if (!count)
    return;

  if (event->callback)
    eventDeliverCallbacks(event, count);
  else
    eventCoroutineResume(event, count, 1, 0);
}

aioUserEvent *newUserEvent(asyncBase *base, int isSemaphore, aioEventCb callback, void *arg)
{
  aioUserEvent *event = (aioUserEvent*)objectAlloc(&base->eventPool, sizeof(aioUserEvent), TAGGED_POINTER_ALIGNMENT);
  if (!event)
    return 0;

  // A stale kernel envelope may inspect only Head while this slot
  // is pooled or being reused. Keep the optional backend timer paired with the
  // slot, preserve its monotonic event generation, reset the tail, and publish
  // the live refcount only after initialization is complete.
  uintptr_t generation = eventHandleGeneration(event);
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  objectHeaderSetType(&event->header, ohtUserEvent);
  event->header.base = base;
  event->callback = (void*)callback;
  event->arg = arg;
  event->header.isSemaphore = isSemaphore;
  if (timer) {
    aioTimerUserEventState *state = eventTimerState(timer);
    state->remaining = 0;
    state->armed = 0;
  }
  __uint64_atomic_store(&event->timerControl.low, 0, amoRelaxed);
  __uint64_atomic_store(&event->timerControl.high, 0, amoRelaxed);
  __uintptr_atomic_store(&event->waiter, 0, amoRelaxed);
  __uintptr_atomic_store(&event->signalState, 0, amoRelaxed);
  event->destructorCb = 0;
  event->destructorCbArg = 0;
  event->activationId = UINT64_MAX;

  // Publish every field above and the incarnation bump performed by the
  // preceding final release. Kernel claims acquire this word before accepting
  // an incarnation, so it must be the last initialization store.
  __uint64_atomic_store(&event->header.tag.low, 1, amoRelease);
  if (!base->methodImpl.initializeUserEvent(event)) {
    __uint64_atomic_store(&event->header.tag.high, generation + 1, amoRelaxed);
    __uint64_atomic_store(&event->header.tag.low, TAG_EVENT_DELETE, amoRelaxed);
    base->methodImpl.releaseUserEvent(event);
    return 0;
  }
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
  if (!object)
    return 0;
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

void userEventStartTimer(aioUserEvent *event, uint64_t usTimeout, int counter)
{
  assert(usTimeout > 0 && "A user-event timer period must be non-zero");
  assert(usTimeout <= (uint64_t)INT64_MAX / 10 && "A user-event timer period exceeds the supported range");
  if (eventReferenceIsDeleting(event))
    return;
  eventTimerPublishConfig(event, usTimeout, counter, 0, 0);
}

void userEventStopTimer(aioUserEvent *event)
{
  if (eventReferenceIsDeleting(event))
    return;
  eventTimerPublishConfig(event, 0, 0, 0, 0);
}

void userEventActivate(aioUserEvent *event)
{
  if (eventReferenceIsDeleting(event))
    return;

  uintptr_t old;
  if (event->header.isSemaphore) {
    old = __uintptr_atomic_fetch_and_add(&event->signalState, 1, amoRelease);
  } else {
    old = __uintptr_atomic_exchange(&event->signalState, 1, amoRelease);
  }

  // The personal descriptor/completion is only a manual doorbell. Once one is
  // pending, signalState carries the exact count or the coalescing gate.
  if (old == 0)
    event->header.base->methodImpl.activate(event);
}

void deleteUserEvent(aioUserEvent *event)
{
  // Arbitrate with waiter commit; acquire imports its published pointer.
  uint64_t oldTag = __uint64_atomic_fetch_and_add(&event->header.tag.low, TAG_EVENT_DELETE, amoAcquire);

  eventTimerPublishConfig(event, 0, 0, 0, 1);
  if (event->callback == 0 && (oldTag & TAG_EVENT_WAITER_COMMITTED)) {
    eventIncrementReference(event, 1);
    event->header.base->methodImpl.enqueue(event->header.base, eventCancellationNode(event));
  }
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

  if (copyFromBuffer(buffer, bytesTransferred, sb, size))
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
      } else {
        asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afSyncStarted, usTimeout, (void*)callback, arg, actRead, &context);
        op->bytesTransferred = *bytesTransferred;
        return &op->root;
      }
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

// Transport initialization is one-shot for an object: the connect operation
// claims the initialization slot at submission. A losing claim is a second
// connect on the same object and completes with aosUnknownError through the
// global queue instead of entering the combiner.
static void connectSubmit(asyncOp *op)
{
  aioObjectRoot *object = op->root.object;
  if (!__uintptr_atomic_compare_and_swap(&object->initializationOp, 0, (uintptr_t)&op->root, amoSeqCst)) {
    opForceStatus(&op->root, aosUnknownError);
    addToGlobalQueue(&op->root);
    return;
  }

  combinerPushOperation(&op->root);
}

void aioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout, aioConnectCb callback, void *arg)
{
  struct Context context;
  fillContext(&context, object->root.header.base->methodImpl.connect, connectFinish, 0, 0);
  asyncOp *op = (asyncOp*)newAsyncOp(&object->root, afNone, usTimeout, (void*)callback, arg, actConnect, &context);
  op->host = *address;
  connectSubmit(op);
}

void aioAccept(aioObject *object, uint64_t usTimeout, aioAcceptCb callback, void *arg)
{
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

// One receive syscall and truncation oracle for both datagram read paths:
// Winsock consumes an oversized datagram and reports WSAEMSGSIZE, POSIX
// returns the clipped payload flagged MSG_TRUNC - either way the datagram is
// gone and must complete as aosBufferTooSmall, never be retried.
static ssize_t readMsgSyscall(aioObject *object, void *buffer, size_t size, struct sockaddr_storage *source, int *truncated)
{
#ifdef OS_WINDOWS
  socketLenTy addrlen = sizeof(*source);
  ssize_t result = recvfrom(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)source, &addrlen);
  *truncated = result == -1 && WSAGetLastError() == WSAEMSGSIZE;
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
  *truncated = result >= 0 && (msg.msg_flags & MSG_TRUNC);
#endif
  return result;
}

static ssize_t writeMsgSyscall(aioObject *object, const HostAddress *address, const void *buffer, size_t size)
{
  struct sockaddr_storage remoteAddress;
  socketLenTy addrlen = hostAddressToSockaddr(address, &remoteAddress);
#ifdef OS_WINDOWS
  return sendto(object->hSocket, buffer, (int)size, 0, (struct sockaddr*)&remoteAddress, addrlen);
#else
  return sendto(object->hSocket, buffer, size, 0, (struct sockaddr*)&remoteAddress, addrlen);
#endif
}

// Datagram fast paths run their syscall without entering the combiner, so the
// sticky delete sweep cannot stop them; the callers gate on DeletePending
// before touching the socket - after objectDelete an incoming flood would
// otherwise keep the path succeeding and teardown would never finish.
// Fire-and-forget learns the rejection inline; a callback completes with
// aosCanceled through the global queue.
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
  // The rejected operation only delivers aosCanceled; nothing ever reads a
  // write payload from it, so skip the capture copy. This also keeps the op
  // pools clean: this op bypasses opRelease, so a captured buffer would ride
  // into the pool around the releaseMethod size cap
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
  int truncated;
  ssize_t result = readMsgSyscall(object, buffer, size, &source, &truncated);

  if (truncated || result >= 0) {
    // Either data arrived synchronously, or the datagram was consumed but cut
    // down to the buffer size - a truncated datagram cannot be retried and
    // must complete as aosBufferTooSmall, or it would be lost with no
    // completion at all
    if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION))
      return truncated ? -(ssize_t)aosBufferTooSmall : result;

    if (flags & afActiveOnce)
      currentFinishedSync = 0;
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.readMsg, readMsgFinish, buffer, size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags, usTimeout, (void*)callback, arg, actReadMsg, &context);
    op->bytesTransferred = truncated ? size : (size_t)result;
    sockaddrToHostAddress(&source, &op->host);
    opForceStatus(&op->root, truncated ? aosBufferTooSmall : aosSuccess);
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
  ssize_t result = writeMsgSyscall(object, address, buffer, size);

  if (result >= 0) {
    if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION))
      return result;

    if (flags & afActiveOnce)
      currentFinishedSync = 0;
    // The datagram already left through the syscall; the op only carries
    // the completion, so the payload capture copy is skipped (afNoCopy)
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.writeMsg, rwFinish, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afNoCopy, usTimeout, (void*)callback, arg, actWriteMsg, &context);
    op->bytesTransferred = (size_t)result;
    opForceStatus(&op->root, aosSuccess);
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
  connectSubmit(op);
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
  int truncated;
  ssize_t result = readMsgSyscall(object, buffer, size, &source, &truncated);

  if (truncated || result >= 0) {
    // Either data arrived synchronously, or the datagram was consumed but cut
    // down to the buffer size - a truncated datagram cannot be retried and
    // must complete as aosBufferTooSmall, or it would be lost with no
    // completion at all
    if (++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION)
      return truncated ? -(ssize_t)aosBufferTooSmall : result;

    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.readMsg, 0, buffer, size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine, usTimeout, 0, 0, actReadMsg, &context);
    op->bytesTransferred = truncated ? size : (size_t)result;
    opForceStatus(&op->root, truncated ? aosBufferTooSmall : aosSuccess);
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
  ssize_t result = writeMsgSyscall(object, address, buffer, size);

  if (result != -1) {
    // Data received synchronously
    if (++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION)
      return result;

    // The datagram already left through the syscall; the op only carries
    // the completion, so the payload capture copy is skipped (afNoCopy)
    struct Context context;
    fillContext(&context, object->root.header.base->methodImpl.writeMsg, 0, (void*)((uintptr_t)buffer), size);
    asyncOp *op = (asyncOp*)newAsyncOp(&object->root, flags | afCoroutine | afNoCopy, usTimeout, 0, 0, actWriteMsg, &context);
    op->host = *address;
    op->bytesTransferred = (size_t)result;
    opForceStatus(&op->root, aosSuccess);
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

void ioSleep(aioUserEvent *event, uint64_t usTimeout)
{
  if (eventReferenceIsDeleting(event))
    return;

  uintptr_t expected = __uintptr_atomic_load(&event->waiter, amoRelaxed);
  assert((expected & ewtMask) == ewtCredits && "Only one coroutine may wait on a user event");
  if (expected) {
    (void)__uintptr_atomic_fetch_and_add(&event->waiter, (uintptr_t)0 - ewtCreditUnit, amoAcquire);
    return;
  }

  assert(usTimeout > 0 && "ioSleep timeout must be non-zero");
  assert(usTimeout <= (uint64_t)INT64_MAX / 10 && "ioSleep timeout exceeds the supported range");
  uint32_t generation = eventTimerPublishConfig(event, usTimeout, 1, 0, 0);
  if (!generation)
    return;
  if (!eventInstallWaiter(event, ewtSleep)) {
    eventTimerCancelGeneration(event, generation);
    return;
  }
  coroutineYield();
  eventFinishWaiter(event);
  eventTimerCancelGeneration(event, generation);
}

void ioWaitUserEvent(aioUserEvent *event)
{
  if (eventReferenceIsDeleting(event))
    return;
  if (!eventInstallWaiter(event, ewtUser))
    return;
  coroutineYield();
  eventFinishWaiter(event);
}
