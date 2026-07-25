#include "events.h"
#include "asyncioImpl.h"
#include "asyncio/coroutine.h"
#include "atomic.h"
#include <assert.h>
#include <stdint.h>

static void eventDeliverCallbacks(aioUserEvent *event, uintptr_t count);
static void eventCoroutineResume(aioUserEvent *event, uintptr_t count, int manual, uint32_t generation);
static uint32_t eventTimerPublishConfig(aioUserEvent *event, uint64_t period, int counter, uint32_t requiredGeneration, int terminal);

void eventIncrementReference(aioUserEvent *event, uintptr_t count)
{
  (void)__uint64_atomic_fetch_and_add(&event->header.tag.low, count, amoRelaxed);
}

void eventDecrementReference(aioUserEvent *event, uintptr_t count)
{
  uintptr_t old = __uint64_atomic_fetch_and_add(&event->header.tag.low, 0ULL - count, amoRelease);
  uintptr_t refs = old & TAG_EVENT_REF_MASK;
#ifndef NDEBUG
  assert(refs >= count && "Double event release detected");
  assert((refs != count || (old & TAG_EVENT_DELETE)) && "Final event reference released without deleteUserEvent");
#endif
  // The last release finalizes; the contract requires a preceding Delete.
  if (refs == count) {
    __atomic_fence(amoAcquire);
    if (event->destructorCb)
      event->destructorCb(event, event->destructorCbArg);

    // Atomic generation increment and full event release.
    uintptr_t generation = __uint64_atomic_load(&event->header.tag.high, amoRelaxed);
    __uint64_atomic_store(&event->header.tag.high, generation + 1, amoRelaxed);
    event->header.base->methodImpl.releaseUserEvent(event);
  }
}

void eventSetDestructorCb(aioUserEvent *event, userEventDestructorCb callback, void *arg)
{
  event->destructorCb = callback;
  event->destructorCbArg = arg;
}

static uint32_t eventTimerNextGeneration(uint32_t generation)
{
  generation++;
  return generation ? generation : 1;
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

// One attempt at applying the CONFIG in *current: extract its parameters, flip the word to ACTIVE with zero ticks, then arm the backend. Arming
// strictly after the flip is the ordering that lets every observer decide the word's variant by a generation compare alone (a tick of
// generation G can only be produced by a timer armed for G, hence after ACTIVE(G)). Returns nonzero on success; on failure a newer config
// landed, *current is refreshed and the caller retries against it (this config dies unapplied).
static int eventTimerApplyConfig(aioUserEvent *event, aioTimer**timer, uint128 *current)
{
  eventTimerStop(event, *timer);

  uint64_t period = eventTimerControlPeriod(*current);
  uint32_t counter = eventTimerControlCounter(*current);
  uint32_t generation = eventTimerControlGeneration(*current);

  uint128 active = {0, (uint64_t)generation | EVENT_TIMER_OWNER};
  uint128 expected = *current;
  if (!__uint128_atomic_compare_and_swap(&event->timerControl, &expected, active)) {
    *current = expected;
    return 0;
  }
  *current = active;

  if (period) {
    int armed = event->header.base->methodImpl.updateEventTimer(event, etuStart, generation, period);
    *timer = eventTimerLoad(event, amoRelaxed);
    if (*timer) {
      // Owner-exclusive writes; remaining == 0 means unlimited, so an armed schedule's finiteness is readable from remaining alone.
      aioTimerUserEventState *state = eventTimerState(*timer);
      state->remaining = counter;
      state->armed = (uint32_t)armed;
      state->period = period;
    }
  }
  return 1;
}

// One attempt of the owner's release CAS: drop the OWNER bit from the control word exactly as observed. Failure means a concurrent publication
// landed;
// *current is refreshed and the owner must reconcile it before retrying.
static inline int eventTimerTryRelease(aioUserEvent *event, uint128 *current)
{
  uint128 unlocked = *current;
  unlocked.high &= ~EVENT_TIMER_OWNER;
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
  uint32_t appliedGeneration = eventTimerControlGeneration(applied);

  for (;;) {
    if (eventTimerControlGeneration(current) != appliedGeneration) {
      if (eventTimerApplyConfig(event, &timer, &current))
        appliedGeneration = eventTimerControlGeneration(current);
      state = timer ? eventTimerState(timer) : 0;
      continue;
    }

    uint64_t pending = eventTimerControlPendingTicks(current);
    if (pending) {
      // A user owner discards the raced input without spending the private counter. Zero tells an IOCP backend to rearm even a one-shot last
      // tick.
      uint128 drained = {0, current.high};
      uint128 expected = current;
      if (!__uint128_atomic_compare_and_swap(&event->timerControl, &expected, drained)) {
        current = expected;
        continue;
      }
      current = drained;
      if (state && state->armed)
        (void)event->header.base->methodImpl.consumeEventTimerTick(event, 0, appliedGeneration, state->period);
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
  uint32_t appliedGeneration = eventTimerControlGeneration(applied);
  uintptr_t deliveryCount = 0;

  for (;;) {
    if (eventTimerControlGeneration(current) != appliedGeneration) {
      if (eventTimerApplyConfig(event, &timer, &current))
        appliedGeneration = eventTimerControlGeneration(current);
      state = timer ? eventTimerState(timer) : 0;
      continue;
    }

    uint64_t pending = eventTimerControlPendingTicks(current);
    if (pending) {
      uint128 drained = {0, current.high};
      uint128 expected = current;
      if (!__uint128_atomic_compare_and_swap(&event->timerControl, &expected, drained)) {
        current = expected;
        continue;
      }
      current = drained;
      if (!state || !state->armed)
        continue;

      uint64_t count = event->header.base->methodImpl.consumeEventTimerTick(event, pending, appliedGeneration, state->period);
      if (state->remaining) {
        // Armed finite schedule (unlimited keeps remaining at zero).
        if (count > state->remaining)
          count = state->remaining;
        state->remaining -= (uint32_t)count;
        if (!state->remaining)
          eventTimerStop(event, timer);
      }
      if (count) {
        if (!event->callback && *deliveryGeneration != appliedGeneration)
          deliveryCount = 0;
        *deliveryGeneration = appliedGeneration;
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
  // The single saturation point for event periods: userEventStartTimer and ioSleep both funnel through here (stop/terminal calls pass 0).
  if (period > MAX_TIMEOUT_US)
    period = MAX_TIMEOUT_US;
  for (;;) {
    // The acquire load pairs with the terminal publication's CAS: a word that shows any post-terminal state guarantees the DELETE bit below is
    // visible, so no publication can slip in after the terminal one. The deletion's own publication is the single exemption from that gate.
    uint128 old = __uint128_atomic_load(&event->timerControl);
    if (!terminal && eventReferenceIsDeleting(event))
      return 0;

    uint32_t generation = eventTimerControlGeneration(old);
    if (requiredGeneration && generation != requiredGeneration)
      return 0;

    uint32_t nextGeneration = eventTimerNextGeneration(generation);
    uint32_t finiteCounter = counter > 0 ? (uint32_t)counter : 0;
    uint128 desired = {period, (uint64_t)nextGeneration | ((uint64_t)finiteCounter << EVENT_TIMER_COUNTER_SHIFT) | EVENT_TIMER_OWNER};
    uint128 expected = old;
    if (!__uint128_atomic_compare_and_swap(&event->timerControl, &expected, desired))
      continue;

    if (!(old.high & EVENT_TIMER_OWNER))
      eventTimerProcessUserOwner(event, old, desired);
    return nextGeneration;
  }
}

static void eventTimerCancelGeneration(aioUserEvent *event, uint32_t generation)
{
  if (generation)
    eventTimerPublishConfig(event, 0, 0, generation, 0);
}

void eventTimerSignal(aioUserEvent *event, uint32_t timerGeneration, uint64_t eventGeneration, uint64_t tickCount)
{
  // A stale harvested timer may outlive logical release and may inspect only Head while the rest of the pooled event is ASan-poisoned. The
  // cheap event-generation prefilter and the DWCAS claim must therefore precede any access to the timer control word or event tail.
  if (timerGeneration == 0 || eventHandleGeneration(event) != eventGeneration)
    return;
  if (!eventTimerTryClaimReference(event, eventGeneration))
    return;

  for (;;) {
    uint128 old = __uint128_atomic_load_relaxed(&event->timerControl);
    // A generation match proves the ACTIVE variant: the timer producing this tick was armed strictly after the CONFIG->ACTIVE flip of the same
    // generation, and any stop/cancel/delete advances the generation.
    if (eventTimerControlGeneration(old) != timerGeneration) {
      eventDecrementReference(event, 1);
      return;
    }

    uint128 desired = {old.low + tickCount, old.high | EVENT_TIMER_OWNER};
    uint128 expected = old;
    if (!__uint128_atomic_compare_and_swap(&event->timerControl, &expected, desired)) {
      if (eventTimerControlGeneration(expected) != timerGeneration) {
        eventDecrementReference(event, 1);
        return;
      }
      continue;
    }

    if (!(old.high & EVENT_TIMER_OWNER)) {
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
  uint128 expected = __uint128_atomic_load_relaxed(&event->waiter);
  for (;;) {
    int kind = (int)(expected.low & ewtMask);
    if (kind == ewtDeleted)
      return;
    // A manual wake publishes cancellation before competing for an ioSleep waiter. A timer validates after loading waiter: any interleaved
    // transition bumps the sequence and fails the CAS, so an old tick can steal neither a later credit nor a reinstalled identical sleeper.
    if (manual && kind == ewtSleep)
      eventTimerPublishConfig(event, 0, 0, 0, 0);
    else if (generation && eventTimerGeneration(event) != generation)
      return;
    uint128 desired = {kind ? (count - 1) * ewtCreditUnit : expected.low + count * ewtCreditUnit, expected.high + 1};
    if (!__uint128_atomic_compare_and_swap(&event->waiter, &expected, desired))
      continue;
    if (!kind)
      return;
    coroutineTy *coroutine = (coroutineTy*)(uintptr_t)(expected.low & ~(uint64_t)ewtMask);
    assert(coroutineIsMain() && "User-event coroutine delivery must run in the main coroutine");
    coroutineCall(coroutine);
    return;
  }
}

static void eventCoroutineCancel(aioUserEvent *event)
{
  uint128 old = __uint128_atomic_load_relaxed(&event->waiter);
  for (;;) {
    unsigned kind = (unsigned)(old.low & ewtMask);
    if (kind == ewtDeleted)
      return;
    uint128 desired = {ewtDeleted, old.high + 1};
    if (__uint128_atomic_compare_and_swap(&event->waiter, &old, desired)) {
      if (kind == ewtUser || kind == ewtSleep) {
        assert(coroutineIsMain() && "User-event coroutine cancellation must run in the main coroutine");
        coroutineCall((coroutineTy*)(uintptr_t)(old.low & ~(uint64_t)ewtMask));
      }
      return;
    }
  }
}

static void eventActivateCancellation(aioUserEvent *event)
{
  // DELETE makes every pending manual count irrelevant. Always post a fresh personal doorbell: a nonzero signalState may belong to a normal
  // activation whose owner has not reached its backend call yet, so cancellation must not wait for that thread to make progress.
  __uintptr_atomic_store(&event->signalState, 1, amoRelease);
  (void)event->header.base->methodImpl.activate(event);
}

static int eventInstallWaiter(aioUserEvent *event, unsigned waiterKind)
{
  uint128 expected = __uint128_atomic_load_relaxed(&event->waiter);
  for (;;) {
    if ((expected.low & ewtMask) == ewtDeleted)
      return 0;
    assert((expected.low & ewtMask) == ewtCredits && "Only one coroutine may wait on a user event");
    if (expected.low == 0) {
      uintptr_t coroutine = (uintptr_t)coroutineCurrent();
      assert(coroutine && !(coroutine & ewtMask) && "Coroutine pointer is not sufficiently aligned");
      uint128 desired = {coroutine | waiterKind, expected.high + 1};
      if (!__uint128_atomic_compare_and_swap(&event->waiter, &expected, desired))
        continue;
      // The second same-word RMW against Delete schedules cancellation.
      uint64_t oldTag = __uint64_atomic_fetch_and_add(&event->header.tag.low, TAG_EVENT_WAITER_COMMITTED, amoRelease);
      if (oldTag & TAG_EVENT_DELETE)
        eventActivateCancellation(event);
      return 1;
    }
    // Credits are consumed by CAS only: a concurrent delete may replace them with the terminal sentinel, which arithmetic would corrupt.
    uint128 desired = {expected.low - ewtCreditUnit, expected.high + 1};
    if (__uint128_atomic_compare_and_swap(&event->waiter, &expected, desired))
      return 0;
  }
}

static void eventFinishWaiter(aioUserEvent *event)
{
  // Clear the rendezvous after Yield; this bit is not a reference.
  (void)__uint64_atomic_fetch_and_add(&event->header.tag.low, 0ULL - TAG_EVENT_WAITER_COMMITTED, amoRelaxed);
}

static void eventProcessCancellation(aioUserEvent *event)
{
  // The waiter sentinel stays terminal, so a late kernel owner cannot turn a timer signal into a credit.
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

  // A callback readiness which claimed before delete remains accepted and must still be delivered. Callback-less events instead use the
  // personal doorbell as the asynchronous cancellation path for a committed waiter.
  if (!event->callback && eventReferenceIsDeleting(event))
    eventProcessCancellation(event);
  else if (event->callback)
    eventDeliverCallbacks(event, count);
  else
    eventCoroutineResume(event, count, 1, 0);
}

aioUserEvent *newUserEvent(asyncBase *base, int isSemaphore, aioEventCb callback, void *arg)
{
  aioUserEvent *event = (aioUserEvent*)objectAlloc(&base->eventPool, sizeof(aioUserEvent), TAGGED_POINTER_ALIGNMENT);
  if (!event)
    return 0;

  // A stale kernel envelope may inspect only Head while this slot is pooled or being reused. Keep the optional backend timer paired with the
  // slot, preserve its monotonic event generation, reset the tail, and publish the live refcount only after initialization is complete.
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
    state->period = 0;
  }
  // The schedule generation survives reuse: reactors validate a stale envelope against the live tag.high and read the CURRENT incarnation from
  // timer->event.generation, so restarting the numbering at 1 would let the previous incarnation's unprocessed readiness pass every gate. Only
  // pending ticks, the finite counter and OWNER reset (a pooled cell's owner has always released: OWNER-clear is the resting state).
  uint64_t scheduleGeneration = (uint32_t)__uint64_atomic_load(&event->timerControl.high, amoRelaxed);
  __uint64_atomic_store(&event->timerControl.low, 0, amoRelaxed);
  __uint64_atomic_store(&event->timerControl.high, scheduleGeneration, amoRelaxed);
  __uint64_atomic_store(&event->waiter.low, 0, amoRelaxed);
  __uint64_atomic_store(&event->waiter.high, 0, amoRelaxed);
  __uintptr_atomic_store(&event->signalState, 0, amoRelaxed);
  event->destructorCb = 0;
  event->destructorCbArg = 0;
  event->activationId = UINT64_MAX;

  // Publish every field above and the incarnation bump performed by the preceding final release. Kernel claims acquire this word before
  // accepting an incarnation, so it must be the last initialization store.
  __uint64_atomic_store(&event->header.tag.low, 1, amoRelease);
  if (!base->methodImpl.initializeUserEvent(event)) {
    __uint64_atomic_store(&event->header.tag.high, generation + 1, amoRelaxed);
    __uint64_atomic_store(&event->header.tag.low, TAG_EVENT_DELETE, amoRelaxed);
    base->methodImpl.releaseUserEvent(event);
    return 0;
  }
  return event;
}

void userEventStartTimer(aioUserEvent *event, uint64_t usTimeout, int counter)
{
  assert(usTimeout > 0 && "A user-event timer period must be non-zero");
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
  if (event->header.isSemaphore)
    old = __uintptr_atomic_fetch_and_add(&event->signalState, 1, amoRelease);
  else
    old = __uintptr_atomic_exchange(&event->signalState, 1, amoRelease);

  // The personal descriptor/completion is only a manual doorbell. Once one is pending, signalState carries the exact count or the coalescing
  // gate. The 0->nonzero edge owns the single doorbell. Interruptible syscall failures are handled inside the backend; common code never spins
  // here.
  if (old == 0)
    (void)event->header.base->methodImpl.activate(event);
}

void deleteUserEvent(aioUserEvent *event)
{
  // Arbitrate with waiter commit; acquire imports its published pointer.
  uint64_t oldTag = __uint64_atomic_fetch_and_add(&event->header.tag.low, TAG_EVENT_DELETE, amoAcquire);

  eventTimerPublishConfig(event, 0, 0, 0, 1);
  if (event->callback == 0 && (oldTag & TAG_EVENT_WAITER_COMMITTED))
    eventActivateCancellation(event);
  eventDecrementReference(event, 1);
}

void ioSleep(aioUserEvent *event, uint64_t usTimeout)
{
  if (eventReferenceIsDeleting(event))
    return;

  uint128 expected = __uint128_atomic_load_relaxed(&event->waiter);
  while (expected.low) {
    // Same CAS-only credit consume as eventInstallWaiter: a delete landing after the liveness check above must find its sentinel intact.
    if ((expected.low & ewtMask) == ewtDeleted)
      return;
    assert((expected.low & ewtMask) == ewtCredits && "Only one coroutine may wait on a user event");
    uint128 desired = {expected.low - ewtCreditUnit, expected.high + 1};
    if (__uint128_atomic_compare_and_swap(&event->waiter, &expected, desired))
      return;
  }

  assert(usTimeout > 0 && "ioSleep timeout must be non-zero");
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
