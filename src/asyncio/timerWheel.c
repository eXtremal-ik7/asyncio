// Timeout grid: exact cascading hierarchical timer wheel on 128-bit CAS.
//
// Geometry: TIMER_WHEEL_LEVELS levels of TIMER_WHEEL_SLOTS slots; level L
// covers windows of 2^(10L) ticks and rotates every 2^(10(L+1)) ticks. A link
// carries its absolute 64-bit deadline tick, so a slot visit either delivers
// it (deadline reached), re-cascades it to a lower level, or drops it when
// the operation's generation moved on (lazy cancellation).
//
// Concurrency: producers publish lock-free through the slot pair CAS and
// only into the exact incarnation covering the deadline - an already-reopened
// window is terminal for its deadlines, the arm is refused (expired) and the
// caller delivers the timeout instead of parking the link a rotation late.
// The sweep is a lock-free protocol of its own - the visit is an idempotent
// DWCAS drain+reopen and the cursor only moves by the exact tick->tick+1
// confirm CAS, which any thread may perform (helping). Any number of threads
// may sweep concurrently; there is no lock and no designated sweeper.

#include "asyncioImpl.h"
#include "atomic.h"
#include <stdlib.h>
#include <time.h>

ConcurrentQueue asyncOpLinkListPool;

static inline unsigned highestBitIndex64(uint64_t value)
{
#if defined(_MSC_VER) && !defined(__clang__)
  unsigned long index;
  _BitScanReverse64(&index, value);
  return (unsigned)index;
#else
  return 63u - (unsigned)__builtin_clzll(value);
#endif
}

// Window width of a level, in ticks
static inline uint64_t timerWheelWidth(unsigned level)
{
  return (uint64_t)1 << (TIMER_WHEEL_LEVEL_BITS * level);
}

// Rotation period of a level = width of the next level up
static inline uint64_t timerWheelPeriod(unsigned level)
{
  return (uint64_t)1 << (TIMER_WHEEL_LEVEL_BITS * (level + 1));
}

// Monotonic ticks for the timeout grid. Deliberately not derived from the
// wall clock (time(0)): a backward NTP/manual step would stall the grid (the
// checkpoint would sit ahead of "now" and the sweep would early-return), and
// a forward step would fire pending timeouts early. A monotonic source
// advances with true elapsed time regardless of any system-date change. All
// grid sites (arm, sweep, checkpoint init) must share this one clock.
uint64_t getMonotonicTicks(void)
{
#if defined(OS_WINDOWS)
  return GetTickCount64() / (TIMER_TICK_MICROSECONDS / 1000);
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * (1000000 / TIMER_TICK_MICROSECONDS) +
         (uint64_t)ts.tv_nsec / (TIMER_TICK_MICROSECONDS * 1000);
#endif
}

void timerWheelInit(asyncBase *base, uint64_t currentTick)
{
  // Plain stores: runs strictly before the base is published to other threads
  for (unsigned level = 0; level < TIMER_WHEEL_LEVELS; level++) {
    uint64_t width = timerWheelWidth(level);
    uint64_t period = timerWheelPeriod(level);
    for (unsigned i = 0; i < TIMER_WHEEL_SLOTS; i++) {
      uint64_t windowStart = (currentTick & ~(period - 1)) + i * width;
      if (windowStart < currentTick)
        windowStart += period;
      base->timerWheel.slots[level][i].low = 0;
      base->timerWheel.slots[level][i].high = windowStart;
    }
  }
  base->timerCloseCursor = (uintptr_t)currentTick;
}

void timerWheelTeardown(asyncBase *base)
{
  // Loop threads are already stopped: links are recycled without delivery,
  // their operations are dead or dying with the base
  for (unsigned level = 0; level < TIMER_WHEEL_LEVELS; level++) {
    for (unsigned i = 0; i < TIMER_WHEEL_SLOTS; i++) {
      volatile uint128Pair *slot = &base->timerWheel.slots[level][i];
      uint128Pair observed = __uint128_atomic_load(slot);
      asyncOpListLink *link = (asyncOpListLink*)(uintptr_t)observed.low;
      slot->low = 0;
      while (link) {
        asyncOpListLink *next = link->next;
        objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
        link = next;
      }
    }
  }
}

int timerWheelInsert(asyncBase *base, asyncOpListLink *link, uint64_t cursor)
{
  uint64_t deadline = link->deadlineTick;

  // The deadline's tick is already confirmed swept: its window was visited,
  // the arm is late. Strictly "<": tick == cursor is the first unconfirmed
  // tick, its window may not have been visited yet and the deadline is not
  // due before that visit anyway - expiring it here would fire early
  if (deadline < cursor)
    return 0;

  for (;;) {
    uint64_t diff = deadline ^ cursor;
    if (diff >> (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS)) {
      // Beyond the wheel range: clamp into the farthest future slot of the
      // top level; its visit re-cascades the link with a fresher cursor, so
      // no precision is lost - the link carries the exact deadline. That slot
      // is almost a whole top-level rotation ahead of the sweep, its visit
      // cannot race this publication - attaching to the observed incarnation
      // is safe here and only here
      unsigned level = TIMER_WHEEL_LEVELS - 1;
      unsigned index = ((unsigned)(cursor >> (TIMER_WHEEL_LEVEL_BITS * level)) + TIMER_WHEEL_SLOTS - 1) %
                       TIMER_WHEEL_SLOTS;
      volatile uint128Pair *slot = &base->timerWheel.slots[level][index];
      uint128Pair observed = __uint128_atomic_load(slot);
      for (;;) {
        link->next = (asyncOpListLink*)(uintptr_t)observed.low;
        uint128Pair desired;
        desired.low = (uint64_t)(uintptr_t)link;
        desired.high = observed.high;
        if (__uint128_atomic_compare_and_swap(slot, &observed, desired))
          return 1;
      }
    }

    unsigned level = diff ? highestBitIndex64(diff) / TIMER_WHEEL_LEVEL_BITS : 0;
    unsigned index = (unsigned)(deadline >> (TIMER_WHEEL_LEVEL_BITS * level)) % TIMER_WHEEL_SLOTS;
    uint64_t expectedBase = deadline & ~(timerWheelWidth(level) - 1);

    // Publish only into the incarnation that covers the deadline: the pair
    // CAS validates head and baseTick together, so "published into a foreign
    // rotation" cannot happen. A CAS loss against a concurrent push reloads
    // the head and retries; a loss against a visit changes baseTick and is
    // caught by the incarnation check
    volatile uint128Pair *slot = &base->timerWheel.slots[level][index];
    uint128Pair observed = __uint128_atomic_load(slot);
    while (observed.high == expectedBase) {
      link->next = (asyncOpListLink*)(uintptr_t)observed.low;
      uint128Pair desired;
      desired.low = (uint64_t)(uintptr_t)link;
      desired.high = expectedBase;
      if (__uint128_atomic_compare_and_swap(slot, &observed, desired))
        return 1;
    }

    // The window's incarnation is gone - its visit already ran (baseTick
    // moves only forward, in whole rotations). At level 0 the window IS the
    // deadline tick: the timeout is due, refuse the publication. Higher up
    // the deadline may still lie ahead inside the drained window - descend,
    // taking the drained window's start as a proven sweep position; the
    // deadline shares that prefix, so the level strictly decreases and the
    // loop is bounded by the number of levels
    if (level == 0)
      return 0;
    cursor = expectedBase;
  }
}

void addToTimeoutQueue(asyncBase *base, asyncOpRoot *op)
{
  asyncOpListLink *timerLink = 0;
  if (!objectPoolGet(&asyncOpLinkListPool, (void**)&timerLink, sizeof(asyncOpListLink)))
    timerLink = malloc(sizeof(asyncOpListLink));
  timerLink->op = op;
  timerLink->generation = opGetGeneration(op);
  timerLink->deadlineTick = timerDeadlineTick(op->endTime);
  // timerId strictly before publication: the moment the link is in the slot a
  // concurrent sweep may deliver the timeout and recycle the operation, and a
  // late store would corrupt the next incarnation's field
  op->timerId = timerLink;
  if (!timerWheelInsert(base, timerLink, __uintptr_atomic_load(&base->timerCloseCursor, amoRelaxed))) {
    // Expired: the deadline's window is already swept. The link never became
    // visible, so it is still producer-owned; recycle it and deliver the
    // timeout through the regular generation-gated cancel. The caller sees a
    // terminal status instead of a started timer (init paths must not issue
    // their I/O after this)
    uintptr_t generation = timerLink->generation;
    op->timerId = 0;
    objectPoolPut(&asyncOpLinkListPool, timerLink, sizeof(asyncOpListLink));
    opCancel(op, generation, aosTimeout);
  }
}

// Drain the slot owning the window that starts at windowStart and reopen it
// for the next rotation as one CAS, so a producer either published into the
// old incarnation (and its link is in the detached chain) or publishes into
// the new one - a link cannot be lost in between. Exactly one caller gets the
// chain of an incarnation: every other visitor of the same window observes
// the advanced baseTick and returns 0 (the visit is idempotent).
asyncOpListLink *timerWheelDetach(asyncBase *base, unsigned level, uint64_t windowStart)
{
  unsigned index = (unsigned)(windowStart >> (TIMER_WHEEL_LEVEL_BITS * level)) % TIMER_WHEEL_SLOTS;
  volatile uint128Pair *slot = &base->timerWheel.slots[level][index];

  uint128Pair observed = __uint128_atomic_load(slot);
  uint128Pair desired;
  do {
    // Already reopened past this window (a concurrent visit or an overlapping
    // catch-up): the links for it were taken by that visit, nothing to do here
    if (observed.high > windowStart)
      return 0;
    desired.low = 0;
    desired.high = windowStart + timerWheelPeriod(level);
  } while (!__uint128_atomic_compare_and_swap(slot, &observed, desired));

  return (asyncOpListLink*)(uintptr_t)observed.low;
}

// Deliver, drop or re-cascade a detached chain. The chain is private to the
// detach winner, so a sweeper stalled here delays only these callbacks: the
// slot is already reopened for producers and the tick is confirmed by any
// helper, the rest of the wheel keeps running
void timerWheelProcessDetached(asyncBase *base, asyncOpListLink *link, uint64_t windowStart)
{
  while (link) {
    asyncOpListLink *next = link->next;
    if (opGetGeneration(link->op) != link->generation) {
      // The operation completed and moved on long ago; lazy cancellation ends
      // here without touching anything beyond the atomic tag
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    } else if (link->deadlineTick <= windowStart) {
      opCancel(link->op, link->generation, aosTimeout);
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    } else if (!timerWheelInsert(base, link, windowStart)) {
      // Cascade: the deadline is inside a narrower window; re-route from the
      // tick being processed (the target level only shrinks for an in-window
      // deadline). A refused publication means the lower window is already
      // terminal - a stalled owner resuming an old chain - so the timeout is
      // due right now
      opCancel(link->op, link->generation, aosTimeout);
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    }
    link = next;
  }
}

static void timerWheelVisit(asyncBase *base, unsigned level, uint64_t windowStart)
{
  timerWheelProcessDetached(base, timerWheelDetach(base, level, windowStart), windowStart);
}

void timerWheelSweepTick(asyncBase *base, uint64_t tick)
{
  // Visit order inside one tick: upper levels first, then level 0 - a link
  // whose deadline sits exactly on a window boundary must migrate down and
  // fire in this very tick, not one rotation later
  for (unsigned level = TIMER_WHEEL_LEVELS - 1; level >= 1; level--) {
    if ((tick & (timerWheelWidth(level) - 1)) == 0)
      timerWheelVisit(base, level, tick);
  }
  timerWheelVisit(base, 0, tick);

  // Confirm the tick. The cursor only ever moves forward by 1 from the exact
  // tick just swept, so a sweeper resurfacing with a long-confirmed tick
  // cannot rewind it; losing the CAS means a helper sweeping the same tick
  // (all its visits are idempotent) confirmed it first
  __uintptr_atomic_compare_and_swap(&base->timerCloseCursor, (uintptr_t)tick, (uintptr_t)(tick + 1));
}

void processTimeoutQueue(asyncBase *base, uint64_t currentTick)
{
  // Relaxed fast check keeps the per-iteration cost of an idle grid to one
  // load; it races with the confirm CAS only formally - a stale (smaller)
  // value just falls through to the acquire re-read below
  if (__uintptr_atomic_load(&base->timerCloseCursor, amoRelaxed) >= currentTick)
    return;

  // No lock and no designated sweeper: visits are idempotent per window
  // incarnation and the cursor moves only by the exact confirm CAS inside
  // timerWheelSweepTick. Re-reading the cursor every round makes the loop a
  // helper - it never re-sweeps a tick another thread already confirmed, and
  // concurrent callers finish the tick of a stalled one instead of waiting
  uint64_t tick;
  while ((tick = (uint64_t)__uintptr_atomic_load(&base->timerCloseCursor, amoAcquire)) < currentTick)
    timerWheelSweepTick(base, tick);
}
