// Timeout grid: exact cascading hierarchical timer wheel on 128-bit CAS.
//
// Geometry: TIMER_WHEEL_LEVELS levels of TIMER_WHEEL_SLOTS slots; level L
// covers windows of 2^(10L) ticks and rotates every 2^(10(L+1)) ticks. A link
// carries its absolute 64-bit deadline tick, so a slot visit either delivers
// it (deadline reached), re-cascades it to a lower level, or drops it when
// the operation's generation moved on (lazy cancellation).
//
// Concurrency in this stage: producers publish lock-free through the slot
// pair CAS; the sweep is still serialized by timerMapLock and the producer
// does not yet validate the observed baseTick against the deadline - a link
// armed into an already-swept window is delivered one rotation late instead
// of immediately (the terminal-window protocol is the next stage).

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
  base->lastCheckPoint = (uintptr_t)currentTick;
  base->timerMapLock = 0;
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

void timerWheelInsert(asyncBase *base, asyncOpListLink *link, uint64_t cursor)
{
  unsigned level;
  unsigned index;
  uint64_t deadline = link->deadlineTick;
  uint64_t diff = deadline ^ cursor;
  if (diff >> (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS)) {
    // Beyond the wheel range: clamp into the farthest future slot of the top
    // level; its visit re-cascades the link with a fresher cursor, so no
    // precision is lost - the link carries the exact deadline
    level = TIMER_WHEEL_LEVELS - 1;
    index = ((unsigned)(cursor >> (TIMER_WHEEL_LEVEL_BITS * level)) + TIMER_WHEEL_SLOTS - 1) %
            TIMER_WHEEL_SLOTS;
  } else {
    level = diff ? highestBitIndex64(diff) / TIMER_WHEEL_LEVEL_BITS : 0;
    index = (unsigned)(deadline >> (TIMER_WHEEL_LEVEL_BITS * level)) % TIMER_WHEEL_SLOTS;
  }

  // Publish into the slot's current incarnation, whatever it is: the pair CAS
  // keeps head and baseTick consistent, and a link attached to a rotation
  // other than its deadline's is straightened out by that rotation's visit
  // (re-cascade forward, deliver late for a missed window - never early:
  // delivery always re-checks deadlineTick). Rejecting a swept window at
  // publication time is the next stage of the protocol.
  volatile uint128Pair *slot = &base->timerWheel.slots[level][index];
  uint128Pair observed = __uint128_atomic_load(slot);
  for (;;) {
    link->next = (asyncOpListLink*)(uintptr_t)observed.low;
    uint128Pair desired;
    desired.low = (uint64_t)(uintptr_t)link;
    desired.high = observed.high;
    if (__uint128_atomic_compare_and_swap(slot, &observed, desired))
      return;
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
  timerWheelInsert(base, timerLink, __uintptr_atomic_load(&base->lastCheckPoint, amoRelaxed));
}

// Drain the slot owning the window that starts at windowStart and process the
// detached list. Drain and reopen for the next rotation are one CAS, so a
// producer either published into the old incarnation (and its link is in the
// detached list) or publishes into the new one - a link cannot be lost in
// between.
static void timerWheelVisit(asyncBase *base, unsigned level, uint64_t windowStart)
{
  unsigned index = (unsigned)(windowStart >> (TIMER_WHEEL_LEVEL_BITS * level)) % TIMER_WHEEL_SLOTS;
  volatile uint128Pair *slot = &base->timerWheel.slots[level][index];

  uint128Pair observed = __uint128_atomic_load(slot);
  uint128Pair desired;
  do {
    // Already reopened past this window (an overlapping catch-up): the links
    // for it were taken by that visit, nothing to do here
    if (observed.high > windowStart)
      return;
    desired.low = 0;
    desired.high = windowStart + timerWheelPeriod(level);
  } while (!__uint128_atomic_compare_and_swap(slot, &observed, desired));

  asyncOpListLink *link = (asyncOpListLink*)(uintptr_t)observed.low;
  while (link) {
    asyncOpListLink *next = link->next;
    if (opGetGeneration(link->op) != link->generation) {
      // The operation completed and moved on long ago; lazy cancellation ends
      // here without touching anything beyond the atomic tag
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    } else if (link->deadlineTick <= windowStart) {
      opCancel(link->op, link->generation, aosTimeout);
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    } else {
      // Cascade: the deadline is inside (or past the start of) a narrower
      // window; re-route from the tick being processed. Strictly monotonic
      // progress - the target level only shrinks for an in-window deadline
      timerWheelInsert(base, link, windowStart);
    }
    link = next;
  }
}

void processTimeoutQueue(asyncBase *base, uint64_t currentTick)
{
  // The lock-free fast check races with the update below only formally: a
  // stale (smaller) value just leads to taking the lock and sweeping an
  // empty range, the checkpoint itself is only advanced under the lock
  if (__uintptr_atomic_load(&base->lastCheckPoint, amoRelaxed) >= currentTick ||
      !__spinlock_try_acquire(&base->timerMapLock))
    return;

  // Visit order inside one tick: upper levels first, then level 0 - a link
  // whose deadline sits exactly on a window boundary must migrate down and
  // fire in this very tick, not one rotation later
  for (uint64_t tick = base->lastCheckPoint; tick < currentTick; tick++) {
    for (unsigned level = TIMER_WHEEL_LEVELS - 1; level >= 1; level--) {
      if ((tick & (timerWheelWidth(level) - 1)) == 0)
        timerWheelVisit(base, level, tick);
    }
    timerWheelVisit(base, 0, tick);
  }

  __uintptr_atomic_store(&base->lastCheckPoint, (uintptr_t)currentTick, amoRelaxed);
  __spinlock_release(&base->timerMapLock);
}
