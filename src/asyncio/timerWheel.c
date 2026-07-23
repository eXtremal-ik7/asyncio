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
#if defined(OS_WINDOWS)
#include <realtimeapiset.h>
#endif

// Build-time guards for the retunable grid constants: the clock formulas
// divide by TICK/1000 and 1000000/TICK exactly, the nanosecond divisor must
// fit an int, the occupancy bitmap packs whole 64-bit words per level and
// the range shift must stay inside 64 bits.
#if defined(_MSC_VER) && !defined(__clang__)
// MSVC C mode runs without /std:c11 and lacks _Static_assert; a negative
// array size fails the build the same way
typedef char timerTickAtLeastOneMillisecond[TIMER_TICK_MICROSECONDS >= 1000 ? 1 : -1];
typedef char timerTickWholeMilliseconds[TIMER_TICK_MICROSECONDS % 1000 == 0 ? 1 : -1];
typedef char timerTickDividesOneSecond[1000000 % TIMER_TICK_MICROSECONDS == 0 ? 1 : -1];
typedef char timerTickNanosecondDivisorFitsInt[TIMER_TICK_MICROSECONDS <= 2147483 ? 1 : -1];
typedef char timerWheelOccupancyWholeWords[TIMER_WHEEL_SLOTS % 64 == 0 ? 1 : -1];
typedef char timerWheelRangeShiftBelow64[TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS < 64 ? 1 : -1];
#else
_Static_assert(TIMER_TICK_MICROSECONDS >= 1000, "millisecond conversions divide by TICK/1000");
_Static_assert(TIMER_TICK_MICROSECONDS % 1000 == 0, "kernel timeouts are whole milliseconds");
_Static_assert(1000000 % TIMER_TICK_MICROSECONDS == 0, "seconds must convert to whole ticks");
_Static_assert(TIMER_TICK_MICROSECONDS <= 2147483, "TICK * 1000 (ns per tick) must fit in int");
_Static_assert(TIMER_WHEEL_SLOTS % 64 == 0, "occupancy bitmap packs whole 64-bit words");
_Static_assert(TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS < 64, "wheel range shift must stay below 64");
#endif

static ConcurrentQueue asyncOpLinkListPool;

static void timerKickUncoveredSleepers(asyncBase *base, uint64_t deadline);

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

static inline unsigned lowestBitIndex64(uint64_t value)
{
#if defined(_MSC_VER) && !defined(__clang__)
  unsigned long index;
  _BitScanForward64(&index, value);
  return (unsigned)index;
#else
  return (unsigned)__builtin_ctzll(value);
#endif
}

// Occupancy bit maintenance (see the protocol comment at struct timerWheel).
// Set participates in the seq-cst lost-wakeup order against the sleeper's
// ordinary bitmap loads. Clear stays an RMW because it must not erase a set
// racing the drain/reopen transition.
static inline void timerWheelMarkOccupied(asyncBase *base, unsigned level, unsigned index)
{
  __uintptr_atomic_fetch_or(&base->timerWheel.occupancy[level][index >> 6],
                            (uintptr_t)1 << (index & 63),
                            amoSeqCst);
}

static inline void timerWheelClearOccupied(asyncBase *base, unsigned level, unsigned index)
{
  __uintptr_atomic_fetch_and(&base->timerWheel.occupancy[level][index >> 6],
                             ~((uintptr_t)1 << (index & 63)),
                             amoSeqCst);
}

static inline int timerWheelIsOccupied(asyncBase *base, unsigned level,
                                       unsigned index)
{
  uintptr_t bits = __uintptr_atomic_load(
    &base->timerWheel.occupancy[level][index >> 6], amoSeqCst);
  return (bits & ((uintptr_t)1 << (index & 63))) != 0;
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
// a forward step would fire pending timeouts early. Active-time semantics on
// every platform: CLOCK_MONOTONIC (Linux, macOS) and unbiased interrupt time
// exclude suspend/hibernation, matching the kernel waits the backends sleep
// in - a relative GetQueuedCompletionStatusEx timeout does not run in
// low-power states either (Windows 8+), so a suspend-counting clock would
// declare deadlines overdue while the wait still had its full active
// remainder to serve. All grid sites (arm, sweep, checkpoint init) must
// share this one clock. The sources cannot fail on supported targets; fail
// stop beats reading an undefined time.
uint64_t getMonotonicTicks(void)
{
#if defined(OS_WINDOWS)
  ULONGLONG interruptTime100ns;
  if (!QueryUnbiasedInterruptTime(&interruptTime100ns))
    abort();
  return interruptTime100ns / (TIMER_TICK_MICROSECONDS * 10u);
#else
  struct timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
    abort();
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
    for (unsigned i = 0; i < TIMER_WHEEL_SLOTS / 64; i++)
      base->timerWheel.occupancy[level][i] = 0;
  }
  base->timerCloseCursor = (uintptr_t)currentTick;
}

void timerWheelTeardown(asyncBase *base)
{
  // Full-quiescence contract: no loop threads, no in-flight armers and no
  // sweeper still owning a detached chain - a concurrent publish or drain
  // would race these plain stores. Links are recycled without delivery,
  // their operations are dead or dying with the base
  for (unsigned level = 0; level < TIMER_WHEEL_LEVELS; level++) {
    for (unsigned i = 0; i < TIMER_WHEEL_SLOTS; i++) {
      volatile uint128 *slot = &base->timerWheel.slots[level][i];
      uint128 observed = __uint128_atomic_load_relaxed(slot);
      asyncOpListLink *link = (asyncOpListLink*)(uintptr_t)observed.low;
      slot->low = 0;
      while (link) {
        asyncOpListLink *next = link->next;
        objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
        link = next;
      }
    }
    for (unsigned i = 0; i < TIMER_WHEEL_SLOTS / 64; i++)
      base->timerWheel.occupancy[level][i] = 0;
  }
}

// One publication attempt into the slot incarnation held in *observed: push
// the link onto the observed head with the pair CAS (head and baseTick are
// validated together) and ensure the occupancy bit. Every successful
// publisher sets the bit itself - the OR is idempotent, and gating it on the
// empty -> non-empty transition would chain this arm's visibility to the
// first publisher, which may be stalled between its CAS and its set: the
// stacked arm would return with its link parked bitless, sleep planning
// (which reads only the bitmap) would commit to an eternal wait, and the
// timeout would hinge on the stalled thread resuming. The set runs after the
// publication, so this arm's own bitless window is closed by its own set
// before the insert returns. On a CAS loss *observed holds the fresh slot
// value and the caller decides whether that incarnation is still the right
// target.
static int timerWheelTryPublish(asyncBase *base, unsigned level, unsigned index,
                                uint128 *observed, asyncOpListLink *link)
{
  volatile uint128 *slot = &base->timerWheel.slots[level][index];
  link->next = (asyncOpListLink*)(uintptr_t)observed->low;
  uint128 desired;
  desired.low = (uint64_t)(uintptr_t)link;
  desired.high = observed->high;
  if (!__uint128_atomic_compare_and_swap(slot, observed, desired))
    return 0;
  timerWheelMarkOccupied(base, level, index);
  return 1;
}

int timerWheelInsert(asyncBase *base, asyncOpListLink *link, uint64_t cursor)
{
  uint64_t deadline = link->deadlineTick;
  const uint64_t range =
    (uint64_t)1 << (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS);

  for (;;) {
    // The deadline's tick is already confirmed swept: its window was visited,
    // the arm is late. Strictly "<": tick == cursor is the first unconfirmed
    // tick, its window may not have been visited yet and the deadline is not
    // due before that visit anyway - expiring it here would fire early.
    // Live only for the caller's cursor: every refresh assigns
    // expectedBase <= deadline, so re-entry cannot trip it (kept as a cheap
    // invariant guard)
    if (deadline < cursor)
      return 0;

    // Route by numeric distance, not by the differing radix prefix. Near
    // deadlines stay near even when cursor and deadline straddle a power-of-
    // two boundary. A far deadline is represented by the last point inside
    // the wheel's current numeric range; using the deadline itself after
    // merely clamping the level could target an incarnation not open yet.
    uint64_t distance = deadline - cursor;
    uint64_t routeDistance = distance < range ? distance : range - 1;
    uint64_t routeTick = cursor + routeDistance;
    unsigned level = routeDistance
      ? highestBitIndex64(routeDistance) / TIMER_WHEEL_LEVEL_BITS : 0;
    uint64_t width = timerWheelWidth(level);
    uint64_t expectedBase = routeTick & ~(width - 1);
    unsigned index = (unsigned)(routeTick >>
      (TIMER_WHEEL_LEVEL_BITS * level)) & (TIMER_WHEEL_SLOTS - 1);

    // Publish only into the incarnation that covers the deadline: the pair
    // CAS validates head and baseTick together, so "published into a foreign
    // rotation" cannot happen. A CAS loss against a concurrent push reloads
    // the head and retries; a loss against a visit changes baseTick and is
    // caught by the incarnation check
    volatile uint128 *slot = &base->timerWheel.slots[level][index];
    uint128 observed = __uint128_atomic_load_relaxed(slot);
    while (observed.high == expectedBase) {
      if (timerWheelTryPublish(base, level, index, &observed, link)) {
        // The insert owns activation duty as well as bitmap publication. This
        // is deliberately per link: a sleeper can scan between two links of
        // the same detached chain being re-published.
        timerKickUncoveredSleepers(base, deadline);
        return 1;
      }
    }

    // The window's incarnation is gone - its visit already ran (baseTick
    // moves only forward, in whole rotations). At level 0 the window IS the
    // routed tick: the deadline is due, refuse the publication. Higher up the
    // deadline may still lie ahead; take the drained window's start as a
    // proven sweep position and route again. Normal in-range routing strictly
    // descends a level; a far route may first advance its stale range hint.
    if (level == 0)
      return 0;
    // The mismatch itself was observed by a relaxed load. A pinned iteration
    // can only observe drain-written values (the incoming pin forbids older
    // ones), so this acquire fence makes that drain synchronize-with us and,
    // through the drainer's own acquire cursor read, pins every visit below
    // expectedBase for the next iteration - the same induction as the first
    // route (see the cursor comment in addToTimeoutQueue). Without it a
    // weakly-ordered CPU may satisfy the next iteration's slot load early,
    // see a pre-reopen baseTick and falsely refuse at level 0 - an early
    // timeout up to a full lower-level rotation ahead of the deadline.
    __atomic_fence(amoAcquire);
    cursor = expectedBase;
  }
}

// Wake one loop if no published sleeper would meet the deadline in time: a
// sleeper waking at wakeTick sweeps every tick below it, so a lag of one
// tick is covered by the sleeper itself and anything longer needs the kick.
// The decision is the MINIMUM over the published horizons - one sleeper
// due at wakeTick <= deadline+1 serves this deadline no matter how late
// the others sleep, so kicking on the first late slot would only wake a
// thread whose work is already scheduled. Awake threads park their slots
// at UINTPTR_MAX and never attract kicks - they re-scan the occupancy
// bitmap before their next sleep. For every publisher of the deadline's
// link, the seq-cst order is:
//   sleeper horizon store -> missed bitmap load -> producer bit set
//                          -> producer horizon load.
// Thus a scan that missed this bit forces this load to observe that sleep
// episode's sentinel or its later, shorter horizon; a stale awake value is
// impossible. The same order legitimizes trusting a covering horizon: it
// is either the slot's current episode (that sleeper wakes in time and its
// sweep delivers this deadline, or wakes earlier and re-scans), or a stale
// value - and reading a stale value here proves this bit's set precedes
// that thread's next bitmap scan in the seq-cst order, so the scan sees
// the bit and the recomputed horizon covers the deadline again. Every
// publisher - a fresh arm, one stacked onto a live chain, or a sweeper
// re-parking a cascading link - performed its own bit set inside the insert
// and runs this check itself, so the litmus binds each publication
// independently; visibility never waits on another thread resuming. Every
// stale reading errs toward a spurious kick, never toward a lost wakeup.
// Coverage itself is a cooperative promise, not a transferable token: a loop
// that wakes early withdraws its horizon and re-establishes coverage only at
// its next prepareSleep, after finishing the in-flight dispatch batch (user
// callbacks included), so a covered deadline may trail that work. Timer
// progress deliberately rests on every loop thread continuing its loop;
// pause-tolerance across a stalled loop thread is out of contract.
static void timerKickUncoveredSleepers(asyncBase *base, uint64_t deadline)
{
  TimerLoopState *states = base->timerLoopStates;
  if (!states)
    return;
  unsigned limit = base->loopThreadLimit;
  int hasSleeper = 0;
  for (unsigned i = 0; i < limit; i++) {
    uintptr_t wakeTick =
      __uintptr_atomic_load(&states[i].wakeTick, amoSeqCst);
    if (wakeTick <= (uintptr_t)(deadline + 1))
      return;
    hasSleeper |= wakeTick != UINTPTR_MAX;
  }
  if (hasSleeper)
    base->methodImpl.wakeupLoop(base);
}

void addToTimeoutQueue(asyncBase *base, asyncOpRoot *op)
{
  asyncOpListLink *timerLink = 0;
  if (!objectPoolGet(&asyncOpLinkListPool, (void**)&timerLink, sizeof(asyncOpListLink)))
    timerLink = malloc(sizeof(asyncOpListLink));
  if (!timerLink) {
    aioObjectRoot *object = op->object;
    uintptr_t objectGeneration =
      objectHeaderGeneration(&object->header);
    (void)opCancel(op,
                   opGetGeneration(op),
                   aosUnknownError,
                   object,
                   objectGeneration);
    return;
  }
  timerLink->op = op;
  timerLink->generation = opGetGeneration(op);
  timerLink->object = op->object;
  timerLink->objectGeneration =
    objectHeaderGeneration(&op->object->header);
  timerLink->deadlineTick = op->deadlineTick;
  // timerId strictly before publication: the moment the link is in the slot a
  // concurrent sweep may deliver the timeout and recycle the operation, and a
  // late store would corrupt the next incarnation's field
  op->timerId = timerLink;
  // Acquire on the cursor is load-bearing for the refusal path: reading c
  // synchronizes with the release confirm of tick c-1 and, through the chain
  // of confirms, happens-after every slot visit of every tick below c. That
  // pins the slot reads inside the insert - a baseTick the sweep already left
  // behind cannot surface, so an incarnation mismatch proves the deadline's
  // window was drained. With a relaxed read the pair load may be satisfied
  // ahead of the cursor load, and a visit landing between the two would make
  // a still-open level-0 window look drained - the arm would be refused and
  // the timeout delivered up to a full level-0 rotation early. The descend
  // path re-establishes this pin per mismatch with an acquire fence inside
  // the insert
  if (!timerWheelInsert(base,
                        timerLink,
                        __uintptr_atomic_load(&base->timerCloseCursor,
                                              amoAcquire))) {
    // Expired: the deadline's window is already swept. The link never became
    // visible, so it is still producer-owned; recycle it and deliver the
    // timeout through the regular generation-gated cancel. The caller sees a
    // terminal status instead of a started timer (init paths must not issue
    // their I/O after this)
    uintptr_t generation = timerLink->generation;
    aioObjectRoot *object = timerLink->object;
    uintptr_t objectGeneration = timerLink->objectGeneration;
    op->timerId = 0;
    objectPoolPut(&asyncOpLinkListPool, timerLink, sizeof(asyncOpListLink));
    (void)opCancel(op,
                   generation,
                   aosTimeout,
                   object,
                   objectGeneration);
    return;
  }

}

// Drain the slot owning the window that starts at windowStart and reopen it
// for the next rotation as one CAS, so a producer either published into the
// old incarnation (and its link is in the detached chain) or publishes into
// the new one - a link cannot be lost in between. Exactly one caller gets the
// chain of an incarnation: every other visitor of the same window observes
// the advanced baseTick and returns 0 (the visit is idempotent).
asyncOpListLink *timerWheelDetach(asyncBase *base, TimerLoopState *state,
                                  unsigned level, uint64_t windowStart)
{
  unsigned index = (unsigned)(windowStart >> (TIMER_WHEEL_LEVEL_BITS * level)) % TIMER_WHEEL_SLOTS;
  volatile uint128 *slot = &base->timerWheel.slots[level][index];
  uintptr_t sequence = 0;
  int opened = 0;
  asyncOpListLink *chain = 0;

  uint128 observed = __uint128_atomic_load_relaxed(slot);
  for (;;) {
    if (observed.high > windowStart) {
      // Already reopened past this window (a concurrent visit or an
      // overlapping catch-up): the links for it were taken by that visit,
      // nothing to do here. A pre-clear issued below on an earlier attempt
      // may have landed after a fresh publication into the reopened
      // incarnation - a live chain must never sit bitless, so re-set the bit
      // when a chain is observed (over-setting is a spurious wakeup at worst)
      if (observed.low)
        timerWheelMarkOccupied(base, level, index);
      break;
    }
    if (!opened) {
      // Empty/bitless windows need no destructive bitmap clear and therefore
      // no pre-clear bracket. The strong DWCAS is still mandatory: it moves
      // the incarnation even when the head is null and acquires a bitless
      // chain from a publisher stalled before its occupancy set. A failed
      // CAS refreshes the pair; reload the bit before deciding again.
      if (!timerWheelIsOccupied(base, level, index)) {
        uint128 desired;
        desired.low = 0;
        desired.high = windowStart + timerWheelPeriod(level);
        if (__uint128_atomic_compare_and_swap(slot, &observed, desired)) {
          chain = (asyncOpListLink*)(uintptr_t)observed.low;
          break;
        }
        continue;
      }

      // Open the pre-clear bracket before the first destructive clear. The
      // odd value need only reach threads that observed the clear itself:
      // the plain store rides on the seq-cst clear RMW that follows it
      opened = 1;
      if (state) {
        sequence = __uintptr_atomic_load(&state->preclearSequence, amoRelaxed);
        __uintptr_atomic_store(&state->preclearSequence, sequence + 1,
                               amoRelaxed);
      } else {
        __uintptr_atomic_fetch_and_add(&base->timerPreclearOverflow,
                                       timerPreclearOverflowEntry,
                                       amoSeqCst);
      }
    }
    // Clear the occupancy bit strictly before the drain CAS: a publication
    // into the reopened incarnation is ordered after that CAS, so its set
    // cannot be erased by this clear; a bit re-set by a producer pushing into
    // the closing window survives past the drain as a spurious one. Between
    // this clear and the CAS the live chain sits bitless - sleepers stay
    // safe because the bracket around this window is still open
    timerWheelClearOccupied(base, level, index);
    uint128 desired;
    desired.low = 0;
    desired.high = windowStart + timerWheelPeriod(level);
    if (__uint128_atomic_compare_and_swap(slot, &observed, desired)) {
      chain = (asyncOpListLink*)(uintptr_t)observed.low;
      break;
    }
  }
  if (opened) {
    // Release order: a sleeper reading the even value must also see the bit
    // repair / drain resolution ordered before it
    if (state)
      __uintptr_atomic_store(&state->preclearSequence, sequence + 2,
                             amoRelease);
    else
      __uintptr_atomic_fetch_and_add(&base->timerPreclearOverflow, ~(uintptr_t)0, amoRelease);
  }
  return chain;
}

// Deliver, drop or re-cascade a detached chain. The chain is private to the
// detach winner, so a sweeper stalled here delays only these callbacks: the
// slot is already reopened for producers and the tick is confirmed by any
// helper, the rest of the wheel keeps running
void timerWheelProcessDetached(asyncBase *base, asyncOpListLink *link, uint64_t windowStart)
{
  while (link) {
    asyncOpListLink *next = link->next;
    // Local copy: past a successful re-insert the link belongs to the wheel
    // again and may be delivered and recycled concurrently
    uint64_t deadlineTick = link->deadlineTick;
    uintptr_t expectedTag =
      (link->generation << TAG_STATUS_SIZE) | aosPending;
    uintptr_t currentTag =
      __uintptr_atomic_load(&link->op->tag, amoRelaxed);
    if (currentTag != expectedTag) {
      // A terminal operation in the same generation is just as dead as a
      // recycled one. The tag is the only operation storage a stale link may
      // inspect; a stale pending read merely preserves the old lazy path.
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    } else if (deadlineTick > windowStart &&
               timerWheelInsert(base, link, windowStart)) {
      // Cascade: the deadline is inside a narrower window; re-route from the
      // tick being processed (the target level only shrinks for an in-window
      // deadline). timerWheelInsert performs this re-publication's activation
      // duty before returning.
    } else {
      // Due links and refused lower-window publications have the same
      // terminal action. The latter is a stalled owner resuming an old chain.
      (void)opCancel(link->op,
                     link->generation,
                     aosTimeout,
                     link->object,
                     link->objectGeneration);
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    }
    link = next;
  }
}

static void timerWheelVisit(asyncBase *base, TimerLoopState *state,
                            unsigned level, uint64_t windowStart)
{
  asyncOpListLink *chain = timerWheelDetach(base, state, level, windowStart);
  if (chain)
    timerWheelProcessDetached(base, chain, windowStart);
}

void timerWheelSweepTick(asyncBase *base, TimerLoopState *state, uint64_t tick)
{
  // Visit order inside one tick: upper levels first, then level 0 - a link
  // whose deadline sits exactly on a window boundary must migrate down and
  // fire in this very tick, not one rotation later
  for (unsigned level = TIMER_WHEEL_LEVELS - 1; level >= 1; level--) {
    if ((tick & (timerWheelWidth(level) - 1)) == 0)
      timerWheelVisit(base, state, level, tick);
  }
  timerWheelVisit(base, state, 0, tick);

  // Confirm the tick. The cursor only ever moves forward by 1 from the exact
  // tick just swept, so a sweeper resurfacing with a long-confirmed tick
  // cannot rewind it; losing the CAS means a helper sweeping the same tick
  // (all its visits are idempotent) confirmed it first. Release on success:
  // this CAS heads the release sequence every acquire read of the cursor
  // synchronizes with - reading a confirmed value must prove that every
  // visit below it happened (the helper loop carries that induction across
  // threads by re-reading the cursor with acquire before each tick). The
  // confirmer reads nothing after the CAS that depends on it, and a failed
  // confirm needs no ordering at all - the helper that won re-synchronizes
  // through its own acquire read
  __uintptr_atomic_compare_and_swap(&base->timerCloseCursor,
                                    (uintptr_t)tick,
                                    (uintptr_t)(tick + 1),
                                    amoRelease);
}

void processTimeoutQueue(asyncBase *base, TimerLoopState *state,
                         uint64_t currentTick)
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
    timerWheelSweepTick(base, state, tick);
}

// Earliest tick a sleeping loop must wake at to serve the parked timers, or
// UINT64_MAX when the whole wheel is empty. Each level is scanned in visit
// order - a full circular pass over its slots starting at the level's next
// visit boundary - and the earliest visit across levels wins: a level-0 bit
// at tick t asks for a wake at t+1 (the sweep of t runs on the first call
// with now > t); an occupied upper-level slot asks for its next visit
// boundary + 1, so a cascade is never overslept - the deadlines inside the
// window are re-routed by that visit and picked up by the following horizon
// scans. Every word the "nothing earlier" conclusion rests on is read with a
// seq-cst load. Together with the seq-cst horizon publication and the
// producer's seq-cst bit-set/horizon-load, the global order forbids both sides
// from missing each other: either this scan observes the fresh bit, or the
// producer observes this sleep episode's horizon and kicks when needed.
// Unlike the former fetch-add(0) scheme, empty scans never take ownership of
// the bitmap cache lines. A word is skipped only when its visit ticks cannot
// beat the best candidate already found - an arithmetic bound, not a read of
// mutable state, so the litmus still covers every slot the conclusion
// depends on. The full 68-word sweep runs only on the way into a deep sleep;
// a busy wheel terminates the level-0 pass on its first occupied word.
// The conclusion is bitmap-only and therefore blind to a chain whose bit an
// in-flight visit already cleared - the caller closes that by checking the
// pre-clear brackets around the scan
static uint64_t timerWheelNearestWake(asyncBase *base, uint64_t from)
{
  uint64_t best = UINT64_MAX;

  for (unsigned level = 0; level < TIMER_WHEEL_LEVELS; level++) {
    unsigned shift = TIMER_WHEEL_LEVEL_BITS * level;
    uint64_t width = timerWheelWidth(level);
    // Visit boundaries only grow with the level (a coarser round-up of the
    // same position): once one cannot beat the best, none of the higher ones
    // can
    uint64_t boundary = (from + width - 1) & ~(width - 1);
    if (boundary + 1 >= best)
      break;

    unsigned slotIndex = (unsigned)(boundary >> shift) & (TIMER_WHEEL_SLOTS - 1);
    uint64_t visitTick = boundary;
    unsigned remaining = TIMER_WHEEL_SLOTS;
    while (remaining) {
      if (visitTick + 1 >= best)
        break;
      unsigned bitPosition = slotIndex & 63;
      unsigned take = 64 - bitPosition;
      if (take > remaining)
        take = remaining;
      uintptr_t bits =
        __uintptr_atomic_load(&base->timerWheel.occupancy[level][slotIndex >> 6],
                              amoSeqCst);
      uintptr_t mask = take == 64 ? ~(uintptr_t)0 : ((((uintptr_t)1 << take) - 1) << bitPosition);
      uintptr_t hit = bits & mask;
      if (hit) {
        // The first occupied slot in visit order is the level's minimum
        uint64_t candidate = visitTick + ((uint64_t)(lowestBitIndex64(hit) - bitPosition) << shift) + 1;
        if (candidate < best)
          best = candidate;
        break;
      }
      visitTick += (uint64_t)take << shift;
      remaining -= take;
      slotIndex = (slotIndex + take) & (TIMER_WHEEL_SLOTS - 1);
    }
  }

  return best;
}

// Version and activity of every relevant pre-clear bracket. Per-loop
// sequences only grow. The overflow high half is its monotonic entry epoch;
// the low half is the active count and deliberately does not participate in
// the version, so closes can no longer cancel a per-loop increment.
typedef struct TimerPreclearSnapshot {
  uintptr_t version;
  int active;
} TimerPreclearSnapshot;

static TimerPreclearSnapshot timerPreclearSnapshot(
  asyncBase *base, TimerLoopState *ownState,
  TimerLoopState *states, unsigned limit)
{
  TimerPreclearSnapshot snapshot = {0, 0};
  for (unsigned i = 0; i < limit; i++) {
    if (&states[i] == ownState)
      continue;
    uintptr_t sequence =
      __uintptr_atomic_load(&states[i].preclearSequence, amoAcquire);
    if (sequence & 1) {
      snapshot.active = 1;
      return snapshot;
    }
    snapshot.version += sequence;
  }
  uintptr_t overflow = __uintptr_atomic_load(&base->timerPreclearOverflow, amoAcquire);
  snapshot.version += overflow >> 32;
  snapshot.active =
    (overflow & timerPreclearOverflowActiveMask) != 0;
  return snapshot;
}

uint64_t timerLoopPrepareSleep(asyncBase *base, TimerLoopState *state,
                               uint64_t currentTick)
{
  assert(state && "timer sleep requires the loop's owned state");
  TimerLoopState *states = base->timerLoopStates;
  unsigned limit = base->loopThreadLimit;
  assert(states && "timer loop states must exist while a loop is active");

  // The worst case this call may commit to goes out first: the scan below
  // must run with a published wakeTick no earlier than the final decision,
  // or a producer racing the scan would see an awake thread and skip the
  // kick while the scan misses its fresh bit. Every later store only
  // shrinks the horizon - a producer reading a stale larger value kicks
  // spuriously at worst
  __uintptr_atomic_store(&state->wakeTick, timerSleepEternal, amoSeqCst);

  // Scan from the confirmed sweep position when it lags the clock (a
  // concurrent sweeper mid-tick): due backlog then wakes immediately instead
  // of hiding behind a full window
  uint64_t cursor = (uint64_t)__uintptr_atomic_load(&base->timerCloseCursor, amoAcquire);
  uint64_t from = cursor < currentTick ? cursor : currentTick;

  // A visitor between its pre-clear and its drain CAS leaves a live chain
  // bitless anywhere in the wheel (the bit has no incarnation), invisible to
  // the scan. Trust the scan only when no bracket was open at either
  // snapshot and none turned over in between: reading a cleared bit orders
  // the bracket open before the closing snapshot, while a completed bracket
  // advances a monotonic component version. An active opening snapshot skips
  // the futile bitmap scan entirely. Otherwise cap at one tick behind the
  // scan origin: due backlog drains immediately, the pair-driven sweep
  // delivers without the bitmap.
  uint64_t wake = from + 1;
  TimerPreclearSnapshot before =
    timerPreclearSnapshot(base, state, states, limit);
  if (!before.active) {
    for (unsigned attempt = 0;;) {
      wake = timerWheelNearestWake(base, from);
      // No validation result can shorten the already-minimal horizon.
      if (wake == from + 1)
        break;
      TimerPreclearSnapshot after =
        timerPreclearSnapshot(base, state, states, limit);
      if (!after.active && after.version == before.version)
        break;
      if (after.active || ++attempt == 2) {
        if (wake > from + 1)
          wake = from + 1;
        break;
      }
      before = after;
    }
  }

  if (wake == UINT64_MAX) {
    // Nothing parked anywhere: wait with no timeout. The eternal sentinel
    // stays published - a later arm kicks through methodImpl.wakeupLoop,
    // while queue traffic carries its own doorbell.
    return UINT64_MAX;
  }

  // Far wakes are clamped to the kernel timeout range (epoll takes int
  // milliseconds); waking early is harmless, the loop re-scans and sleeps on
  uint64_t sleepTicks = wake > currentTick ? wake - currentTick : 0;
  const uint64_t sleepTicksLimit =
    0x7FFFFFFFu / (TIMER_TICK_MICROSECONDS / 1000);
  if (sleepTicks > sleepTicksLimit) {
    sleepTicks = sleepTicksLimit;
    wake = currentTick + sleepTicks;
  }
  __uintptr_atomic_store(&state->wakeTick, (uintptr_t)wake, amoRelease);
  return wake;
}

void timerLoopCancelSleep(TimerLoopState *state)
{
  // Awake: the thread sweeps on its own before its next sleep, kicking it
  // would be a wasted syscall. A producer racing this store at worst reads
  // the stale horizon and issues one spurious wakeup
  __uintptr_atomic_store(&state->wakeTick, UINTPTR_MAX, amoRelaxed);
}
