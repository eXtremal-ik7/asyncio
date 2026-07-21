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

static ConcurrentQueue asyncOpLinkListPool;

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
    for (unsigned i = 0; i < TIMER_WHEEL_SLOTS / 64; i++)
      base->timerWheel.occupancy[level][i] = 0;
  }
  base->timerCloseCursor = (uintptr_t)currentTick;
}

void timerWheelTeardown(asyncBase *base)
{
  // Loop threads are already stopped: links are recycled without delivery,
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

  for (;;) {
    // The deadline's tick is already confirmed swept: its window was visited,
    // the arm is late. Strictly "<": tick == cursor is the first unconfirmed
    // tick, its window may not have been visited yet and the deadline is not
    // due before that visit anyway - expiring it here would fire early.
    // Re-checked after each far-branch cursor refresh
    if (deadline < cursor)
      return 0;

    if ((deadline - cursor) >> (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS)) {
      // Far by numeric distance - the XOR image mistakes a near deadline
      // across a range binary boundary for far. The farthest top-level
      // slot's visit precedes the deadline and re-cascades the exact tick;
      // an incarnation mismatch means a stale hint - reroute from the
      // authoritative cursor
      unsigned level = TIMER_WHEEL_LEVELS - 1;
      unsigned index = ((unsigned)(cursor >> (TIMER_WHEEL_LEVEL_BITS * level)) + TIMER_WHEEL_SLOTS - 1) %
                       TIMER_WHEEL_SLOTS;
      uint64_t windowStart = (cursor & ~(timerWheelPeriod(level) - 1)) +
                             (uint64_t)index * timerWheelWidth(level);
      uint64_t expectedBase = windowStart > cursor ? windowStart
                                                   : windowStart + timerWheelPeriod(level);
      volatile uint128 *slot = &base->timerWheel.slots[level][index];
      // Routing snapshot only; the successful DWCAS inside the publish helper
      // is the publication acquire and validates both possibly torn words.
      uint128 observed = __uint128_atomic_load_relaxed(slot);
      while (observed.high == expectedBase) {
        if (timerWheelTryPublish(base, level, index, &observed, link))
          return 1;
      }
      cursor = (uint64_t)__uintptr_atomic_load(&base->timerCloseCursor, amoAcquire);
      continue;
    }

    // XOR picks the level; across a range binary boundary it overshoots the
    // top level for an in-range distance - clamp, expectedBase still targets
    // the deadline's own window
    uint64_t diff = deadline ^ cursor;
    unsigned level = diff ? highestBitIndex64(diff) / TIMER_WHEEL_LEVEL_BITS : 0;
    if (level >= TIMER_WHEEL_LEVELS)
      level = TIMER_WHEEL_LEVELS - 1;
    unsigned index = (unsigned)(deadline >> (TIMER_WHEEL_LEVEL_BITS * level)) % TIMER_WHEEL_SLOTS;
    uint64_t expectedBase = deadline & ~(timerWheelWidth(level) - 1);

    // Publish only into the incarnation that covers the deadline: the pair
    // CAS validates head and baseTick together, so "published into a foreign
    // rotation" cannot happen. A CAS loss against a concurrent push reloads
    // the head and retries; a loss against a visit changes baseTick and is
    // caught by the incarnation check
    volatile uint128 *slot = &base->timerWheel.slots[level][index];
    uint128 observed = __uint128_atomic_load_relaxed(slot);
    while (observed.high == expectedBase) {
      if (timerWheelTryPublish(base, level, index, &observed, link))
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
static void timerKickUncoveredSleepers(asyncBase *base, uint64_t deadline)
{
  if (!base->timerSleep)
    return;
  uintptr_t earliest = UINTPTR_MAX;
  for (unsigned i = 0; i < base->loopThreadLimit; i++) {
    uintptr_t wakeTick =
      __uintptr_atomic_load(&base->timerSleep[i].wakeTick, amoSeqCst);
    if (wakeTick < earliest)
      earliest = wakeTick;
    // A covering sleeper fixes the decision as "no kick"; later slots can
    // only lower the minimum further. No such exit for UINTPTR_MAX: an
    // awake slot covers nothing, a later slot may still demand the kick
    if (earliest <= (uintptr_t)(deadline + 1))
      break;
  }
  if (earliest != UINTPTR_MAX && earliest > (uintptr_t)(deadline + 1))
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
  timerLink->deadlineTick = timerDeadlineTick(op->endTime);
  // The deadline is read back after the publication for the wakeup check, and
  // a published link belongs to the wheel - keep a local copy
  uint64_t deadline = timerLink->deadlineTick;
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
  // the timeout delivered up to a full level-0 rotation early
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

  // A published link belongs to the wheel; the kick decision reads the local
  // deadline copy
  timerKickUncoveredSleepers(base, deadline);
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
  volatile uint128 *slot = &base->timerWheel.slots[level][index];
  // Bracket identity: a loop thread of this base owns a TimerSleepSlot,
  // anyone else shares the overflow word
  TimerSleepSlot *visitor =
    loopThreadBase == base && base->timerSleep && messageLoopThreadId < base->loopThreadLimit
      ? &base->timerSleep[messageLoopThreadId] : 0;
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
      // Open the pre-clear bracket before the first destructive clear. The
      // odd value need only reach threads that observed the clear itself:
      // the plain store rides on the seq-cst clear RMW that follows it
      opened = 1;
      if (visitor) {
        sequence = __uintptr_atomic_load(&visitor->preclearSequence, amoRelaxed);
        __uintptr_atomic_store(&visitor->preclearSequence, sequence + 1, amoRelaxed);
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
    if (visitor)
      __uintptr_atomic_store(&visitor->preclearSequence, sequence + 2, amoRelease);
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
    if (opGetGeneration(link->op) != link->generation) {
      // The operation completed and moved on long ago; lazy cancellation ends
      // here without touching anything beyond the atomic tag
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    } else if (deadlineTick <= windowStart) {
      (void)opCancel(link->op,
                     link->generation,
                     aosTimeout,
                     link->object,
                     link->objectGeneration);
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
    } else if (timerWheelInsert(base, link, windowStart)) {
      // Cascade: the deadline is inside a narrower window; re-route from the
      // tick being processed (the target level only shrinks for an in-window
      // deadline). The re-parking sweeper is this link's publisher now, with
      // a fresh arm's full activation duty: the bit set ran inside the
      // insert, the uncovered-sleeper check runs here - a sleeper that
      // scanned between the upper window's drain and this lower publication
      // has no other way to learn of the migrated deadline
      timerKickUncoveredSleepers(base, deadlineTick);
    } else {
      // A refused publication means the lower window is already terminal - a
      // stalled owner resuming an old chain - so the timeout is due right now
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

// Sum and parity of every pre-clear bracket (per-slot sequences plus the
// overflow word). Components only grow, so equal sums mean no bracket
// turnover between two snapshots.
typedef struct TimerPreclearSnapshot {
  uintptr_t sum;
  int active;
} TimerPreclearSnapshot;

static TimerPreclearSnapshot timerPreclearSnapshot(asyncBase *base)
{
  TimerPreclearSnapshot snapshot = {0, 0};
  for (unsigned i = 0; i < base->loopThreadLimit; i++) {
    uintptr_t sequence = __uintptr_atomic_load(&base->timerSleep[i].preclearSequence, amoAcquire);
    snapshot.sum += sequence;
    snapshot.active |= (int)(sequence & 1);
  }
  uintptr_t overflow = __uintptr_atomic_load(&base->timerPreclearOverflow, amoAcquire);
  snapshot.sum += overflow;
  snapshot.active |= (overflow & timerPreclearOverflowActiveMask) != 0;
  return snapshot;
}

uint32_t timerLoopPrepareSleep(asyncBase *base, unsigned threadId, uint64_t currentTick, uint32_t fallbackMs)
{
  const uint32_t tickMs = TIMER_TICK_MICROSECONDS / 1000;
  TimerSleepSlot *slot =
    base->timerSleep && threadId < base->loopThreadLimit ? &base->timerSleep[threadId] : 0;
  // A thread beyond the horizon array has no channel for the wakeup kick:
  // cap its wait at one grid tick so a deadline it cannot be told about is
  // late by at most the tick (an out-of-contract configuration; delivery is
  // otherwise carried by the slotted threads)
  if (!slot)
    return tickMs < fallbackMs ? tickMs : fallbackMs;

  // The worst case this call may commit to goes out first: the scan below
  // must run with a published wakeTick no earlier than the final decision,
  // or a producer racing the scan would see an awake thread and skip the
  // kick while the scan misses its fresh bit. Every later store only
  // shrinks the horizon - a producer reading a stale larger value kicks
  // spuriously at worst
  __uintptr_atomic_store(&slot->wakeTick, timerSleepEternal, amoSeqCst);

  // Scan from the confirmed sweep position when it lags the clock (a
  // concurrent sweeper mid-tick): due backlog then wakes immediately instead
  // of hiding behind a full window
  uint64_t cursor = (uint64_t)__uintptr_atomic_load(&base->timerCloseCursor, amoAcquire);
  uint64_t from = cursor < currentTick ? cursor : currentTick;

  // A visitor between its pre-clear and its drain CAS leaves a live chain
  // bitless anywhere in the wheel (the bit has no incarnation), invisible to
  // the scan. Trust the scan only when no bracket was open at either
  // snapshot and none turned over in between: reading a cleared bit orders
  // the bracket open before the closing snapshot, a close mid-scan changes
  // the sum - one re-scan then sees the repaired bit. Both parity checks are
  // load-bearing: an overflow exit is the one negative sum term and can
  // cancel a slot close, but it presupposes a bracket already open at the
  // opening snapshot. Otherwise cap at one tick behind the scan origin: due
  // backlog drains immediately, the pair-driven sweep delivers without the
  // bitmap
  uint64_t wake;
  TimerPreclearSnapshot before = timerPreclearSnapshot(base);
  for (unsigned attempt = 0;;) {
    wake = timerWheelNearestWake(base, from);
    TimerPreclearSnapshot after = timerPreclearSnapshot(base);
    if (!before.active && !after.active && after.sum == before.sum)
      break;
    if (before.active || after.active || ++attempt == 2) {
      if (wake > from + 1)
        wake = from + 1;
      break;
    }
    before = after;
  }

  if (wake == UINT64_MAX) {
    // Nothing parked anywhere: wait with no timeout. The eternal sentinel
    // stays published - a later arm kicks through methodImpl.wakeupLoop,
    // while queue traffic carries its own doorbell.
    return UINT32_MAX;
  }

  // Far wakes are clamped to the kernel timeout range (epoll takes int
  // milliseconds); waking early is harmless, the loop re-scans and sleeps on
  uint64_t sleepTicks = wake > currentTick ? wake - currentTick : 0;
  const uint64_t sleepTicksLimit = 0x7FFFFFFFu / tickMs;
  if (sleepTicks > sleepTicksLimit) {
    sleepTicks = sleepTicksLimit;
    wake = currentTick + sleepTicks;
  }
  __uintptr_atomic_store(&slot->wakeTick, (uintptr_t)wake, amoRelease);
  return (uint32_t)(sleepTicks * tickMs);
}

void timerLoopCancelSleep(asyncBase *base, unsigned threadId)
{
  // Awake: the thread sweeps on its own before its next sleep, kicking it
  // would be a wasted syscall. A producer racing this store at worst reads
  // the stale horizon and issues one spurious wakeup
  if (base->timerSleep && threadId < base->loopThreadLimit)
    __uintptr_atomic_store(&base->timerSleep[threadId].wakeTick, UINTPTR_MAX, amoRelaxed);
}
