// Timer-wheel stress: every concurrent property of the wheel in one binary.
//
// Phase A (ownership): T threads publish links into the same level-0 windows,
// then race timerWheelDetach over every window. Exactly one caller may get a
// non-empty chain per window incarnation and every published link must be
// claimed by exactly one winner - the DWCAS drain+reopen makes the visit
// idempotent, so losers get nothing instead of a second copy.
//
// Phase B (cursor helping): T threads race timerWheelSweepTick and the
// production processTimeoutQueue over a block of ticks, with a simulated
// stall - a thread
// sometimes detaches a window and "hangs" before the confirm CAS, leaving the
// confirm to helpers. The cursor must land exactly on the block end and never
// rewind. Links are armed through the production addToTimeoutQueue path and
// made stale (generation bump) before the sweep, so every visit recycles them
// through the real pool without delivering into mock-free operations.
//
// Phase C (late insertion): the sweeper drains the deadline window (the real
// drain+reopen visit) and only then the producer publishes a link for the
// already swept tick. The terminal window protocol must reject every late
// insertion - the producer observes a foreign baseTick and reports expired -
// so nothing is accepted and nothing is parked in the reopened incarnations.
//
// Phases A and B also verify the occupancy-bitmap invariant at their
// quiescence points and after the teardown: a slot holding a chain must have
// its bit set (a bitless chain would let a sleeping loop oversleep the
// deadline; extra bits over empty slots are legal spurious wakeups).
//
// Usage: timerwheelstress [threads] [rounds] [itemsPerThread]

#include "asyncioImpl.h"
#include "atomic.h"

#include <algorithm>
#include <atomic>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

namespace {

class ReusableBarrier {
public:
  explicit ReusableBarrier(unsigned participants) : participants_(participants) {}

  void wait()
  {
    unsigned generation = generation_.load(std::memory_order_acquire);
    if (arrived_.fetch_add(1, std::memory_order_acq_rel) + 1 == participants_) {
      arrived_.store(0, std::memory_order_relaxed);
      generation_.fetch_add(1, std::memory_order_release);
      return;
    }

    while (generation_.load(std::memory_order_acquire) == generation)
      std::this_thread::yield();
  }

private:
  const unsigned participants_;
  std::atomic<unsigned> arrived_{0};
  std::atomic<unsigned> generation_{0};
};

// op first: a detached link leads back to its TimerSlot through link->op
struct TimerSlot {
  asyncOpRoot op{};
  asyncOpListLink link{};
  std::atomic<unsigned> claims{0};
};

struct Lcg {
  uint64_t state;
  explicit Lcg(uint64_t seed) : state(seed * 2654435761u + 1) {}
  uint64_t next()
  {
    state = state * 6364136223846793005ULL + 1442695040888963407ULL;
    return state >> 33;
  }
};

uint64_t parseArgument(const char *value, const char *name)
{
  char *end = nullptr;
  uint64_t parsed = strtoull(value, &end, 10);
  if (!value[0] || !end || *end || parsed == 0) {
    fprintf(stderr, "invalid %s: %s\n", name, value);
    exit(2);
  }
  return parsed;
}

// Occupancy invariant, checked at quiescence points (no visit or publication
// in flight): a slot holding a chain must have its bit set - a bitless chain
// is a lost wakeup, a sleeping loop would oversleep its deadlines. Spurious
// set bits over empty slots are legal (they only cost a wakeup) and are not
// counted.
uint64_t occupancyViolations(asyncBase *base)
{
  uint64_t violations = 0;
  for (unsigned level = 0; level < TIMER_WHEEL_LEVELS; ++level) {
    for (unsigned index = 0; index < TIMER_WHEEL_SLOTS; ++index) {
      uint128 pair = __uint128_atomic_load(&base->timerWheel.slots[level][index]);
      if (pair.low &&
          !(base->timerWheel.occupancy[level][index >> 6] & (static_cast<uintptr_t>(1) << (index & 63))))
        ++violations;
    }
  }
  return violations;
}

// The teardown contract: no bits survive it
uint64_t occupancyResidue(asyncBase *base)
{
  uint64_t residue = 0;
  for (unsigned level = 0; level < TIMER_WHEEL_LEVELS; ++level) {
    for (unsigned word = 0; word < TIMER_WHEEL_SLOTS / 64; ++word)
      residue += base->timerWheel.occupancy[level][word] != 0;
  }
  return residue;
}

} // namespace

int main(int argc, char **argv)
{
  unsigned threads = 4;
  uint64_t rounds = 128;
  uint64_t itemsPerThread = 256;

  if (argc > 1) {
    uint64_t parsedThreads = parseArgument(argv[1], "threads");
    if (parsedThreads >= (std::numeric_limits<unsigned>::max)()) {
      fprintf(stderr, "threads exceeds the supported thread count\n");
      return 2;
    }
    threads = static_cast<unsigned>(parsedThreads);
  }
  if (argc > 2)
    rounds = parseArgument(argv[2], "rounds");
  if (argc > 3)
    itemsPerThread = parseArgument(argv[3], "itemsPerThread");
  if (argc > 4) {
    fprintf(stderr, "usage: timerwheelstress [threads] [rounds] [itemsPerThread]\n");
    return 2;
  }

  // Phases A and C walk one window per distinct tick; a round must not reuse
  // a physical slot with two different window starts out of cursor order
  uint64_t windowsPerRound = itemsPerThread < TIMER_WHEEL_SLOTS ? itemsPerThread : TIMER_WHEEL_SLOTS;
  if (itemsPerThread > (std::numeric_limits<uint64_t>::max)() / threads) {
    fprintf(stderr, "configuration exceeds the stress counter range\n");
    return 2;
  }
  uint64_t itemsPerRound = static_cast<uint64_t>(threads) * itemsPerThread;
  if (itemsPerRound > (std::numeric_limits<size_t>::max)() / sizeof(TimerSlot) ||
      rounds > (std::numeric_limits<uint64_t>::max)() / itemsPerRound - 1 ||
      rounds > ((std::numeric_limits<uint64_t>::max)() - 1) / (windowsPerRound + 2048)) {
    fprintf(stderr, "configuration exceeds the tick counter range\n");
    return 2;
  }

  printf("timerwheelstress: %u thread(s), %" PRIu64 " round(s), %" PRIu64
         " item(s)/thread/round\n",
         threads, rounds, itemsPerThread);

  ReusableBarrier barrier(threads + 1);
  std::atomic<uint64_t> corrupt{0};

  // ---- Phase A: detach ownership ----
  // Only the wheel part of the base is exercised; no loop threads exist
  std::unique_ptr<asyncBase> ownershipBase(new asyncBase{});
  timerWheelInit(ownershipBase.get(), 0);
  size_t slotCount = static_cast<size_t>(threads) * windowsPerRound;
  std::unique_ptr<TimerSlot[]> slots(new TimerSlot[slotCount]);
  std::unique_ptr<std::atomic<unsigned>[]> nonEmptyDetaches(
    new std::atomic<unsigned>[static_cast<size_t>(windowsPerRound)]{});

  // Ticks start at 0 and tile the tick line with no gap: every window of
  // every touched slot is visited exactly once and in order, which keeps the
  // slot incarnations in the same lockstep a real contiguous sweep maintains
  // (a skipped window would make the strict producer refuse its first reuse)
  auto tickFor = [windowsPerRound](uint64_t round, uint64_t window) {
    return round * windowsPerRound + window;
  };

  std::vector<std::thread> workers;
  workers.reserve(threads);
  for (unsigned threadIdx = 0; threadIdx < threads; ++threadIdx) {
    workers.emplace_back([&, threadIdx] {
      TimerSlot *mySlots = &slots[static_cast<size_t>(threadIdx) * windowsPerRound];
      for (uint64_t round = 0; round < rounds; ++round) {
        uint64_t roundBase = tickFor(round, 0);
        barrier.wait();

        // Publish: every thread contends on every window's slot
        for (uint64_t window = 0; window < windowsPerRound; ++window) {
          TimerSlot &slot = mySlots[static_cast<size_t>(window)];
          slot.link.op = &slot.op;
          slot.link.generation = 1;
          slot.link.next = nullptr;
          slot.link.deadlineTick = roundBase + window;
          if (!timerWheelInsert(ownershipBase.get(), &slot.link, slot.link.deadlineTick))
            corrupt.fetch_add(1, std::memory_order_relaxed);
        }

        barrier.wait();
        // Main checks the occupancy invariant over the fully published wheel
        barrier.wait();

        // Race the visits, starting at staggered offsets so the interleaving
        // is not lockstep. The winner owns the whole chain and claims each
        // link exactly once; everyone else must observe an already-reopened
        // window
        uint64_t offset = windowsPerRound * threadIdx / threads;
        for (uint64_t step = 0; step < windowsPerRound; ++step) {
          uint64_t window = (offset + step) % windowsPerRound;
          asyncOpListLink *chain =
            timerWheelDetach(ownershipBase.get(), nullptr, 0,
                             roundBase + window);
          if (!chain)
            continue;
          nonEmptyDetaches[static_cast<size_t>(window)].fetch_add(1, std::memory_order_relaxed);
          for (asyncOpListLink *link = chain; link; link = link->next) {
            uint64_t linkWindow = link->deadlineTick - roundBase;
            TimerSlot *owner = reinterpret_cast<TimerSlot*>(link->op);
            if (linkWindow != window || link != &owner->link ||
                owner->claims.fetch_add(1, std::memory_order_relaxed) != 0)
              corrupt.fetch_add(1, std::memory_order_relaxed);
          }
        }

        barrier.wait();
        // Main verifies and resets the claim counters before the next round
        barrier.wait();
      }
    });
  }

  uint64_t lostLinks = 0;
  uint64_t doubleDetaches = 0;
  uint64_t bitlessChains = 0;
  for (uint64_t round = 0; round < rounds; ++round) {
    barrier.wait();
    barrier.wait();
    bitlessChains += occupancyViolations(ownershipBase.get());
    barrier.wait();
    barrier.wait();

    for (size_t i = 0; i < slotCount; ++i) {
      if (slots[i].claims.exchange(0, std::memory_order_relaxed) != 1)
        lostLinks++;
    }
    for (uint64_t window = 0; window < windowsPerRound; ++window) {
      if (nonEmptyDetaches[static_cast<size_t>(window)].exchange(0, std::memory_order_relaxed) != 1)
        doubleDetaches++;
    }

    barrier.wait();
  }

  for (std::thread &worker : workers)
    worker.join();
  workers.clear();

  // Every window was detached, so the wheel holds no stress-owned memory and
  // the teardown must not leak it into the global link pool
  timerWheelTeardown(ownershipBase.get());
  bitlessChains += occupancyResidue(ownershipBase.get());

  printf("phase A (ownership): %" PRIu64 " link(s), lost/multiply-claimed: %" PRIu64
         ", non-single detaches: %" PRIu64 ", bitless chains: %" PRIu64 ", corrupt: %" PRIu64 "\n",
         rounds * slotCount, lostLinks, doubleDetaches, bitlessChains, corrupt.load());
  if (lostLinks || doubleDetaches || bitlessChains || corrupt.load()) {
    fprintf(stderr, "FAILED: a window chain was lost, stolen, claimed twice or left bitless\n");
    return 1;
  }

  // ---- Phase B: concurrent sweepTick, cursor helping ----
  const uint64_t blockTicks = 512;
  std::unique_ptr<asyncBase> sweepBase(new asyncBase{});
  timerWheelInit(sweepBase.get(), 0);
  size_t opCount = static_cast<size_t>(threads) * itemsPerThread;
  std::unique_ptr<asyncOpRoot[]> ops(new asyncOpRoot[opCount]{});
  // addToTimeoutQueue now captures the operation owner's generation for a
  // later validated cancel. These stress operations deliberately become stale
  // before delivery, but they still need a valid owner while they are armed.
  // One immutable shell is sufficient: opSetStatus loses on every detached
  // link, so no combiner signal is ever pushed to it.
  aioObjectRoot sweepOwner{};
  __uint64_atomic_store(&sweepOwner.header.tag.high, 1, amoRelaxed);
  for (size_t i = 0; i < opCount; ++i)
    ops[i].object = &sweepOwner;
  std::atomic<uint64_t> cursorViolations{0};

  for (unsigned threadIdx = 0; threadIdx < threads; ++threadIdx) {
    workers.emplace_back([&, threadIdx] {
      asyncOpRoot *myOps = &ops[static_cast<size_t>(threadIdx) * itemsPerThread];
      Lcg random(threadIdx + 100);
      for (uint64_t round = 0; round < rounds; ++round) {
        uint64_t blockStart = round * blockTicks;
        uint64_t blockEnd = blockStart + blockTicks;
        barrier.wait();

        // Arm through the production path: level-0 deadlines inside the block
        // and level-1 deadlines beyond it (their windows are swept by later
        // rounds or recycled by the teardown). Then bump the generations so
        // every visit takes the stale path - the operations are shells, a
        // delivery would walk into mock-free cancel machinery
        for (uint64_t item = 0; item < itemsPerThread; ++item) {
          asyncOpRoot &op = myOps[static_cast<size_t>(item)];
          uint64_t deadline = blockStart + 1 + random.next() % (blockTicks + 1024);
          op.deadlineTick = deadline;
          addToTimeoutQueue(sweepBase.get(), &op);
        }
        for (uint64_t item = 0; item < itemsPerThread; ++item)
          myOps[static_cast<size_t>(item)].tag += static_cast<uintptr_t>(1) << TAG_STATUS_SIZE;

        barrier.wait();
        // Main checks the occupancy invariant over the armed wheel
        barrier.wait();

        // Race the sweep with no lock and no election. The simulated stall
        // detaches a window and leaves the tick unconfirmed: helpers re-visit
        // it idempotently and confirm; the stalled chain stays private and is
        // processed by its owner afterwards
        uint64_t tick;
        while ((tick = static_cast<uint64_t>(__uintptr_atomic_load(
                  &sweepBase->timerCloseCursor, amoAcquire))) < blockEnd) {
          if (random.next() % 8 == 0) {
            asyncOpListLink *chain =
              timerWheelDetach(sweepBase.get(), nullptr, 0, tick);
            for (asyncOpListLink *link = chain; link; link = link->next) {
              if (link->deadlineTick != tick)
                corrupt.fetch_add(1, std::memory_order_relaxed);
            }
            std::this_thread::yield();
            timerWheelProcessDetached(sweepBase.get(), chain, tick);
            continue;
          }
          if (random.next() % 4 == 0) {
            // The production entry point is the same helper loop; a partial
            // horizon keeps it interleaved with the manual sweeps and stalls
            // instead of eating the whole block in one call
            processTimeoutQueue(sweepBase.get(), nullptr,
                                std::min<uint64_t>(blockEnd, tick + 1 + random.next() % 8));
            continue;
          }
          timerWheelSweepTick(sweepBase.get(), nullptr, tick);
        }
        // Atomic re-read: a straggler may still be confirming the last tick
        if (static_cast<uint64_t>(__uintptr_atomic_load(&sweepBase->timerCloseCursor, amoAcquire)) > blockEnd)
          cursorViolations.fetch_add(1, std::memory_order_relaxed);

        barrier.wait();
        barrier.wait();
      }
    });
  }

  uint64_t tagViolations = 0;
  uint64_t bitlessArmed = 0;
  for (uint64_t round = 0; round < rounds; ++round) {
    barrier.wait();
    barrier.wait();
    bitlessArmed += occupancyViolations(sweepBase.get());
    barrier.wait();
    barrier.wait();

    if (static_cast<uint64_t>(sweepBase->timerCloseCursor) != (round + 1) * blockTicks)
      cursorViolations++;
    uintptr_t expectedTag = static_cast<uintptr_t>(round + 1) << TAG_STATUS_SIZE;
    for (size_t i = 0; i < opCount; ++i) {
      if (ops[i].tag != expectedTag)
        tagViolations++;
    }

    barrier.wait();
  }

  for (std::thread &worker : workers)
    worker.join();
  workers.clear();

  // Stale links whose windows lie beyond the last block are still parked in
  // the wheel; the official teardown recycles them into the pool
  timerWheelTeardown(sweepBase.get());
  bitlessArmed += occupancyResidue(sweepBase.get());

  printf("phase B (cursor): %" PRIu64 " tick(s), cursor violations: %" PRIu64
         ", touched operations: %" PRIu64 ", bitless chains: %" PRIu64 ", corrupt: %" PRIu64 "\n",
         rounds * blockTicks, cursorViolations.load(), tagViolations, bitlessArmed, corrupt.load());
  if (cursorViolations.load() || tagViolations || bitlessArmed || corrupt.load()) {
    fprintf(stderr, "FAILED: the sweep protocol broke without the lock\n");
    return 1;
  }

  // ---- Phase C: late insertion is refused by the terminal window ----
  std::unique_ptr<asyncBase> lateBase(new asyncBase{});
  timerWheelInit(lateBase.get(), 0);
  std::unique_ptr<TimerSlot[]> lateSlots(new TimerSlot[static_cast<size_t>(itemsPerRound)]);
  std::atomic<uint64_t> lateAccepted{0};
  uint64_t unexpectedlyPresentBeforeAdd = 0;
  uint64_t stranded = 0;

  auto lateTickFor = [itemsPerRound](uint64_t round, uint64_t index) {
    return round * itemsPerRound + index;
  };

  for (unsigned threadIdx = 0; threadIdx < threads; ++threadIdx) {
    workers.emplace_back([&, threadIdx] {
      uint64_t first = static_cast<uint64_t>(threadIdx) * itemsPerThread;
      uint64_t last = first + itemsPerThread;
      for (uint64_t round = 0; round < rounds; ++round) {
        for (uint64_t index = first; index < last; ++index) {
          TimerSlot &slot = lateSlots[static_cast<size_t>(index)];
          uint64_t tick = lateTickFor(round, index);
          slot.link.op = &slot.op;
          slot.link.generation = 1;
          slot.link.next = nullptr;
          slot.link.deadlineTick = tick;
        }

        // All links are producer-owned but deliberately not published yet.
        barrier.wait();
        // The sweeper visits every target window before this opens.
        barrier.wait();

        // Every publication must observe the reopened incarnation and report
        // expired; the link then stays producer-owned and is reused next round
        for (uint64_t index = first; index < last; ++index) {
          TimerSlot &slot = lateSlots[static_cast<size_t>(index)];
          if (timerWheelInsert(lateBase.get(), &slot.link, slot.link.deadlineTick))
            lateAccepted.fetch_add(1, std::memory_order_relaxed);
        }

        barrier.wait();
        // The verifier inspects the slots before the links are reused.
        barrier.wait();
      }
    });
  }

  // Consecutive ticks of one round share physical level-0 slots modulo the
  // wheel size; the drains walk the windows in tick order, and the post-add
  // check peeks each distinct physical slot once.
  uint64_t distinctSlots = itemsPerRound < TIMER_WHEEL_SLOTS ? itemsPerRound : TIMER_WHEEL_SLOTS;

  for (uint64_t round = 0; round < rounds; ++round) {
    uint64_t roundBase = lateTickFor(round, 0);
    barrier.wait();

    // This is the real sweep: drain and reopen every window of the round in
    // cursor order. In production the checkpoint is published after these
    // visits, before the delayed producers resume.
    for (uint64_t s = 0; s < itemsPerRound; ++s) {
      for (asyncOpListLink *link = timerWheelDetach(
             lateBase.get(), nullptr, 0, roundBase + s); link;
           link = link->next)
        unexpectedlyPresentBeforeAdd++;
    }

    barrier.wait();
    barrier.wait();

    // Nothing may be parked in the reopened incarnations
    for (uint64_t s = 0; s < distinctSlots; ++s) {
      uint128 observed = __uint128_atomic_load(
        &lateBase->timerWheel.slots[0][(roundBase + s) % TIMER_WHEEL_SLOTS]);
      if (observed.low != 0)
        stranded++;
    }

    barrier.wait();
  }

  for (std::thread &worker : workers)
    worker.join();

  printf("phase C (late insertion): %" PRIu64 " late attempt(s), accepted: %" PRIu64
         ", present before add: %" PRIu64 ", parked after sweep: %" PRIu64 "\n",
         rounds * itemsPerRound, lateAccepted.load(), unexpectedlyPresentBeforeAdd, stranded);
  if (lateAccepted.load() || unexpectedlyPresentBeforeAdd || stranded) {
    fprintf(stderr,
            "FAILED: timeout wheel accepted %" PRIu64
            " timer(s) after their deadline windows were swept\n",
            lateAccepted.load());
    return 1;
  }

  printf("OK\n");
  return 0;
}
