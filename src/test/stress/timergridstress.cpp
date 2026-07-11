// Timeout-wheel late-insertion stress.
//
// The wheel drains and reopens a slot with one DWCAS, so a producer that
// resumes after the sweep publishes into the reopened incarnation: the link
// is not lost, but it fires a full rotation late instead of expiring
// immediately. This stress repeats that legal ordering over unique absolute
// ticks:
//
//   sweeper:  drain the deadline slot (conceptually publish the checkpoint)
//   producer: timerWheelInsert(link for the already swept tick)
//   verifier: drain the slot again to diagnose/recover the late link
//
// Producers run concurrently so slot publication is contended, while a
// reusable barrier makes the logical race deterministic. A correct terminal
// window protocol rejects every late insertion (producer observes a foreign
// baseTick and expires the link), so the verifier finds zero links. The
// current publish-into-observed-incarnation stage is expected to fail; this
// is the red stage of the timer-wheel TDD change.
//
// Usage: timergridstress [producers] [rounds] [itemsPerProducer]

#include "asyncioImpl.h"

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

struct TimerSlot {
  asyncOpRoot op{};
  asyncOpListLink link{};
};

// Detach a level-0 slot's list without delivering or reopening it: the
// verifier owns recovered links, and baseTick is left alone so producers keep
// attaching to a stable incarnation across rounds.
asyncOpListLink *drainLevel0Slot(asyncBase *base, uint64_t tick)
{
  volatile uint128Pair *slot = &base->timerWheel.slots[0][tick % TIMER_WHEEL_SLOTS];
  uint128Pair observed = __uint128_atomic_load(slot);
  uint128Pair desired;
  do {
    if (observed.low == 0)
      return nullptr;
    desired.low = 0;
    desired.high = observed.high;
  } while (!__uint128_atomic_compare_and_swap(slot, &observed, desired));
  return reinterpret_cast<asyncOpListLink*>(static_cast<uintptr_t>(observed.low));
}

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

} // namespace

int main(int argc, char **argv)
{
  unsigned producers = 4;
  uint64_t rounds = 64;
  uint64_t itemsPerProducer = 1024;

  if (argc > 1) {
    uint64_t parsedProducers = parseArgument(argv[1], "producers");
    if (parsedProducers >= (std::numeric_limits<unsigned>::max)()) {
      fprintf(stderr, "producers exceeds the supported thread count\n");
      return 2;
    }
    producers = static_cast<unsigned>(parsedProducers);
  }
  if (argc > 2)
    rounds = parseArgument(argv[2], "rounds");
  if (argc > 3)
    itemsPerProducer = parseArgument(argv[3], "itemsPerProducer");
  if (argc > 4) {
    fprintf(stderr, "usage: timergridstress [producers] [rounds] [itemsPerProducer]\n");
    return 2;
  }

  if (producers == 0 ||
      itemsPerProducer > (std::numeric_limits<uint64_t>::max)() / producers) {
    fprintf(stderr, "configuration exceeds the stress counter range\n");
    return 2;
  }
  uint64_t itemsPerRound = static_cast<uint64_t>(producers) * itemsPerProducer;
  if (itemsPerRound > (std::numeric_limits<size_t>::max)() / sizeof(TimerSlot) ||
      rounds > (std::numeric_limits<uint64_t>::max)() / itemsPerRound - 1) {
    fprintf(stderr, "configuration exceeds the tick counter range\n");
    return 2;
  }

  printf("timergridstress: %u producer(s), %" PRIu64
         " round(s), %" PRIu64 " timer(s)/producer/round\n",
         producers, rounds, itemsPerProducer);

  // Only the wheel part of the base is exercised; no loop threads exist
  std::unique_ptr<asyncBase> base(new asyncBase{});
  timerWheelInit(base.get(), 0);
  std::unique_ptr<TimerSlot[]> slots(new TimerSlot[static_cast<size_t>(itemsPerRound)]);
  ReusableBarrier barrier(producers + 1);
  uint64_t unexpectedlyPresentBeforeAdd = 0;
  uint64_t stranded = 0;
  uint64_t corrupt = 0;

  auto tickFor = [itemsPerRound](uint64_t round, uint64_t index) {
    return 1 + round * itemsPerRound + index;
  };

  std::vector<std::thread> threads;
  threads.reserve(producers);
  for (unsigned producer = 0; producer < producers; ++producer) {
    threads.emplace_back([&, producer] {
      uint64_t first = static_cast<uint64_t>(producer) * itemsPerProducer;
      uint64_t last = first + itemsPerProducer;
      for (uint64_t round = 0; round < rounds; ++round) {
        for (uint64_t index = first; index < last; ++index) {
          TimerSlot &slot = slots[static_cast<size_t>(index)];
          uint64_t tick = tickFor(round, index);
          slot.link.op = &slot.op;
          slot.link.generation = 1;
          slot.link.next = nullptr;
          slot.link.deadlineTick = tick;
        }

        // All links are producer-owned but deliberately not published yet.
        barrier.wait();
        // The sweeper drains every target slot before this opens.
        barrier.wait();

        // The post-sweep cursor routes every deadline to its level-0 slot.
        for (uint64_t index = first; index < last; ++index) {
          TimerSlot &slot = slots[static_cast<size_t>(index)];
          timerWheelInsert(base.get(), &slot.link, slot.link.deadlineTick);
        }

        barrier.wait();
        // Do not reuse slot storage until the verifier has detached any link
        // accepted by the publish-into-observed implementation.
        barrier.wait();
      }
    });
  }

  // Consecutive ticks of one round share physical level-0 slots modulo the
  // wheel size, so both drains walk each distinct slot once and inspect the
  // whole detached chain.
  uint64_t distinctSlots = itemsPerRound < TIMER_WHEEL_SLOTS ? itemsPerRound : TIMER_WHEEL_SLOTS;

  for (uint64_t round = 0; round < rounds; ++round) {
    uint64_t roundBase = tickFor(round, 0);
    barrier.wait();

    // This is the real sweep. In production the checkpoint is published after
    // these drains, before the delayed producers resume.
    for (uint64_t s = 0; s < distinctSlots; ++s) {
      for (asyncOpListLink *link = drainLevel0Slot(base.get(), roundBase + s); link;
           link = link->next)
        unexpectedlyPresentBeforeAdd++;
    }

    barrier.wait();
    barrier.wait();

    // Diagnostic second drain: any returned link was accepted after the only
    // production sweep and would otherwise fire a rotation late (or, with a
    // terminal-window protocol, must have been expired by the producer).
    for (uint64_t s = 0; s < distinctSlots; ++s) {
      for (asyncOpListLink *link = drainLevel0Slot(base.get(), roundBase + s); link;
           link = link->next) {
        stranded++;
        uint64_t offset = link->deadlineTick - roundBase;
        if (offset >= itemsPerRound ||
            link != &slots[static_cast<size_t>(offset)].link ||
            link->deadlineTick % TIMER_WHEEL_SLOTS != (roundBase + s) % TIMER_WHEEL_SLOTS)
          corrupt++;
      }
    }

    barrier.wait();
  }

  for (std::thread &thread : threads)
    thread.join();

  uint64_t total = rounds * itemsPerRound;
  printf("late attempts: %" PRIu64
         ", present before add: %" PRIu64
         ", stranded after sweep: %" PRIu64
         ", corrupt: %" PRIu64 "\n",
         total, unexpectedlyPresentBeforeAdd, stranded, corrupt);

  if (unexpectedlyPresentBeforeAdd || corrupt || stranded) {
    fprintf(stderr,
            "FAILED: timeout wheel accepted %" PRIu64
            " timer(s) after their deadline windows were swept\n",
            stranded);
    return 1;
  }

  printf("OK\n");
  return 0;
}
