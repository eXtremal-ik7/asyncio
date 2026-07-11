// Timeout-grid late-insertion stress.
//
// The timeout grid currently sweeps a bucket with exchange(head, NULL). A
// producer that resumes after that sweep can publish into the reopened NULL
// head even though the global checkpoint has already passed the bucket. The
// timer is then stranded until the 32-bit second key wraps.
//
// This stress repeats that legal ordering over unique absolute buckets:
//
//   sweeper:  extract(empty bucket), conceptually publish checkpoint
//   producer: pageMapAdd(link for the already swept bucket)
//   verifier: inspect the bucket only to diagnose/recover a stranded link
//
// Producers run concurrently so page publication and bucket insertion are also
// contended, while a reusable barrier makes the logical race deterministic.
// A correct terminal-bucket implementation rejects every late insertion, so
// the verifier finds zero links. The current OPEN -> NULL implementation is
// expected to fail; this is the red stage of the timer-grid TDD change.
//
// Usage: timergridstress [producers] [rounds] [itemsPerProducer]

#include "asyncio/api.h"

#include <atomic>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <limits>
#include <memory>
#include <thread>
#include <vector>

extern "C" {
void pageMapInit(pageMap *map);
asyncOpListLink *pageMapExtractAll(pageMap *map, uint64_t tm);
void pageMapAdd(pageMap *map, asyncOpListLink *link);
}

namespace {

constexpr size_t kPageMapSize = size_t{1} << 16;

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

void destroyPageMap(pageMap *map)
{
  if (!map->map)
    return;
  for (size_t i = 0; i < kPageMapSize; ++i)
    free(map->map[i]);
  free(map->map);
  map->map = nullptr;
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
    if (parsedProducers >= std::numeric_limits<unsigned>::max()) {
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
      itemsPerProducer > std::numeric_limits<uint64_t>::max() / producers) {
    fprintf(stderr, "configuration exceeds the stress counter range\n");
    return 2;
  }
  uint64_t itemsPerRound = static_cast<uint64_t>(producers) * itemsPerProducer;
  if (itemsPerRound > std::numeric_limits<size_t>::max() / sizeof(TimerSlot) ||
      rounds > (UINT32_MAX - 1ULL) / itemsPerRound) {
    fprintf(stderr, "configuration exceeds the timeout grid's 32-bit key range\n");
    return 2;
  }

  printf("timergridstress: %u producer(s), %" PRIu64
         " round(s), %" PRIu64 " timer(s)/producer/round\n",
         producers, rounds, itemsPerProducer);

  pageMap map{};
  pageMapInit(&map);
  std::unique_ptr<TimerSlot[]> slots(new TimerSlot[static_cast<size_t>(itemsPerRound)]);
  ReusableBarrier barrier(producers + 1);
  uint64_t unexpectedlyPresentBeforeAdd = 0;
  uint64_t stranded = 0;
  uint64_t corrupt = 0;

  auto bucketFor = [itemsPerRound](uint64_t round, uint64_t index) {
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
          uint64_t bucket = bucketFor(round, index);
          slot.op.endTime = bucket * 1000000ULL;
          slot.link.op = &slot.op;
          slot.link.tag = 1;
          slot.link.next = nullptr;
        }

        // All links are producer-owned but deliberately not published yet.
        barrier.wait();
        // The sweeper closes/extracts every target bucket before this opens.
        barrier.wait();

        for (uint64_t index = first; index < last; ++index)
          pageMapAdd(&map, &slots[static_cast<size_t>(index)].link);

        barrier.wait();
        // Do not reuse slot storage until the verifier has detached any link
        // incorrectly accepted by the old implementation.
        barrier.wait();
      }
    });
  }

  for (uint64_t round = 0; round < rounds; ++round) {
    barrier.wait();

    // This is the real sweep. In production the checkpoint is published after
    // these extracts, before the delayed producers resume.
    for (uint64_t index = 0; index < itemsPerRound; ++index) {
      uint64_t bucket = bucketFor(round, index);
      if (pageMapExtractAll(&map, bucket))
        unexpectedlyPresentBeforeAdd++;
    }

    barrier.wait();
    barrier.wait();

    // Diagnostic second extract: any returned link was accepted after the only
    // production sweep and would otherwise remain stranded behind checkpoint.
    for (uint64_t index = 0; index < itemsPerRound; ++index) {
      asyncOpListLink *head = pageMapExtractAll(&map, bucketFor(round, index));
      if (!head)
        continue;
      stranded++;
      TimerSlot &expected = slots[static_cast<size_t>(index)];
      if (head != &expected.link || head->next)
        corrupt++;
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

  destroyPageMap(&map);

  if (unexpectedlyPresentBeforeAdd || corrupt || stranded) {
    fprintf(stderr,
            "FAILED: timeout grid accepted %" PRIu64
            " timer(s) after their deadline buckets were swept\n",
            stranded);
    return 1;
  }

  printf("OK\n");
  return 0;
}
