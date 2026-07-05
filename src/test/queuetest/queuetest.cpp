// MPMC stress/consistency test for ConcurrentQueue (asyncio/ringBuffer.c)
//
// Producers push unique values, consumers pop them concurrently; the test fails
// if any value is lost, duplicated or was never pushed (garbage). This is the
// scenario where a missing release/acquire pair on ConcurrentQueueElement::sequence
// corrupts data on weakly-ordered CPUs (ARM64), so the test is also intended to be
// run under ThreadSanitizer (configure with -DBUILD_SANITIZE_THREAD=ON).
//
// Usage: queuetest [producers] [consumers] [itemsPerProducer] [maxInflight]
//   maxInflight < 4096 keeps the test inside the first partition;
//   larger values also exercise partition growth.

extern "C" {
#include "asyncio/ringBuffer.h"
}

#include <atomic>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

static ConcurrentQueue queue;
static std::atomic<long> inflight(0);
static std::atomic<unsigned> doneProducers(0);
static std::atomic<uintptr_t> errGarbage(0);
static std::atomic<uintptr_t> errDuplicate(0);
static std::vector<std::atomic<unsigned>> seen;

static unsigned producers = 4;
static unsigned consumers = 4;
static uintptr_t itemsPerProducer = 1u << 22;
static long maxInflight = 3000;

static void producerProc(unsigned id)
{
  uintptr_t begin = (uintptr_t)id * itemsPerProducer;
  for (uintptr_t i = 0; i < itemsPerProducer; i++) {
    while (inflight.load(std::memory_order_relaxed) > maxInflight)
      std::this_thread::yield();
    inflight.fetch_add(1, std::memory_order_relaxed);
    concurrentQueuePush(&queue, (void*)(begin + i + 1));
  }

  doneProducers.fetch_add(1);
}

static void consumerProc()
{
  unsigned idleRounds = 0;
  for (;;) {
    void *data;
    if (concurrentQueuePop(&queue, &data)) {
      idleRounds = 0;
      inflight.fetch_sub(1, std::memory_order_relaxed);
      uintptr_t value = (uintptr_t)data;
      if (value == 0 || value > seen.size()) {
        errGarbage.fetch_add(1);
        fprintf(stderr, "error: garbage value popped: %p\n", data);
        continue;
      }

      if (seen[value - 1].fetch_add(1) != 0) {
        errDuplicate.fetch_add(1);
        fprintf(stderr, "error: value %" PRIuPTR " popped twice\n", value);
      }
    } else {
      // Queue looks empty; leave after all producers finished and it stays empty
      if (doneProducers.load() == producers && ++idleRounds > 1000)
        return;
      std::this_thread::yield();
    }
  }
}

int main(int argc, char **argv)
{
  if (argc > 1)
    producers = (unsigned)atoi(argv[1]);
  if (argc > 2)
    consumers = (unsigned)atoi(argv[2]);
  if (argc > 3)
    itemsPerProducer = (uintptr_t)strtoull(argv[3], 0, 10);
  if (argc > 4)
    maxInflight = atol(argv[4]);
  if (!producers || !consumers || !itemsPerProducer || maxInflight <= 0) {
    fprintf(stderr, "usage: queuetest [producers] [consumers] [itemsPerProducer] [maxInflight]\n");
    return 1;
  }

  seen = std::vector<std::atomic<unsigned>>(producers * itemsPerProducer);
  for (auto &counter: seen)
    counter.store(0, std::memory_order_relaxed);

  printf("queuetest: %u producer(s), %u consumer(s), %" PRIuPTR " items, max inflight %ld\n",
         producers, consumers, (uintptr_t)seen.size(), maxInflight);

  std::vector<std::thread> threads;
  for (unsigned i = 0; i < consumers; i++)
    threads.emplace_back(consumerProc);
  for (unsigned i = 0; i < producers; i++)
    threads.emplace_back(producerProc, i);
  for (auto &thread: threads)
    thread.join();

  uintptr_t lost = 0;
  for (auto &counter: seen) {
    if (counter.load(std::memory_order_relaxed) == 0)
      lost++;
  }

  printf("popped garbage: %" PRIuPTR ", duplicates: %" PRIuPTR ", lost: %" PRIuPTR "\n",
         errGarbage.load(), errDuplicate.load(), lost);
  if (errGarbage.load() || errDuplicate.load() || lost) {
    printf("FAILED\n");
    return 1;
  }

  printf("OK\n");
  return 0;
}
