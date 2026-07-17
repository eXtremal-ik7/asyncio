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

// --- Deterministic straggler replay -----------------------------------------
// A producer that loaded WritePartition and stalled can resume after the
// partition was filled, drained and abandoned by both cursors; the fully
// wrapped ring accepts its claim as the next lap and the element is lost.
// Far too narrow for thread stress (two-instruction window, stall spanning a
// whole fill+drain cycle), so the schedule is replayed by hand in one thread:
// the straggler's steps after the stale load are partitionPush from
// ringBuffer.c verbatim, with plain accesses (the replay has no concurrency).
static int stragglerResumePush(ConcurrentQueuePartition *partition, void *data, size_t mask)
{
  ConcurrentQueueElement *ring = partition->queue;
  size_t pos = partition->enqueuePos;
  for (;;) {
    ConcurrentQueueElement *element = &ring[pos & mask];
    size_t seq = element->sequence;
    intptr_t diff = (intptr_t)seq - (intptr_t)pos;
    if (diff == 0) {
      partition->enqueuePos = pos + 1;
      element->data = data;
      element->sequence = pos + 1;
      return 1;
    }
    if (diff < 0)
      return 0;
    pos = partition->enqueuePos;
  }
}

static bool stragglerReplay()
{
  static ConcurrentQueue ladder;

  // The straggler reads WritePartition == 0 here and stalls; the other
  // producers fill partition 0 to the brim (the push that finds it full
  // advances the ladder and lands in partition 1)
  uintptr_t pushed = 0;
  while (ladder.WritePartition == 0)
    concurrentQueuePush(&ladder, (void*)++pushed);

  size_t capacity = (size_t)pushed - 1;
  if (capacity == 0 || (capacity & (capacity - 1)) != 0) {
    fprintf(stderr, "error: unexpected partition 0 capacity %zu\n", capacity);
    return false;
  }

  // Consumers drain everything: the read cursor leaves partition 0 for good
  uintptr_t popped = 0;
  void *data;
  while (concurrentQueuePop(&ladder, &data))
    popped++;
  if (popped != pushed) {
    fprintf(stderr, "error: drained %" PRIuPTR " of %" PRIuPTR " elements\n",
            popped, pushed);
    return false;
  }

  // The straggler resumes into the abandoned partition. If the claim is
  // refused, finish exactly as concurrentQueuePush would: re-read the ladder
  // cursor and push into the live partition.
  void *marker = &ladder;
  int landedStale = stragglerResumePush(&ladder.Partitions[0], marker, capacity - 1);
  if (!landedStale)
    concurrentQueuePush(&ladder, marker);

  // No silent loss: a push that reported success must be visible to consumers
  if (!concurrentQueuePop(&ladder, &data)) {
    fprintf(stderr, "error: straggler element lost (stale-partition claim %s)\n",
            landedStale ? "succeeded" : "refused");
    return false;
  }
  if (data != marker) {
    fprintf(stderr, "error: straggler replay popped garbage: %p\n", data);
    return false;
  }
  if (concurrentQueuePop(&ladder, &data)) {
    fprintf(stderr, "error: straggler replay left an extra element\n");
    return false;
  }
  return true;
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

  bool replayOk = stragglerReplay();
  printf("straggler replay: %s\n", replayOk ? "OK" : "FAILED");
  if (!replayOk)
    return 1;

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
