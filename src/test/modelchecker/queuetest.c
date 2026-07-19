// Bounded GenMC harness for ConcurrentQueue. Run all cases with
// src/test/modelchecker/run.sh. Set GENMC or put genmc in PATH.
//
// Cases: 1 - two producers, 2 - two consumers, 3 - partition growth,
// 4 - payload publication, 5 - claim versus seal. Spin-assume is disabled
// because it does not account for compare-exchange updating expected and
// reports the fixed claim-versus-seal path as a non-terminating spinloop.
// The production first partition holds 4096 elements; this translation unit
// reduces it to two to keep exhaustive exploration practical.
#include <assert.h>
#include <pthread.h>
#include <stdint.h>

#define CONCURRENT_QUEUE_INITIAL_SIZE_LOG2 1

#include "atomic.h"

// Negative control for the GPT-P24 test: emulate the old compare-and-swap
// claim, which did not refresh expected after losing to the partition seal.
#ifdef QUEUE_MODEL_BROKEN_P24
static unsigned modelBrokenClaimFailures;

static int modelBrokenCompareExchange(uintptr_t volatile *ptr,
                                      uintptr_t *expected,
                                      uintptr_t desired,
                                      AtomicMemoryOrder order)
{
  int result = __uintptr_atomic_compare_and_swap(ptr, *expected, desired, order);
  if (!result)
    assert(modelBrokenClaimFailures++ == 0);
  return result;
}

#define __uintptr_atomic_compare_exchange modelBrokenCompareExchange
#endif

// Negative control for the payload-publication test.
#ifdef QUEUE_MODEL_BROKEN_ORDER
#define amoAcquire amoRelaxed
#define amoRelease amoRelaxed
#endif

// Include the implementation so the claim-versus-seal case can exercise the
// private partitionPush function without copying its algorithm into the test.
#include "../../asyncio/ringBuffer.c"

#ifndef QUEUE_MODEL_CASE
#define QUEUE_MODEL_CASE 1
#endif

static void recordValue(unsigned *seen, unsigned count, void *data)
{
  uintptr_t value = (uintptr_t)data;
  assert(value > 0 && value <= count);
  assert(seen[value - 1]++ == 0);
}

#if QUEUE_MODEL_CASE == 1

static ConcurrentQueue queue;

static void *pushValue(void *data)
{
  concurrentQueuePush(&queue, data);
  return 0;
}

int main(void)
{
  pthread_t first;
  pthread_t second;
  void *data;
  unsigned seen[2] = {0, 0};

  assert(pthread_create(&first, 0, pushValue, (void*)1) == 0);
  assert(pthread_create(&second, 0, pushValue, (void*)2) == 0);
  assert(pthread_join(first, 0) == 0);
  assert(pthread_join(second, 0) == 0);

  assert(concurrentQueuePop(&queue, &data));
  recordValue(seen, 2, data);
  assert(concurrentQueuePop(&queue, &data));
  recordValue(seen, 2, data);
  assert(!concurrentQueuePop(&queue, &data));
  return 0;
}

#elif QUEUE_MODEL_CASE == 2

static ConcurrentQueue queue;
static void *popped[2];

static void *popValue(void *arg)
{
  uintptr_t index = (uintptr_t)arg;
  assert(concurrentQueuePop(&queue, &popped[index]));
  return 0;
}

int main(void)
{
  pthread_t first;
  pthread_t second;
  void *data;
  unsigned seen[2] = {0, 0};

  concurrentQueuePush(&queue, (void*)1);
  concurrentQueuePush(&queue, (void*)2);

  assert(pthread_create(&first, 0, popValue, (void*)0) == 0);
  assert(pthread_create(&second, 0, popValue, (void*)1) == 0);
  assert(pthread_join(first, 0) == 0);
  assert(pthread_join(second, 0) == 0);

  recordValue(seen, 2, popped[0]);
  recordValue(seen, 2, popped[1]);
  assert(!concurrentQueuePop(&queue, &data));
  return 0;
}

#elif QUEUE_MODEL_CASE == 3

static ConcurrentQueue queue;
static void *popped;

static void *pushThird(void *unused)
{
  (void)unused;
  concurrentQueuePush(&queue, (void*)3);
  return 0;
}

static void *popFirst(void *unused)
{
  (void)unused;
  assert(concurrentQueuePop(&queue, &popped));
  return 0;
}

int main(void)
{
  pthread_t producer;
  pthread_t consumer;
  void *data;
  unsigned seen[3] = {0, 0, 0};

  concurrentQueuePush(&queue, (void*)1);
  concurrentQueuePush(&queue, (void*)2);

  assert(pthread_create(&producer, 0, pushThird, 0) == 0);
  assert(pthread_create(&consumer, 0, popFirst, 0) == 0);
  assert(pthread_join(producer, 0) == 0);
  assert(pthread_join(consumer, 0) == 0);

  recordValue(seen, 3, popped);
  assert(concurrentQueuePop(&queue, &data));
  recordValue(seen, 3, data);
  assert(concurrentQueuePop(&queue, &data));
  recordValue(seen, 3, data);
  assert(!concurrentQueuePop(&queue, &data));
  return 0;
}

#elif QUEUE_MODEL_CASE == 4

typedef struct QueuePayload {
  int value;
} QueuePayload;

static ConcurrentQueue queue;
static QueuePayload payload;
static QueuePayload *popped;

static void *publishPayload(void *unused)
{
  (void)unused;
  payload.value = 42;
  concurrentQueuePush(&queue, &payload);
  return 0;
}

static void *consumePayload(void *unused)
{
  (void)unused;
  void *data;
  if (concurrentQueuePop(&queue, &data)) {
    popped = (QueuePayload*)data;
    assert(popped->value == 42);
  }
  return 0;
}

int main(void)
{
  pthread_t producer;
  pthread_t consumer;

  assert(pthread_create(&producer, 0, publishPayload, 0) == 0);
  assert(pthread_create(&consumer, 0, consumePayload, 0) == 0);
  assert(pthread_join(producer, 0) == 0);
  assert(pthread_join(consumer, 0) == 0);
  return 0;
}

#elif QUEUE_MODEL_CASE == 5

static ConcurrentQueuePartition partition;
static ConcurrentQueueElement element;
static int pushResult;
static int sealResult;

static void *pushClaim(void *unused)
{
  (void)unused;
  pushResult = partitionPush(&partition, &element, (void*)1, 0);
  return 0;
}

static void *sealPartition(void *unused)
{
  (void)unused;
  sealResult = __uintptr_atomic_compare_and_swap(&partition.enqueuePos, 0, CONCURRENT_QUEUE_SEALED, amoSeqCst);
  return 0;
}

int main(void)
{
  pthread_t producer;
  pthread_t consumer;

  partition.queue = &element;
  element.sequence = 0;

  assert(pthread_create(&producer, 0, pushClaim, 0) == 0);
  assert(pthread_create(&consumer, 0, sealPartition, 0) == 0);
  assert(pthread_join(producer, 0) == 0);
  assert(pthread_join(consumer, 0) == 0);

  assert(pushResult + sealResult == 1);
  if (pushResult)
    assert(element.data == (void*)1 && element.sequence == 1);
  return 0;
}

#else
#error Unknown QUEUE_MODEL_CASE
#endif
