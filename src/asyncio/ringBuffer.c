// Lock free unbounded queue
// Based on bounded queue code from Dmitry Vyukov
// http://www.1024cores.net

#include "asyncio/ringBuffer.h"
#include "atomic.h"
#include <assert.h>
#include <stdlib.h>

#define CONCURRENT_QUEUE_INITIAL_SIZE_LOG2 12

// A drained, abandoned partition has enqueuePos pinned here forever: 2^62 is
// beyond any reachable position (the largest ring holds 2^43 elements), so
// every cell reads as "full" and a late push re-routes to the live partition.
#define CONCURRENT_QUEUE_SEALED ((size_t)1 << 62)

static void partitionInit(ConcurrentQueuePartition *buffer, size_t size)
{
  assert((size & (size-1)) == 0 && "Invalid ring buffer size");
  if (!__pointer_atomic_load((void *volatile*)&buffer->queue, amoAcquire)) {
    ConcurrentQueueElement *queue = (ConcurrentQueueElement*)malloc(size*sizeof(ConcurrentQueueElement));
    for (size_t i = 0; i < size; i++)
      queue[i].sequence = i;
    if (!__pointer_atomic_compare_and_swap((void *volatile*)&buffer->queue,
                                           0,
                                           queue,
                                           amoSeqCst))
      free(queue);
  }
}

static int partitionPush(ConcurrentQueuePartition *buffer, void *data, size_t mask)
{
  ConcurrentQueueElement *queue = (ConcurrentQueueElement*)__pointer_atomic_load((void *volatile*)&buffer->queue, amoAcquire);
  ConcurrentQueueElement *element = 0;
  size_t pos = __uintptr_atomic_load(&buffer->enqueuePos, amoRelaxed);
  for (;;) {
    element = &queue[pos & mask];
    // Acquire pairs with consumer's release: the cell is reusable only after its previous data was read
    size_t seq = __uintptr_atomic_load(&element->sequence, amoAcquire);
    intptr_t diff = (intptr_t)seq - (intptr_t)pos;
    if (diff == 0) {
      // The position word only allocates the cell index: exclusivity comes
      // from the CAS atomicity itself, and every publication edge rides
      // element->sequence (the acquire above cannot sink below its check,
      // the release below pairs with the other side)
      if (__uintptr_atomic_compare_and_swap(&buffer->enqueuePos, pos, pos+1, amoRelaxed))
        break;
    } else if (diff < 0) {
      // Queue is full
      return 0;
    } else {
      pos = __uintptr_atomic_load(&buffer->enqueuePos, amoRelaxed);
    }
  }

  element->data = data;
  // Release makes the data store visible before the new sequence value
  __uintptr_atomic_store(&element->sequence, pos + 1, amoRelease);
  return 1;
}

static int partitionPop(ConcurrentQueuePartition *buffer, void **data, size_t mask)
{
  ConcurrentQueueElement *queue = (ConcurrentQueueElement*)__pointer_atomic_load((void *volatile*)&buffer->queue, amoAcquire);
  if (!queue)
    return 0;

  ConcurrentQueueElement *element = 0;
  size_t pos = __uintptr_atomic_load(&buffer->dequeuePos, amoRelaxed);
  for (;;) {
    element = &queue[pos & mask];
    // Acquire pairs with producer's release: data store must be visible before the sequence check passes
    size_t seq = __uintptr_atomic_load(&element->sequence, amoAcquire);
    intptr_t diff = (intptr_t)seq - (intptr_t)(pos+1);
    if (diff == 0) {
      // Relaxed for the same reason as the enqueuePos claim: the index
      // allocator publishes nothing, element->sequence carries all edges
      if (__uintptr_atomic_compare_and_swap(&buffer->dequeuePos, pos, pos+1, amoRelaxed))
        break;
    } else if (diff < 0) {
      // Queue is empty
      return 0;
    } else {
      pos = __uintptr_atomic_load(&buffer->dequeuePos, amoRelaxed);
    }
  }

  *data = element->data;
  // Release keeps the data load above; producers must not overwrite the cell before it completes
  __uintptr_atomic_store(&element->sequence, pos + (mask+1), amoRelease);
  return 1;
}

void concurrentQueuePush(ConcurrentQueue *queue, void *data)
{
  for (;;) {
    uint32_t currentWritePartition = __uint_atomic_load(&queue->WritePartition, amoRelaxed);
    ConcurrentQueuePartition *partition = &queue->Partitions[currentWritePartition];
    size_t partitionSize = (size_t)1 << (currentWritePartition + CONCURRENT_QUEUE_INITIAL_SIZE_LOG2);
    size_t mask = partitionSize-1;

    partitionInit(partition, partitionSize);
    if (partitionPush(partition, data, mask))
      return;

    // The last partition cannot fill up (its element array alone exceeds the
    // user address space), but the index must never leave the array: if it
    // somehow did fill, the push degrades to spinning on the full partition
    // instead of indexing out of bounds
    if (currentWritePartition + 1 < CONCURRENT_QUEUE_PARTITIONS)
      __uint_atomic_compare_and_swap(&queue->WritePartition,
                                     currentWritePartition,
                                     currentWritePartition+1,
                                     amoSeqCst);
  }
}

int concurrentQueuePop(ConcurrentQueue *queue, void **data)
{
  for (;;) {
    uint32_t currentReadPartition = __uint_atomic_load(&queue->ReadPartition, amoRelaxed);
    ConcurrentQueuePartition *partition = &queue->Partitions[currentReadPartition];
    size_t partitionSize = (size_t)1 << (currentReadPartition + CONCURRENT_QUEUE_INITIAL_SIZE_LOG2);
    size_t mask = partitionSize-1;

    if (partitionPop(partition, data, mask))
      return 1;

    if (currentReadPartition == __uint_atomic_load(&queue->WritePartition, amoRelaxed))
      return 0;

    // Advance only past a drained partition, sealing it on the way out. A
    // failed pop can also mean a claimed-but-not-published cell: advancing
    // would strand every later element (the ladder never returns). The seal
    // pins enqueuePos to a permanently-full value so a producer stalled on a
    // stale cursor cannot resume into the abandoned ring as its next lap.
    // Claims race the seal on the same word, so the CAS settles it: success
    // only if still drained; a slipped-in claim fails it and the pop retries;
    // a partition sealed by another consumer just needs the cursor helped.
    size_t enqueuePos = __uintptr_atomic_load(&partition->enqueuePos, amoSeqCst);
    if (enqueuePos != CONCURRENT_QUEUE_SEALED) {
      if (enqueuePos != __uintptr_atomic_load(&partition->dequeuePos, amoSeqCst))
        continue;
      if (!__uintptr_atomic_compare_and_swap(&partition->enqueuePos,
                                             enqueuePos,
                                             CONCURRENT_QUEUE_SEALED,
                                             amoSeqCst))
        continue;
    }

    __uint_atomic_compare_and_swap(&queue->ReadPartition,
                                   currentReadPartition,
                                   currentReadPartition+1,
                                   amoSeqCst);
  }
}
