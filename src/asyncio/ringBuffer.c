// Lock free unbounded queue
// Based on bounded queue code from Dmitry Vyukov
// http://www.1024cores.net

#include "asyncio/ringBuffer.h"
#include "atomic.h"
#include <assert.h>
#include <stdlib.h>

#define CONCURRENT_QUEUE_INITIAL_SIZE_LOG2 12

static void partitionInit(ConcurrentQueuePartition *buffer, size_t size)
{
  assert((size & (size-1)) == 0 && "Invalid ring buffer size");
  if (!__pointer_atomic_load((void *volatile*)&buffer->queue, amoAcquire)) {
    ConcurrentQueueElement *queue = (ConcurrentQueueElement*)malloc(size*sizeof(ConcurrentQueueElement));
    for (size_t i = 0; i < size; i++)
      queue[i].sequence = i;
    if (!__pointer_atomic_compare_and_swap((void *volatile*)&buffer->queue, 0, queue))
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
      if (__uintptr_atomic_compare_and_swap(&buffer->enqueuePos, pos, pos+1))
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
      if (__uintptr_atomic_compare_and_swap(&buffer->dequeuePos, pos, pos+1))
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

    __uint_atomic_compare_and_swap(&queue->WritePartition, currentWritePartition, currentWritePartition+1);
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

    __uint_atomic_compare_and_swap(&queue->ReadPartition, currentReadPartition, currentReadPartition+1);
  }
}
