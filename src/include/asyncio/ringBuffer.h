// Lock free bounded queue
// Original code from Dmitry Vyukov
// http://www.1024cores.net

#ifndef __ASYNCIO_RINGBUFFER_H_
#define __ASYNCIO_RINGBUFFER_H_

#include <stddef.h>
#include <stdint.h>

typedef struct ConcurrentQueueElement {
  void *data;
  volatile size_t sequence;
} ConcurrentQueueElement;

// Partition i holds 2^(i+12) elements (4096 in the first, doubling onward).
// The ladder is what makes the queue unbounded, so its top must be
// unreachable: with 32 partitions the element array of the last one alone
// is 2^47 bytes - past the 47..48-bit user address space - so the ladder
// cannot be exhausted by construction.
#define CONCURRENT_QUEUE_PARTITIONS 32

// The buffer pointer is read by both sides on every operation, enqueuePos is
// a CAS word contended by producers (sealed to a poison value once the
// partition is drained and abandoned - see ringBuffer.c), dequeuePos - by
// consumers. The pads keep each on its own cache line (Vyukov's layout), so
// one side's CAS traffic does not invalidate the other side's loads; the
// trailing pad isolates dequeuePos from the next partition's buffer pointer
// as well.
typedef struct ConcurrentQueuePartition {
  ConcurrentQueueElement *queue;
  char pad0[64 - sizeof(ConcurrentQueueElement*)];
  volatile size_t enqueuePos;
  char pad1[64 - sizeof(size_t)];
  volatile size_t dequeuePos;
  char pad2[64 - sizeof(size_t)];
} ConcurrentQueuePartition;

typedef struct ConcurrentQueue {
  ConcurrentQueuePartition Partitions[CONCURRENT_QUEUE_PARTITIONS];
  volatile uint32_t ReadPartition;
  volatile uint32_t WritePartition;
} ConcurrentQueue;

// Concurrent ring buffer API
void concurrentQueuePush(ConcurrentQueue *queue, void *data);
int concurrentQueuePop(ConcurrentQueue *queue, void **data);

#endif //__ASYNCIO_RINGBUFFER_H_
