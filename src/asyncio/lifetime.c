#include "lifetime.h"
#include "asyncioImpl.h"
#include "asyncio/coroutine.h"
#include "atomic.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

void *alignedMalloc(size_t size, size_t alignment)
{
#ifdef OS_COMMONUNIX
  void *memptr;
  return posix_memalign(&memptr, alignment, size) == 0 ? memptr : 0;
#else
  return _aligned_malloc(size, alignment);
#endif
}

void alignedFree(void *ptr)
{
#ifdef OS_COMMONUNIX
  free(ptr);
#else
  _aligned_free(ptr);
#endif
}

void *objectAlloc(ConcurrentQueue *pool, size_t size, size_t alignment)
{
  void *object = 0;
  if (concurrentQueuePop(pool, &object)) {
    ASAN_UNPOISON_MEMORY_REGION(object, size);
    return object;
  }
  if (size < sizeof(objectHeader) || alignment < 16 || (alignment & (alignment - 1)) != 0)
    return 0;
  object = alignedMalloc(size, alignment);
  if (!object)
    return 0;
  // The header may later be inspected by a kernel producer without a
  // C-language publication edge. Keep even fresh initialization atomic;
  // relaxed stores are plain stores on the supported CPUs and publish no
  // payload.
  memset((uint8_t*)object + sizeof(uint128), 0, size - sizeof(uint128));
  objectHeader *header = (objectHeader*)object;
  __uint64_atomic_store(&header->tag.low, 0, amoRelaxed);
  __uint64_atomic_store(&header->tag.high, 1, amoRelaxed);
  return object;
}

void objectFree(ConcurrentQueue *pool, void *object, size_t size)
{
  assert(object && size >= sizeof(objectHeader));
  // A stale envelope still reads tag.high and the immutable category. The
  // per-incarnation detail and base remain protected by the type-specific
  // claim, so poison them together with the rest of the dead object.
  ASAN_POISON_MEMORY_REGION((uint8_t*)object + offsetof(objectHeader, base), size - offsetof(objectHeader, base));
  concurrentQueuePush(pool, object);
}

void *__tagged_pointer_make(void *ptr, uintptr_t data)
{
  return (void*)(((intptr_t)ptr) + ((intptr_t)(data & TAGGED_POINTER_DATA_MASK)));
}

void __tagged_pointer_decode(void *ptr, void **outPtr, uintptr_t *outData)
{
  intptr_t p = (intptr_t)ptr;
  *outPtr = (void*)(p & TAGGED_POINTER_PTR_MASK);
  *outData = p & TAGGED_POINTER_DATA_MASK;
}

uintptr_t objectIncrementReference(aioObjectRoot *object, uintptr_t count)
{
  // Retain is legal only through an already-owned reference and publishes no
  // payload. The final release below is the sole lifetime synchronization
  // point, so acquire ordering on every increment would only tax ARM64.
  uintptr_t result = __uintptr_atomic_fetch_and_add(&object->refs, count, amoRelaxed);
  assert(result != 0 && "Removed object access detected");
  return result;
}

uintptr_t objectDecrementReference(aioObjectRoot *object, uintptr_t count)
{
  uintptr_t result = __uintptr_atomic_fetch_and_add(&object->refs, (uintptr_t)0 - count, amoRelease);
  assert((intptr_t)result > 0 && "Double object release detected");
  if (result == count) {
    // Only the last releaser pays acquire. This load reads the zero published
    // by our release RMW and therefore synchronizes with every release whose
    // RMW belongs to the same release sequence. Besides being cheaper than an
    // acquire fence on ARM64, an acquire load is understood by GCC TSan.
    uintptr_t finalRefs = __uintptr_atomic_load(&object->refs, amoAcquire);
    assert(finalRefs == 0 && "Object reference resurrected from zero");
    (void)finalRefs;
    combinerPushDelete(object);
  }
  return result;
}

void initObjectRoot(aioObjectRoot *object, asyncBase *base, IoObjectTy type, aioObjectDestructor destructor)
{
  // Stale kernel producers may still validate the previous incarnation with
  // a DWCAS while this type-stable cell is initialized, so every tag access
  // remains atomic. Their generation cannot match and they cannot write.
  // Construction is published later by the first ownership transition, not
  // through this reset, therefore a relaxed store is sufficient here.
  __uint64_atomic_store(&object->header.tag.low, taggedAsyncOpNull().data, amoRelaxed);
  object->readQueue.head = object->readQueue.tail = 0;
  object->writeQueue.head = object->writeQueue.tail = 0;
  objectHeaderSetType(&object->header, ohtObject);
  object->header.objectType = type;
  object->header.base = base;
  // These words remain atomic for their whole pooled lifetime. Relaxed init
  // is still a plain store on the supported CPUs, but avoids mixed
  // atomic/plain accesses at the hand-off between object incarnations; the
  // first Head ownership transition, not these stores, publishes construction.
  __uintptr_atomic_store(&object->refs, 1, amoRelaxed);
  object->destructor = destructor;
  object->destructorCb = 0;
  object->destructorCbArg = 0;
  __uint_atomic_store(&object->DeletePending, 0, amoRelaxed);
  // initializationOp is protected by combiner ownership after publication.
  object->initializationOp = 0;
}

void objectSetDestructorCb(aioObjectRoot *object, aioObjectDestructorCb callback, void *arg)
{
  object->destructorCb = callback;
  object->destructorCbArg = arg;
}

void cancelIo(aioObjectRoot *object)
{
  combinerPushCounter(object, COMBINER_TAG_CANCELIO);
}

void objectDelete(aioObjectRoot *object)
{
  // Before the cancelIo push: the pass it triggers must already see the flag.
  // Release-store: readers use relaxed loads, cross-thread visibility rides on
  // the seq-cst Head publication of that push; the flag is sticky, so a stale
  // read only delays the gate by one pass.
  __uint_atomic_store(&object->DeletePending, 1, amoRelease);
  cancelIo(object);
  objectDecrementReference(object, 1);
}

#if ASYNCIO_POOL_MARKS
// Pool -> element size map, filled at the first fresh allocation from each
// pool (a pool's first put cannot precede its first alloc, so release always
// finds its entry). It lets releaseAsyncOp poison the parked operation
// without threading a size through every release path. Racing registrations
// of one pool may produce duplicates; lookup takes the first match and the
// sizes are equal, so they are harmless.
typedef struct OpPoolSizeEntry {
  ConcurrentQueue *volatile pool;
  size_t size;
} OpPoolSizeEntry;

static OpPoolSizeEntry opPoolSizes[32];
static volatile unsigned opPoolSizeCount;

static void opPoolSizeRegister(ConcurrentQueue *pool, size_t size)
{
  for (;;) {
    unsigned count = __uint_atomic_load(&opPoolSizeCount, amoAcquire);
    for (unsigned i = 0; i < count; i++) {
      if (__pointer_atomic_load((void *volatile*)&opPoolSizes[i].pool, amoAcquire) == pool)
        return;
    }
    if (count >= sizeof(opPoolSizes) / sizeof(opPoolSizes[0]))
      return; // Overflow: operations of extra pools park unmarked.
    if (__uint_atomic_compare_and_swap(&opPoolSizeCount, count, count + 1, amoSeqCst)) {
      opPoolSizes[count].size = size;
      // Size is written before the pool pointer publishes the entry.
      __pointer_atomic_store((void *volatile*)&opPoolSizes[count].pool, pool, amoRelease);
      return;
    }
  }
}

static size_t opPoolSizeLookup(ConcurrentQueue *pool)
{
  unsigned count = __uint_atomic_load(&opPoolSizeCount, amoAcquire);
  for (unsigned i = 0; i < count; i++) {
    if (__pointer_atomic_load((void *volatile*)&opPoolSizes[i].pool, amoAcquire) == pool)
      return opPoolSizes[i].size;
  }
  return 0;
}
#endif

int asyncOpAlloc(asyncBase *base,
                 size_t size,
                 int isRealTime,
                 ConcurrentQueue *objectPool,
                 ConcurrentQueue *objectTimerPool,
                 asyncOpRoot **result)
{
  int hasAllocatedNew = 0;
  asyncOpRoot *op = 0;
  ConcurrentQueue *buffer = !isRealTime ? objectPool : objectTimerPool;
  if (!concurrentQueuePop(buffer, (void**)&op)) {
    op = (asyncOpRoot*)alignedMalloc(size, 1u << COMBINER_TAG_SIZE);
    if (!op) {
      *result = 0;
      return 0;
    }
    if (isRealTime) {
      base->methodImpl.initializeTimer(base, op);
      poolCacheHandoff(op->timerId);
    } else {
      op->timerId = 0;
    }
    op->tag = 0;
    hasAllocatedNew = 1;
#if ASYNCIO_POOL_MARKS
    opPoolSizeRegister(buffer, size);
  } else {
    ASAN_UNPOISON_MEMORY_REGION(op, size);
#endif
  }

  op->objectPool = buffer;
  *result = op;
  return hasAllocatedNew;
}

void releaseAsyncOp(asyncOpRoot *op)
{
  aioObjectRoot *object = op->object;
  ConcurrentQueue *pool = op->objectPool;
#if ASYNCIO_POOL_MARKS
  // Mark before publishing: once the pointer is in the pool another thread
  // may pop and unmark it. The leading tag word stays readable - stale grid
  // links and realtime timer envelopes validate through it (see asyncOpRoot).
  size_t size = opPoolSizeLookup(pool);
  if (size)
    ASAN_POISON_MEMORY_REGION((uint8_t*)op + sizeof(op->tag), size - sizeof(op->tag));
#endif
  concurrentQueuePush(pool, op);
  objectDecrementReference(object, 1);
}

void initAsyncOpRoot(asyncOpRoot *op,
                     aioExecuteProc *startMethod,
                     aioCancelProc *cancelMethod,
                     aioFinishProc *finishMethod,
                     aioReleaseProc *releaseMethod,
                     aioObjectRoot *object,
                     void *callback,
                     void *arg,
                     AsyncFlags flags,
                     int opCode,
                     uint64_t timeout)
{
  op->executeMethod = startMethod;
  op->cancelMethod = cancelMethod;
  // TODO: better type control
  op->finishMethod = (flags & afCoroutine) ? (aioFinishProc*)coroutineCurrent() : finishMethod;
  op->releaseMethod = releaseMethod;
  op->executeQueue.prev = 0;
  op->executeQueue.next = 0;
  op->next = taggedAsyncOpNull();
  op->object = object;
  op->flags = flags;
  op->opCode = opCode;
  op->callback = callback;
  op->arg = arg;
  op->timeout = timeout < MAX_TIMEOUT_US ? timeout : MAX_TIMEOUT_US;
  op->running = (flags & afRunning) ? arRunning : arWaiting;
  objectIncrementReference(object, 1);
  // Publish the tag last, with release ordering. Until this store lands the
  // tag still holds the previous incarnation's terminal status, so any stale
  // CAS (kernel timer event, timeout-grid link) loses by status and never
  // reads the fields above mid-initialization. Whoever later wins a
  // generation-CAS on the tag acquires this store and therefore observes
  // every field written above — this is the only synchronization edge for
  // consumers whose op pointer travelled through the kernel (kevent udata /
  // epoll_data), a path the memory model knows nothing about.
  __uintptr_atomic_store(&op->tag, ((opGetGeneration(op) + 1) << TAG_STATUS_SIZE) | aosPending, amoRelease);
}
