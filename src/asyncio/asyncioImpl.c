#include "asyncioImpl.h"
#include "asyncio/coroutine.h"
#include "atomic.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef WIN32
#include <signal.h>
#endif

__tls unsigned currentFinishedSync;
__tls unsigned messageLoopThreadId;
__tls asyncBase *loopThreadBase;
static __tls unsigned loopThreadSlotOwned;

void eqRemove(List *list, asyncOpRoot *op)
{
  // An operation that lost its status race is detached by the list walker;
  // the racing party that won still releases it through here later. With
  // cleared links and not being the head it is provably not a member -
  // unlinking it anyway would rewrite head/tail through stale pointers and
  // corrupt whatever the queue (or the memory reuser) holds by now
  if (op->executeQueue.prev == 0 && op->executeQueue.next == 0 && list->head != op)
    return;

  if (op->executeQueue.prev) {
    op->executeQueue.prev->executeQueue.next = op->executeQueue.next;
  } else {
    list->head = op->executeQueue.next;
    if (list->head == 0)
      list->tail = 0;
  }

  if (op->executeQueue.next) {
    op->executeQueue.next->executeQueue.prev = op->executeQueue.prev;
  } else {
    list->tail = op->executeQueue.prev;
    if (list->tail == 0)
      list->head = 0;
  }

  op->executeQueue.prev = 0;
  op->executeQueue.next = 0;
}

void eqPushBack(List *list, asyncOpRoot *op)
{
  op->executeQueue.prev = list->tail;
  op->executeQueue.next = 0;
  if (list->tail) {
    list->tail->executeQueue.next = op;
    list->tail = op;
  } else {
    list->head = list->tail = op;
  }
}

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
  // C-language
  // publication edge. Keep even fresh initialization atomic; relaxed stores
  // are plain stores on the supported CPUs and publish no payload.
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

void __tagged_pointer_decode(void *ptr, void**outPtr, uintptr_t *outData)
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

void eventIncrementReference(aioUserEvent *event, uintptr_t count)
{
  (void)__uint64_atomic_fetch_and_add(&event->header.tag.low, count, amoRelaxed);
}

void eventDecrementReference(aioUserEvent *event, uintptr_t count)
{
  uintptr_t old = __uint64_atomic_fetch_and_add(&event->header.tag.low, 0ULL - count, amoRelease);
  uintptr_t refs = old & TAG_EVENT_REF_MASK;
#ifndef NDEBUG
  assert(refs >= count && "Double event release detected");
  assert((refs != count || (old & TAG_EVENT_DELETE)) && "Final event reference released without deleteUserEvent");
#endif
  // The last release finalizes; the contract requires a preceding Delete.
  if (refs == count) {
    __atomic_fence(amoAcquire);
    if (event->destructorCb)
      event->destructorCb(event, event->destructorCbArg);

    // Atomic generation increment and full event release
    uintptr_t generation = __uint64_atomic_load(&event->header.tag.high, amoRelaxed);
    __uint64_atomic_store(&event->header.tag.high, generation + 1, amoRelaxed);
    event->header.base->methodImpl.releaseUserEvent(event);
  }
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
  __uintptr_atomic_store(&object->initializationOp, 0, amoRelaxed);
}

void objectSetDestructorCb(aioObjectRoot *object, aioObjectDestructorCb callback, void *arg)
{
  object->destructorCb = callback;
  object->destructorCbArg = arg;
}

void eventSetDestructorCb(aioUserEvent *event, userEventDestructorCb callback, void *arg)
{
  event->destructorCb = callback;
  event->destructorCbArg = arg;
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
  // read only delays the gate by one pass
  __uint_atomic_store(&object->DeletePending, 1, amoRelease);
  cancelIo(object);
  objectDecrementReference(object, 1);
}

uintptr_t opGetGeneration(asyncOpRoot *op)
{
  return __uintptr_atomic_load(&op->tag, amoRelaxed) >> TAG_STATUS_SIZE;
}

AsyncOpStatus opGetStatus(asyncOpRoot *op)
{
  return __uintptr_atomic_load(&op->tag, amoRelaxed) & TAG_STATUS_MASK;
}

int opSetStatus(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status)
{
  return __uintptr_atomic_compare_and_swap(&op->tag,
                                           (generation << TAG_STATUS_SIZE) | aosPending,
                                           (generation << TAG_STATUS_SIZE) | (uintptr_t)status,
                                           amoSeqCst);
}

void opForceStatus(asyncOpRoot *op, AsyncOpStatus status)
{
  __uintptr_atomic_store(&op->tag, (__uintptr_atomic_load(&op->tag, amoRelaxed) & TAG_GENERATION_MASK) | (uintptr_t)status, amoRelaxed);
}

#if ASYNCIO_POOL_MARKS
// Pool -> element size map, filled at the first fresh allocation from each
// pool (a pool's first put cannot precede its first alloc, so release always
// finds its entry). It lets releaseAsyncOp poison the parked operation
// without threading a size through every release path. Racing registrations
// of one pool may produce duplicates; lookup takes the first match and the
// sizes are equal, so they are harmless.
typedef struct OpPoolSizeEntry {
  ConcurrentQueue * volatile pool;
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
      return;  // overflow: operations of extra pools park unmarked
    if (__uint_atomic_compare_and_swap(&opPoolSizeCount, count, count + 1, amoSeqCst)) {
      opPoolSizes[count].size = size;
      // Size is written before the pool pointer publishes the entry
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
                 asyncOpRoot**result)
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
  // links and realtime timer envelopes validate through it (see asyncOpRoot)
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

static void opArmTimer(asyncOpRoot *op)
{
  if (op->timeout) {
    asyncBase *base = op->object->header.base;
    if (op->flags & afRealtime) {
      // start timer for this operation
      base->methodImpl.startTimer(op);
    } else {
      // Saturating add: a huge timeout must not wrap into the past
      uint64_t nowUs = getMonotonicTicks() * TIMER_TICK_MICROSECONDS;
      op->endTime = nowUs + op->timeout;
      if (op->endTime < nowUs)
        op->endTime = UINT64_MAX;
      addToTimeoutQueue(base, op);
    }
  }
}

static void opRun(asyncOpRoot *op, List *list)
{
  eqPushBack(list, op);
  opArmTimer(op);
}

// One-shot transport initialization does not live in the read/write queues.
// Its slot is claimed before submission; ordinary operations submitted after
// it may queue, but cannot start until initialization leaves the slot. This
// ordering is an API contract: an object is not reinitialized, and no ordinary
// I/O may precede its initialization. Leaving the slot kicks both queues; a
// failure also cancels everything queued behind it with the same status.
static void initializationRelease(asyncOpRoot *op, AsyncOpStatus status, uint32_t *needStart)
{
  aioObjectRoot *object = op->object;
  if (__uintptr_atomic_load(&object->initializationOp, amoRelaxed) == (uintptr_t)op)
    __uintptr_atomic_store(&object->initializationOp, 0, amoRelaxed);
  opRelease(op, status, 0);
  if (status != aosSuccess) {
    cancelOperationList(&object->readQueue, status);
    cancelOperationList(&object->writeQueue, status);
  }
  if (needStart)
    *needStart |= IO_EVENT_READ | IO_EVENT_WRITE;
}

static void initializationTryComplete(asyncOpRoot *op, AsyncOpStatus status, uint32_t *needStart)
{
  if (status == aosPending) {
    op->running = arRunning;
    return;
  }

  // Terminal: the operation (connect / composite handshake) is done - a progress
  // signal brought us here, so the result stands even if a concurrent timeout
  // won the status race. Adopt our status if we win, otherwise keep the winner's
  // terminal status, and release now: nothing else will complete it (its child
  // operations are already consumed, so a late cancelMethod would find nothing).
  opSetStatus(op, opGetGeneration(op), status);
  initializationRelease(op, opGetStatus(op), needStart);
}

// Re-drive the parked initialization operation from backend READ/WRITE
// progress. Its one-shot-before-I/O contract makes either positional bit
// unambiguous while the slot is occupied.
void processInitializationOp(aioObjectRoot *object, uint32_t *needStart)
{
  asyncOpRoot *op = (asyncOpRoot*)__uintptr_atomic_load(&object->initializationOp, amoRelaxed);
  if (!op)
    return;
  AsyncOpStatus status = opGetStatus(op);
  switch (combinerSelectInitializationAction(op->running, status)) {
    case ciaNone: return;

    case ciaRelease:
      // Either a proactor completion arrived after cancellation, or a child
      // completion made a composite handshake terminal. In both cases the I/O
      // which owned the slot is done; do not re-run executeMethod.
      initializationRelease(op, status, needStart);
      return;

    case ciaExecute: initializationTryComplete(op, op->executeMethod(op), needStart); return;
  }
}

static void cancelInitializationOp(aioObjectRoot *object, AsyncOpStatus status)
{
  asyncOpRoot *op = (asyncOpRoot*)__uintptr_atomic_load(&object->initializationOp, amoRelaxed);
  if (!op)
    return;
  if (opSetStatus(op, opGetGeneration(op), status)) {
    if (op->running == arRunning) {
      op->running = arCancelling;
      if (op->cancelMethod(op))
        initializationRelease(op, status, 0);
    }
    // arWaiting: aaStart is still queued in the combiner; it will observe the
    // status and release the slot
  }
}

// The one bulk-cancel of everything parked on an object: the initialization
// slot and both queues. Shared by the three sweep positions (CANCELIO tag,
// DELETE tag, DeletePending at ownership release), which must stay in sync.
static void cancelAllObjectOperations(aioObjectRoot *object)
{
  cancelInitializationOp(object, aosCanceled);
  cancelOperationList(&object->readQueue, aosCanceled);
  cancelOperationList(&object->writeQueue, aosCanceled);
}

// Start an op-node captured by the combiner (submission). The former
// processAction(aaStart): the initialization-slot owner is armed and driven in place,
// an ordinary operation is queued; the combiner reconciles the rest by status.
void startOperation(asyncOpRoot *op, uint32_t *needStart)
{
  aioObjectRoot *object = op->object;

  // Ownership of the initialization slot (claimed by CAS at submission, e.g.
  // in aioConnect) routes an operation to the initialization path; there is no flag to
  // spoof, any other operation goes to its read/write queue.
  if (__uintptr_atomic_load(&object->initializationOp, amoRelaxed) == (uintptr_t)op) {
    if (opGetStatus(op) != aosPending) {
      // Cancelled between submission and start (a cancelIo sweep): the sweep
      // does not reach a not-yet-started operation, releasing the slot is on us
      initializationRelease(op, opGetStatus(op), needStart);
      return;
    }
    opArmTimer(op);
    if (opGetStatus(op) != aosPending) {
      // The arm itself expired the operation (its deadline window was already
      // swept): the timeout won before the I/O started, so never issue the
      // connect/handshake. Releasing here mirrors the pre-start branch above -
      // the cancel signal's sweep will find the slot already empty
      initializationRelease(op, opGetStatus(op), needStart);
      return;
    }
    initializationTryComplete(op, op->executeMethod(op), needStart);
    return;
  }

  List *list;
  uint32_t tag;
  if (op->opCode & OPCODE_WRITE) {
    list = &object->writeQueue;
    tag = IO_EVENT_WRITE;
  } else {
    list = &object->readQueue;
    tag = IO_EVENT_READ;
  }

  opRun(op, list);
  *needStart |= (list->head && list->head->running == arWaiting) ? tag : 0;
}

// Survivor accumulator shared by the positional sweeps (reapQueue and
// cancelOperationList): survivors keep their relative order in a freshly
// linked queue, everything else was released in place. The caller clears the
// op's links before appending, so a released op never leaves a stale prev
// behind.
typedef struct {
  asyncOpRoot *head;
  asyncOpRoot *tail;
} SurvivorList;

static inline void survivorAppend(SurvivorList *survivors, asyncOpRoot *op)
{
  op->executeQueue.prev = survivors->tail;
  if (survivors->tail)
    survivors->tail->executeQueue.next = op;
  else
    survivors->head = op;
  survivors->tail = op;
}

static inline void survivorCommit(List *list, const SurvivorList *survivors)
{
  list->head = survivors->head;
  list->tail = survivors->tail;
}

// Reap terminal operations from one queue positionally: release the ones with no
// I/O in flight, hold an in-flight op (cancelMethod == 0) on its head until its
// late completion. Pending operations are left untouched. The queue is rebuilt
// from survivors; if the new head is a ready-to-start operation, ask for a kick.
static void reapQueue(List *list, uint32_t tag, uint32_t *needStart)
{
  asyncOpRoot *op = list->head;
  SurvivorList survivors = {0, 0};
  while (op) {
    asyncOpRoot *next = op->executeQueue.next;
    int keep = 0, release = 0;
    AsyncOpStatus status = opGetStatus(op);
    switch (combinerSelectReapAction(op->running, status)) {
      case craKeep: keep = 1; break;

      case craCancel:
        op->running = arCancelling;
        if (op->cancelMethod(op))
          release = 1;
        else
          keep = 1; // in-flight: hold positional for the late completion
        break;

      case craRelease: release = 1; break;
    }

    op->executeQueue.prev = op->executeQueue.next = 0;
    if (keep)
      survivorAppend(&survivors, op);
    else if (release)
      opRelease(op, status, 0);
    op = next;
  }

  survivorCommit(list, &survivors);
  if (survivors.head && survivors.head->running == arWaiting)
    *needStart |= tag;
}

// CANCEL/CANCELIO reconcile: a cancel source (timeout/opCancel/cancelIo) has
// already set the terminal status (winner-takes) and asked the combiner to
// scan. Reap the terminals it left behind, positionally, across the
// initialization slot and both queues.
void reapObject(aioObjectRoot *object, uint32_t tag, uint32_t *needStart)
{
  // Positional cancelIo sweep: the CANCELIO bit rides the chain entry that
  // was the head at cancelIo() time, and the backend starts that entry's
  // operation before calling here - everything submitted before the call is
  // already in the queues and gets swept, entries pushed after it sit above
  // the bit and survive
  if (tag & COMBINER_TAG_CANCELIO)
    cancelAllObjectOperations(object);

  asyncOpRoot *ex = (asyncOpRoot*)__uintptr_atomic_load(&object->initializationOp, amoRelaxed);
  if (ex) {
    AsyncOpStatus status = opGetStatus(ex);
    switch (combinerSelectReapAction(ex->running, status)) {
      case craCancel:
        ex->running = arCancelling;
        if (ex->cancelMethod(ex))
          initializationRelease(ex, status, needStart);
        break;

      case craRelease: initializationRelease(ex, status, needStart); break;

      case craKeep:
        // Pending, or already cancelling an in-flight operation: wait for its
        // normal/late READ or WRITE progress.
        break;
    }
  }
  reapQueue(&object->readQueue, IO_EVENT_READ, needStart);
  reapQueue(&object->writeQueue, IO_EVENT_WRITE, needStart);
}

void opRelease(asyncOpRoot *op, AsyncOpStatus status, List *executeList)
{
  if (op->timerId && status != aosTimeout && (op->flags & afRealtime))
    op->object->header.base->methodImpl.stopTimer(op);

  if (executeList)
    eqRemove(executeList, op);
  if (op->releaseMethod)
    op->releaseMethod(op);
  addToGlobalQueue(op);
}

void executeOperationList(List *list)
{
  asyncOpRoot *op = list->head;
  // Both queues are frozen while transport initialization is in flight; they
  // are kicked when it leaves the slot.
  if (op && __uintptr_atomic_load(&op->object->initializationOp, amoRelaxed))
    return;

  while (op) {
    asyncOpRoot *next = op->executeQueue.next;
    AsyncOpStatus status = opGetStatus(op);
    if (status != aosPending) {
      // Already terminal - a progress completion set it (a PROGRESS_* signal
      // brought us here, so the I/O is done) or it was cancelled while queued.
      // Finish without re-issuing: re-running executeMethod on a proactor would
      // post a fresh overlapped I/O for a completed operation.
      opRelease(op, status, 0);
      op = next;
      continue;
    }

    // Dying object: nothing new may start. A run-to-completion submission
    // would finish inside this very pass, dodge the ownership-release sweep
    // (it only reaps queued operations) and let a flood pin the object past
    // objectDelete forever; leave it queued for the sweep to cancel
    if (__uint_atomic_load(&op->object->DeletePending, amoRelaxed))
      break;

    status = op->executeMethod(op);
    if (status == aosPending) {
      op->running = arRunning;
      break;
    }

    // The syscall has completed, so this position is ours to release even if
    // a concurrent timeout/cancel won the terminal status CAS. Its signal is
    // positional and cannot release the operation after we advance the queue;
    // preserve the winner's status, but never drop the completed operation.
    opSetStatus(op, opGetGeneration(op), status);
    opRelease(op, opGetStatus(op), 0);
    op = next;
  }

  list->head = op;
  if (op)
    // The dropped prefix was released without unlinking; a stale prev left
    // here would send a later eqRemove writing into recycled memory
    op->executeQueue.prev = 0;
  else
    list->tail = 0;
}

void cancelOperationList(List *list, AsyncOpStatus status)
{
  // Positional-aware bulk cancel (cancelIo / DeletePending / disconnect /
  // connect-fail cascade). An in-flight operation whose cancelMethod returns 0
  // (a proactor abort request the kernel owns until its completion arrives) must
  // stay on its head so the late positional PROGRESS_* signal still finds and
  // releases it - dropping it would strand the operation and, on objectDelete,
  // leak the object whose reference it holds. By the one-in-flight invariant at
  // most the head survives; the queue is rebuilt from the survivors rather than
  // wiped.
  asyncOpRoot *op = list->head;
  SurvivorList survivors = {0, 0};
  while (op) {
    asyncOpRoot *next = op->executeQueue.next;
    int keep = 0, release = 0;
    if (opSetStatus(op, opGetGeneration(op), status)) {
      if (op->running == arRunning) {
        op->running = arCancelling;
        if (op->cancelMethod(op))
          release = 1;
        else
          keep = 1; // in-flight: hold positional for the late completion
      } else {
        release = 1; // queued, not started: no I/O in flight, release now
      }
    } else {
      // Status race lost: the winner (a concurrent completion/timeout) drives
      // the release through its own reconcile scan. Leave the operation in the
      // queue - the signal no longer carries the op pointer, so the queue is the
      // only way that scan can still find it.
      keep = 1;
    }

    op->executeQueue.prev = op->executeQueue.next = 0;
    if (keep)
      survivorAppend(&survivors, op);
    else if (release)
      opRelease(op, status, 0);
    op = next;
  }

  survivorCommit(list, &survivors);
}

int opCancel(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status, aioObjectRoot *object, uintptr_t objectGeneration)
{
  // Cancel signal, gated on winning the terminal status (the loser is redundant,
  // the winner drives the cancel). The combiner scans and reaps positionally;
  // an in-flight op stays on its head until its late completion releases it.
  if (!opSetStatus(op, generation, status))
    return 1;
  if (!object)
    return 1;
  // op storage may recycle immediately after the winning status CAS. The arm
  // handle is the only legal route to its owner from this point onward.
  return combinerPushValidated(object, objectGeneration, COMBINER_TAG_CANCEL);
}

void resumeParent(asyncOpRoot *op, AsyncOpStatus status)
{
  // Progress from a child completion: always signal the parent, best-effort
  // status on failure. Never gate the signal on the status CAS - a child
  // completion must still release a parent a concurrent timeout put into
  // arCancelling (invariant 1/5).
  if (status != aosSuccess)
    opSetStatus(op, opGetGeneration(op), status);
  combinerPushProgress(op);
}

void addToGlobalQueue(asyncOpRoot *op)
{
  op->object->header.base->methodImpl.enqueue(op->object->header.base, op);
}

void executeGlobalQueue(asyncBase *base)
{
  asyncOpRoot *op;
  while (concurrentQueuePop(&base->globalQueue, (void**)&op)) {
    assert(op && "empty node in the global queue (the quit marker is gone)");
    if (eventIsQueueTask(op)) {
      eventExecuteQueuedTask(op);
      continue;
    }

    assert(opGetStatus(op) != aosPending && "finishing pending operation!");
    currentFinishedSync = 0;
    if (op->flags & afCoroutine) {
      assert(coroutineIsMain() && "Execute global queue from non-main coroutine");
      coroutineCall((coroutineTy*)op->finishMethod);
    } else {
      if (op->callback)
        op->finishMethod(op);
      releaseAsyncOp(op);
    }
  }
}

int copyFromBuffer(void *dst, size_t *offset, struct ioBuffer *src, size_t size)
{
  size_t needRead = size - *offset;
  size_t remaining = src->dataSize - src->offset;
  if (needRead <= remaining) {
    memcpy((uint8_t*)dst + *offset, (uint8_t*)src->ptr + src->offset, needRead);
    *offset += needRead;
    src->offset += needRead;
    return 1;
  } else {
    memcpy((uint8_t*)dst + *offset, (uint8_t*)src->ptr + src->offset, remaining);
    *offset += remaining;
    src->offset = 0;
    src->dataSize = 0;
    return 0;
  }
}

int loopThreadEnter(asyncBase *base)
{
  assert(!loopThreadSlotOwned && "one thread cannot nest asyncLoop invocations");
  if (loopThreadSlotOwned)
    return 0;
  const unsigned wordBits = (unsigned)(sizeof(uintptr_t) * 8);
  for (unsigned id = 0; id < base->loopThreadLimit; id++) {
    unsigned word = id / wordBits;
    assert(word < base->loopThreadSlotWords);
    uintptr_t bit = (uintptr_t)1 << (id % wordBits);
    // Loop entry is cold, but there is no reason to issue an RMW against
    // every occupied bit. The relaxed prefilter may only cause a harmless
    // extra attempt; the acquire RMW remains the actual claim.
    if (__uintptr_atomic_load(&base->loopThreadSlots[word], amoRelaxed) & bit)
      continue;
    uintptr_t old = __uintptr_atomic_fetch_or(&base->loopThreadSlots[word], bit, amoAcquire);
    if (!(old & bit)) {
      messageLoopThreadId = id;
      loopThreadSlotOwned = 1;
      loopThreadBase = base;
      __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, 1, amoSeqCst);
      return 1;
    }
  }

  assert(0 && "asyncLoop concurrency exceeds createAsyncBase(loopThreads)");
  return 0;
}

unsigned loopThreadExit(asyncBase *base)
{
  assert(loopThreadSlotOwned && "asyncLoop exit without an owned loop slot");
  const unsigned wordBits = (unsigned)(sizeof(uintptr_t) * 8);
  unsigned id = messageLoopThreadId;
  unsigned word = id / wordBits;
  assert(id < base->loopThreadLimit && word < base->loopThreadSlotWords);
  uintptr_t bit = (uintptr_t)1 << (id % wordBits);

  // Publish awake before making the index reusable. The active counter drops
  // while the bit is still owned, so a new entrant cannot be included in the
  // returned quit-propagation count by racing reuse of this exact slot.
  __uintptr_atomic_store(&base->timerSleep[id].wakeTick, UINTPTR_MAX, amoRelaxed);
  unsigned remaining = __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, (unsigned)-1, amoSeqCst) - 1;
  __uintptr_atomic_fetch_and(&base->loopThreadSlots[word], ~bit, amoRelease);
  loopThreadSlotOwned = 0;
  loopThreadBase = 0;
  return remaining;
}

static inline int combinerTaskHandlerCommon(aioObjectRoot *object, uint32_t tag)
{
  // The cancelIo sweep lives in reapObject(), driven by the position of the
  // CANCELIO bit in the captured chain - an eager sweep here would run before
  // the captured submissions are started and let an operation submitted
  // before the cancelIo() call escape it
  if (tag & COMBINER_TAG_DELETE) {
    cancelAllObjectOperations(object);
    // Ownership serializes the only writer. Generation is an identity token,
    // not a publication channel, so a relaxed load/store avoids a locked RMW.
    uintptr_t generation = objectHeaderGeneration(&object->header);
    __uint64_atomic_store(&object->header.tag.high, generation + 1, amoRelaxed);
    if (object->destructorCb)
      object->destructorCb(object, object->destructorCbArg);
    object->destructor(object);
    return 1;
  }

  return 0;
}

// One captured Head entry: the shared DELETE handling runs first - it destroys
// the object and the drain must stop (non-zero return, nothing may touch the
// object afterwards) - otherwise the backend task handler runs the node/tag.
static inline int combinerDispatch(aioObjectRoot *object,
                                   combinerTaskHandlerTy *taskHandler,
                                   asyncOpRoot *op,
                                   uint32_t tag)
{
  if (combinerTaskHandlerCommon(object, tag))
    return 1;
  taskHandler(object, op, tag);
  return 0;
}

void combiner(aioObjectRoot *object, AsyncOpTaggedPtr stackTop, AsyncOpTaggedPtr forRun)
{
  AsyncOpTaggedPtr stubOp = taggedAsyncOpStub();
  combinerTaskHandlerTy *combinerTaskHandler = object->header.base->methodImpl.combinerTaskHandler;

  if (forRun.data) {
    asyncOpRoot *op;
    uint32_t tag;
    taggedAsyncOpDecode(forRun, &op, &tag);
    if (combinerDispatch(object, combinerTaskHandler, op, tag))
      return;
  }

  for (;;) {
    AsyncOpTaggedPtr currentHead;
    while ((currentHead.data = __uint64_atomic_load(&object->header.tag.low, amoRelaxed)) == stackTop.data) {
      // A dying object is swept once more at every ownership-release point:
      // this is the only position that is guaranteed to come after every
      // action of the captured chains, so a submission that slipped past the
      // positional CANCELIO sweep cannot survive the delete and pin the
      // object (sticky DeletePending; the sweep is idempotent, re-runs only
      // cost a re-check)
      if (__uint_atomic_load(&object->DeletePending, amoRelaxed))
        cancelAllObjectOperations(object);
      if (__uint64_atomic_compare_and_swap(&object->header.tag.low, stackTop.data, 0, amoSeqCst))
        return;
    }

    while (!__uint64_atomic_compare_and_swap(&object->header.tag.low, currentHead.data, stackTop.data, amoSeqCst))
      // Still only a CAS expected. The successful capture CAS above acquires
      // the chain before the first next-pointer is read.
      currentHead.data = __uint64_atomic_load(&object->header.tag.low, amoRelaxed);

    // The captured chain is linked newest to oldest (each push points to the
    // previous head); running it as is would invert single-thread submission
    // order. Reverse the operation nodes first. A no-op node (stub or bare
    // counter tag) has no next field, so it can only be the chain tail; it
    // keeps running after the operations pushed on top of it.
    AsyncOpTaggedPtr reversed = taggedAsyncOpNull();
    AsyncOpTaggedPtr tail = taggedAsyncOpNull();
    while (currentHead.data && currentHead.data != stackTop.data) {
      asyncOpRoot *current;
      uint32_t tag;
      taggedAsyncOpDecode(currentHead, &current, &tag);

      if (current == (asyncOpRoot*)stubOp.data || !current) {
        tail = currentHead;
        break;
      }

      AsyncOpTaggedPtr next = current->next;
      current->next = reversed;
      reversed = currentHead;
      currentHead = next;
    }

    // Run dequeued tasks in submission order. The tail (a stub or a bare
    // counter tag with no next field) is the oldest entry of the chain, so
    // its tags run before the operations pushed on top of it - a CANCEL bit
    // OR-ed onto it must not sweep submissions that arrived after the cancel.
    // A DELETE tag destroys the object: nothing may touch it afterwards,
    // including the head CAS of the next loop turn - return immediately.
    // Nothing gets abandoned by that: the tag is pushed on the refcount
    // hitting zero, every not-yet-finished operation holds a reference, so
    // an operation action can never legally sit behind the DELETE tag
    if (tail.data) {
      asyncOpRoot *current;
      uint32_t tag;
      taggedAsyncOpDecode(tail, &current, &tag);
      if (current == (asyncOpRoot*)stubOp.data)
        current = 0;
      if (combinerDispatch(object, combinerTaskHandler, current, tag))
        return;
    }

    while (reversed.data) {
      asyncOpRoot *current;
      uint32_t tag;
      taggedAsyncOpDecode(reversed, &current, &tag);
      reversed = current->next;
      if (combinerDispatch(object, combinerTaskHandler, current, tag))
        return;
    }
  }
}
