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

#define PAGE_MAP_SIZE (1u << 16)

__tls unsigned currentFinishedSync;
__tls unsigned messageLoopThreadId;

ConcurrentQueue asyncOpLinkListPool;

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

static inline uint64_t getPt(uint64_t endTime)
{
  return (endTime / 1000000) + (endTime % 1000000 != 0);
}

static inline void pageMapKeys(uint64_t tm, uint32_t *lo, uint32_t *hi)
{
  uint32_t ltm = (uint32_t)tm;
  *hi = ltm >> 16;
  *lo = ltm & 0xFFFF;
}

static inline void *pageMapAlloc()
{
  return calloc(PAGE_MAP_SIZE, sizeof(void*));
}

void *alignedMalloc(size_t size, size_t alignment)
{
#ifdef OS_COMMONUNIX
  void *memptr;
  return posix_memalign(&memptr, alignment, size) == 0 ? memptr: 0;
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

void pageMapInit(pageMap *map)
{
  map->map = (asyncOpListLink***)pageMapAlloc();
  map->lock = 0;
}

asyncOpListLink *pageMapExtractAll(pageMap *map, uint64_t tm)
{
  uint32_t lo;
  uint32_t hi;
  pageMapKeys(tm, &lo, &hi);
  asyncOpListLink **p1 = (asyncOpListLink**)__pointer_atomic_load(
    (void* volatile*)&map->map[hi], amoAcquire);
  return p1 ? __pointer_atomic_exchange((void* volatile*)&p1[lo], 0) : 0;
}

void pageMapAdd(pageMap *map, asyncOpListLink *link)
{
  uint32_t lo;
  uint32_t hi;
  pageMapKeys(getPt(link->op->endTime), &lo, &hi);
  asyncOpListLink **p1 = (asyncOpListLink**)__pointer_atomic_load(
    (void* volatile*)&map->map[hi], amoAcquire);
  if (!p1) {
    p1 = (asyncOpListLink**)pageMapAlloc();
    if (!__pointer_atomic_compare_and_swap((void* volatile*)&map->map[hi], 0, p1)) {
      free(p1);
      p1 = (asyncOpListLink**)__pointer_atomic_load(
        (void* volatile*)&map->map[hi], amoAcquire);
    }
  }

  asyncOpListLink *current;
  do {
    current = link->next = (asyncOpListLink*)__pointer_atomic_load(
      (void* volatile*)&p1[lo], amoAcquire);
  } while (!__pointer_atomic_compare_and_swap((void* volatile*)&p1[lo], current, link));
}

uintptr_t objectIncrementReference(aioObjectRoot *object, uintptr_t count)
{
  uintptr_t result = __uintptr_atomic_fetch_and_add(&object->refs, count);
  assert(result != 0 && "Removed object access detected");
  return result;
}

uintptr_t objectDecrementReference(aioObjectRoot *object, uintptr_t count)
{
  uintptr_t result = __uintptr_atomic_fetch_and_add(&object->refs, (uintptr_t)0-count);
  assert((intptr_t)result > 0 && "Double object release detected");
  if (result == count)
    combinerPushCounter(object, COMBINER_TAG_DELETE);
  return result;
}

uintptr_t eventIncrementReference(aioUserEvent *event, uintptr_t tag)
{
  return __uintptr_atomic_fetch_and_add(&event->tag, tag);
}

uintptr_t eventDecrementReference(aioUserEvent *event, uintptr_t tag)
{
  uintptr_t result = __uintptr_atomic_fetch_and_add(&event->tag, (uintptr_t)0 - tag) & TAG_EVENT_MASK;
  if (result == (tag & TAG_EVENT_MASK)) {
    if (event->destructorCb)
      event->destructorCb(event, event->destructorCbArg);

    concurrentQueuePush(event->root.objectPool, event);
  }

  return result;
}

int eventTryActivate(aioUserEvent *event)
{
  if (!event->isSemaphore) {
    uintptr_t result = __uintptr_atomic_fetch_and_add(&event->tag, TAG_EVENT_OP + 1);
    if ((result & TAG_EVENT_OP_MASK) == 0) {
      return 1;
    } else {
      __uintptr_atomic_fetch_and_add(&event->tag, STATIC_CAST(uintptr_t, 0)-(TAG_EVENT_OP + 1));
      return 0;
    }
  } else {
    __uintptr_atomic_fetch_and_add(&event->tag, 1);
    return 1;
  }
}

void eventDeactivate(aioUserEvent *event)
{
  if (!event->isSemaphore)
    __uintptr_atomic_fetch_and_add(&event->tag, (uintptr_t)0-TAG_EVENT_OP);
}

void addToTimeoutQueue(asyncBase *base, asyncOpRoot *op)
{
  asyncOpListLink *timerLink = 0;
  if (!objectPoolGet(&asyncOpLinkListPool, (void**)&timerLink, sizeof(asyncOpListLink)))
    timerLink = malloc(sizeof(asyncOpListLink));
  timerLink->op = op;
  timerLink->tag = opGetGeneration(op);
  pageMapAdd(&base->timerMap, timerLink);
  op->timerId = timerLink;
}

// Monotonic seconds for the timeout grid. Deliberately not derived from the
// wall clock (time(0)): a backward NTP/manual step would stall the grid (the
// checkpoint would sit ahead of "now" and the sweep would early-return below),
// and a forward step would fire pending timeouts early. A monotonic source
// advances with true elapsed time regardless of any system-date change. All
// grid sites (arm, sweep, checkpoint init) must share this one clock.
uint64_t getMonotonicSeconds(void)
{
#if defined(OS_WINDOWS)
  return (uint64_t)(GetTickCount64() / 1000ULL);
#else
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec;
#endif
}

void processTimeoutQueue(asyncBase *base, uint64_t currentTime)
{
  // The lock-free fast check races with the update below only formally: a
  // stale (smaller) value just leads to taking the lock and sweeping an
  // empty range, the checkpoint itself is only advanced under the lock
  if (__uintptr_atomic_load(&base->lastCheckPoint, amoRelaxed) >= currentTime ||
      !__spinlock_try_acquire(&base->timerMapLock))
    return;

  // check timeout queue
  uint64_t begin = base->lastCheckPoint;
  for (; begin < currentTime; begin++) {
    asyncOpListLink *link = pageMapExtractAll(&base->timerMap, (uint64_t)begin);
    while (link) {
      asyncOpListLink *next = link->next;
      opCancel(link->op, link->tag, aosTimeout);
      objectPoolPut(&asyncOpLinkListPool, link, sizeof(asyncOpListLink));
      link = next;
    }
  }

  __uintptr_atomic_store(&base->lastCheckPoint, (uintptr_t)currentTime, amoRelaxed);
  __spinlock_release(&base->timerMapLock);
}

void initObjectRoot(aioObjectRoot *object, asyncBase *base, IoObjectTy type, aioObjectDestructor destructor)
{
  object->Head = taggedAsyncOpNull();
  object->readQueue.head = object->readQueue.tail = 0;
  object->writeQueue.head = object->writeQueue.tail = 0;
  object->base = base;
  object->type = type;
  object->refs = 1;
  object->destructor = destructor;
  object->destructorCb = 0;
  object->destructorCbArg = 0;
  object->CancelIoFlag = 0;
  object->DeletePending = 0;
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
  if (__uint_atomic_fetch_and_add(&object->CancelIoFlag, 1) == 0)
    combinerPushCounter(object, COMBINER_TAG_CANCEL);
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
                                          (generation<<TAG_STATUS_SIZE) | aosPending,
                                          (generation<<TAG_STATUS_SIZE) | (uintptr_t)status);
}

void opForceStatus(asyncOpRoot *op, AsyncOpStatus status)
{
  __uintptr_atomic_store(&op->tag,
                         (__uintptr_atomic_load(&op->tag, amoRelaxed) & TAG_GENERATION_MASK) | (uintptr_t)status,
                         amoRelaxed);
}

uintptr_t opEncodeTag(asyncOpRoot *op, uintptr_t tag)
{
  return ((__uintptr_atomic_load(&op->tag, amoRelaxed) >> TAG_STATUS_SIZE) & ~((uintptr_t)TAGGED_POINTER_DATA_MASK)) | (tag & (uintptr_t)TAGGED_POINTER_DATA_MASK);
}

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
    if (isRealTime)
      base->methodImpl.initializeTimer(base, op);
    else
      op->timerId = 0;
    op->tag = 0;
    hasAllocatedNew = 1;
  }

  op->objectPool = buffer;
  *result = op;
  return hasAllocatedNew;
}

void releaseAsyncOp(asyncOpRoot *op)
{
  aioObjectRoot *object = op->object;
  concurrentQueuePush(op->objectPool, op);
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
  // Atomic store: a stale timeout-grid link may concurrently CAS the tag of
  // this recycled storage; bumping the generation is what invalidates it
  __uintptr_atomic_store(&op->tag, ((opGetGeneration(op)+1) << TAG_STATUS_SIZE) | aosPending, amoRelaxed);
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
  op->timeout = timeout;
  op->running = (flags & afRunning) ? arRunning : arWaiting;
  objectIncrementReference(object, 1);
}

static void opArmTimer(asyncOpRoot *op)
{
  if (op->timeout) {
    asyncBase *base = op->object->base;
    if (op->flags & afRealtime) {
      // start timer for this operation
      base->methodImpl.startTimer(op);
    } else {
      // add operation to timeout grid
      op->endTime = getMonotonicSeconds() * 1000000ULL + op->timeout;
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
    case ciaNone:
      return;

    case ciaRelease:
      // Either a proactor completion arrived after cancellation, or a child
      // completion made a composite handshake terminal. In both cases the I/O
      // which owned the slot is done; do not re-run executeMethod.
      initializationRelease(op, status, needStart);
      return;

    case ciaExecute:
      initializationTryComplete(op, op->executeMethod(op), needStart);
      return;
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

// Reap terminal operations from one queue positionally: release the ones with no
// I/O in flight, hold an in-flight op (cancelMethod == 0) on its head until its
// late completion. Pending operations are left untouched. The queue is rebuilt
// from survivors; if the new head is a ready-to-start operation, ask for a kick.
static void reapQueue(List *list, uint32_t tag, uint32_t *needStart)
{
  asyncOpRoot *op = list->head;
  asyncOpRoot *keptHead = 0, *keptTail = 0;
  while (op) {
    asyncOpRoot *next = op->executeQueue.next;
    int keep = 0, release = 0;
    AsyncOpStatus status = opGetStatus(op);
    switch (combinerSelectReapAction(op->running, status)) {
      case craKeep:
        keep = 1;
        break;

      case craCancel:
        op->running = arCancelling;
        if (op->cancelMethod(op))
          release = 1;
        else
          keep = 1;   // in-flight: hold positional for the late completion
        break;

      case craRelease:
        release = 1;
        break;
    }

    op->executeQueue.prev = op->executeQueue.next = 0;
    if (keep) {
      op->executeQueue.prev = keptTail;
      if (keptTail)
        keptTail->executeQueue.next = op;
      else
        keptHead = op;
      keptTail = op;
    } else if (release) {
      opRelease(op, status, 0);
    }
    op = next;
  }

  list->head = keptHead;
  list->tail = keptTail;
  if (keptHead && keptHead->running == arWaiting)
    *needStart |= tag;
}

// CANCEL reconcile: a cancel source (timeout/opCancel/cancelIo) has already set
// the terminal status (winner-takes) and asked the combiner to scan. Reap the
// terminals it left behind, positionally, across the initialization slot and both
// queues.
void reapObject(aioObjectRoot *object, uint32_t *needStart)
{
  asyncOpRoot *ex = (asyncOpRoot*)__uintptr_atomic_load(&object->initializationOp, amoRelaxed);
  if (ex) {
    AsyncOpStatus status = opGetStatus(ex);
    switch (combinerSelectReapAction(ex->running, status)) {
      case craCancel:
        ex->running = arCancelling;
        if (ex->cancelMethod(ex))
          initializationRelease(ex, status, needStart);
        break;

      case craRelease:
        initializationRelease(ex, status, needStart);
        break;

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
  if (op->timerId && status != aosTimeout) {
    if (op->flags & afRealtime)
      op->object->base->methodImpl.stopTimer(op);
  }

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
  // Positional-aware bulk cancel (CancelIoFlag / DeletePending / disconnect /
  // connect-fail cascade). An in-flight operation whose cancelMethod returns 0
  // (a proactor abort request the kernel owns until its completion arrives) must
  // stay on its head so the late positional PROGRESS_* signal still finds and
  // releases it - dropping it would strand the operation and, on objectDelete,
  // leak the object whose reference it holds. By the one-in-flight invariant at
  // most the head survives; the queue is rebuilt from the survivors rather than
  // wiped.
  asyncOpRoot *op = list->head;
  asyncOpRoot *keptHead = 0, *keptTail = 0;
  while (op) {
    asyncOpRoot *next = op->executeQueue.next;
    int keep = 0, release = 0;
    if (opSetStatus(op, opGetGeneration(op), status)) {
      if (op->running == arRunning) {
        op->running = arCancelling;
        if (op->cancelMethod(op))
          release = 1;
        else
          keep = 1;   // in-flight: hold positional for the late completion
      } else {
        release = 1;  // queued, not started: no I/O in flight, release now
      }
    } else {
      // Status race lost: the winner (a concurrent completion/timeout) drives
      // the release through its own reconcile scan. Leave the operation in the
      // queue - the signal no longer carries the op pointer, so the queue is the
      // only way that scan can still find it.
      keep = 1;
    }

    op->executeQueue.prev = op->executeQueue.next = 0;
    if (keep) {
      op->executeQueue.prev = keptTail;
      if (keptTail)
        keptTail->executeQueue.next = op;
      else
        keptHead = op;
      keptTail = op;
    } else if (release) {
      opRelease(op, status, 0);
    }
    op = next;
  }

  list->head = keptHead;
  list->tail = keptTail;
}

void opCancel(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status)
{
  // Cancel signal, gated on winning the terminal status (the loser is redundant,
  // the winner drives the cancel). The combiner scans and reaps positionally;
  // an in-flight op stays on its head until its late completion releases it.
  if (opSetStatus(op, generation, status))
    combinerPushCounter(op->object, COMBINER_TAG_CANCEL);
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
  op->object->base->methodImpl.enqueue(op->object->base, op);
}

int executeGlobalQueue(asyncBase *base)
{
  asyncOpRoot *op;
  while (concurrentQueuePop(&base->globalQueue, (void**)&op)) {
    if (!op)
      return 0;

    switch (op->opCode) {
      case actUserEvent : {
        aioUserEvent *event = (aioUserEvent*)op;
        eventDeactivate(event);
        op->finishMethod(op);
        break;
      }

      default : {
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
  }

  return 1;
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


void graceRetire(asyncBase *base, aioObjectRoot *object, aioObjectDestructor *memoryRelease)
{
  // The resource half of the destructor already ran; the field is dead from
  // here on and repurposed to carry the memory half to graceReclaim.
  // The object is parked unconditionally: with graceFrozen set reclamation
  // never runs and the limbo only grows. A leak in a pathological thread
  // configuration is the safe degradation - an immediate release here would
  // be the original use-after-free while unslotted loop threads still hold
  // harvested batches
  object->destructor = memoryRelease;
  __spinlock_acquire(&base->graceLimboLock);
  // The write is serialized by the lock, but loop threads read the epoch
  // concurrently; the full-barrier RMW also keeps the epoch advance ordered
  // after the object's logical detachment from the backend
  object->GraceEpoch = __uintptr_atomic_fetch_and_add(&base->graceEpoch, 1) + 1;
  // The list head is also read lock-free by the fast checks in graceReclaim
  // and graceQuiesce, so its writes must be atomic; relaxed is enough, the
  // readers only test it against NULL before taking this very lock
  object->GraceNext = base->graceLimbo;
  __pointer_atomic_store((void *volatile*)&base->graceLimbo, object, amoRelaxed);
  __spinlock_release(&base->graceLimboLock);
}

void graceReclaim(asyncBase *base)
{
  if (!__pointer_atomic_load((void *volatile*)&base->graceLimbo, amoRelaxed) ||
      __uint_atomic_load(&base->graceFrozen, amoRelaxed))
    return;

  // The scan runs under the limbo lock: a thread claims its slot before its
  // first kernel wait, and the retirement of any object its batches could
  // reference passes through this very lock afterwards - a scan ordered
  // after that retirement therefore cannot read the slot as unclaimed.
  // Slots above the high-water mark were never claimed and gate nothing
  aioObjectRoot *ripe;
  __spinlock_acquire(&base->graceLimboLock);
  uintptr_t minSeen = UINTPTR_MAX;
  unsigned slots = __uint_atomic_load(&base->graceSlotCount, amoAcquire);
  unsigned i;
  for (i = 0; i < slots; i++) {
    uintptr_t seen = __uintptr_atomic_load(&base->graceSeen[i].seen, amoAcquire);
    if (seen < minSeen)
      minSeen = seen;
  }

  // Epochs decrease along the list: everything past the first ripe object
  // is ripe as well. Release outside the lock - pool pushes and frees do
  // not need it, and a concurrent retire must not wait on them
  aioObjectRoot *prev = 0;
  aioObjectRoot *cur = base->graceLimbo;
  while (cur && cur->GraceEpoch > minSeen) {
    prev = cur;
    cur = cur->GraceNext;
  }
  ripe = cur;
  if (prev)
    prev->GraceNext = 0;
  else
    __pointer_atomic_store((void *volatile*)&base->graceLimbo, 0, amoRelaxed);
  __spinlock_release(&base->graceLimboLock);

  while (ripe) {
    aioObjectRoot *next = ripe->GraceNext;
    ripe->destructor(ripe);
    ripe = next;
  }
}

void graceThreadEnter(asyncBase *base)
{
  // Freezing reclamation is the memory-safe degradation for thread
  // configurations the slots cannot describe: dead objects then stay in
  // the limbo for the rest of the base's life
  if (messageLoopThreadId >= GRACE_LOOP_THREAD_LIMIT) {
    __uint_atomic_store(&base->graceFrozen, 1, amoRelaxed);
    return;
  }

  // Claim the slot for this thread's lifetime in the loop; entry is a
  // quiescent point, so the current epoch is a valid first stamp. A failed
  // claim means the id was adopted while its previous holder is still
  // inside the message loop (a loop thread started while others were
  // shutting down): two threads sharing a slot could mask each other's
  // batches, so reclamation shuts down. A cleanly exited holder leaves
  // UINTPTR_MAX behind - plain quit-then-restart cycles and mid-run loop
  // thread additions claim successfully. The claim must happen right at
  // entry: a check without a reservation would leave a window between the
  // id assignment and the first stamp where the same id handed out again
  // goes undetected
  uintptr_t epoch = __uintptr_atomic_fetch_and_add(&base->graceEpoch, 0);
  if (!__uintptr_atomic_compare_and_swap(&base->graceSeen[messageLoopThreadId].seen, UINTPTR_MAX, epoch)) {
    __uint_atomic_store(&base->graceFrozen, 1, amoRelaxed);
    return;
  }

  // High-water mark of claimed slots bounds the reclaim scan, so the slot
  // array can afford a generous limit. The claim above and this bump are
  // both ordered before the thread's first kernel wait
  unsigned needed = messageLoopThreadId + 1;
  unsigned current = __uint_atomic_load(&base->graceSlotCount, amoRelaxed);
  while (current < needed && !__uint_atomic_compare_and_swap(&base->graceSlotCount, current, needed))
    current = __uint_atomic_load(&base->graceSlotCount, amoRelaxed);
}

void graceQuiesce(asyncBase *base)
{
  if (messageLoopThreadId >= GRACE_LOOP_THREAD_LIMIT)
    return;

  // Publishing the stamp with release orders all processing of the preceding
  // kernel batch before the reclaimer's acquire load. graceEpoch is only a
  // sequence token: a relaxed load is sufficient, and a stale value merely
  // delays reclamation
  __uintptr_atomic_store(&base->graceSeen[messageLoopThreadId].seen,
                         __uintptr_atomic_load(&base->graceEpoch, amoRelaxed),
                         amoRelease);
  if (__pointer_atomic_load((void *volatile*)&base->graceLimbo, amoRelaxed))
    graceReclaim(base);
}

static inline int combinerTaskHandlerCommon(aioObjectRoot *object, uint32_t tag)
{
  // Cheap relaxed load keeps the hot path free of RMW; the exchange runs only
  // when a cancel is actually pending. Consuming the counter before the sweep
  // (not after) means a cancelIo racing with the sweep sees 0, increments and
  // pushes a fresh CANCEL pass - a coalesced request cannot be lost
  if (__uint_atomic_load(&object->CancelIoFlag, amoRelaxed) != 0 &&
      __uint_atomic_exchange(&object->CancelIoFlag, 0) != 0) {
    cancelInitializationOp(object, aosCanceled);
    cancelOperationList(&object->readQueue, aosCanceled);
    cancelOperationList(&object->writeQueue, aosCanceled);
  }

  if (tag & COMBINER_TAG_DELETE) {
    cancelInitializationOp(object, aosCanceled);
    cancelOperationList(&object->readQueue, aosCanceled);
    cancelOperationList(&object->writeQueue, aosCanceled);
    if (object->destructorCb)
      object->destructorCb(object, object->destructorCbArg);
    object->destructor(object);
    return 1;
  }

  return 0;
}

void combiner(aioObjectRoot *object, AsyncOpTaggedPtr stackTop, AsyncOpTaggedPtr forRun)
{
  AsyncOpTaggedPtr stubOp = taggedAsyncOpStub();
  combinerTaskHandlerTy *combinerTaskHandler = object->base->methodImpl.combinerTaskHandler;

  if (forRun.data) {
    asyncOpRoot *op;
    uint32_t tag;
    taggedAsyncOpDecode(forRun, &op, &tag);
    if (combinerTaskHandlerCommon(object, tag))
      return;
    combinerTaskHandler(object, op, tag);
  }

  for (;;) {
    AsyncOpTaggedPtr currentHead;
    while ( (currentHead.data = __uintptr_atomic_load(&object->Head.data, amoAcquire)) == stackTop.data ) {
      // A dying object is swept once more at every ownership-release point:
      // this is the only position that is guaranteed to come after every
      // action of the captured chains, so a submission that slipped past the
      // CancelIoFlag pass cannot survive the delete and pin the object
      // (sticky flag; the sweep is idempotent, re-runs only cost a re-check)
      if (__uint_atomic_load(&object->DeletePending, amoRelaxed)) {
        cancelInitializationOp(object, aosCanceled);
        cancelOperationList(&object->readQueue, aosCanceled);
        cancelOperationList(&object->writeQueue, aosCanceled);
      }
      if (__uintptr_atomic_compare_and_swap(&object->Head.data, stackTop.data, 0))
        return;
    }

    while (!__uintptr_atomic_compare_and_swap(&object->Head.data, currentHead.data, stackTop.data))
      currentHead.data = __uintptr_atomic_load(&object->Head.data, amoAcquire);

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

    // Run dequeued tasks in submission order.
    // A DELETE tag destroys the object: nothing may touch it afterwards,
    // including the head CAS of the next loop turn - return immediately.
    // Nothing gets abandoned by that: the tag is pushed on the refcount
    // hitting zero, every not-yet-finished operation holds a reference, so
    // an operation action can never legally sit behind the DELETE tag
    while (reversed.data) {
      asyncOpRoot *current;
      uint32_t tag;
      taggedAsyncOpDecode(reversed, &current, &tag);
      reversed = current->next;
      if (combinerTaskHandlerCommon(object, tag))
        return;
      combinerTaskHandler(object, current, tag);
    }

    if (tail.data) {
      asyncOpRoot *current;
      uint32_t tag;
      taggedAsyncOpDecode(tail, &current, &tag);
      if (current == (asyncOpRoot*)stubOp.data)
        current = 0;
      if (combinerTaskHandlerCommon(object, tag))
        return;
      combinerTaskHandler(object, current, tag);
    }
  }
}
