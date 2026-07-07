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
  asyncOpListLink **p1 = map->map[hi];
  return p1 ? __pointer_atomic_exchange((void* volatile*)&p1[lo], 0) : 0;
}

void pageMapAdd(pageMap *map, asyncOpListLink *link)
{
  uint32_t lo;
  uint32_t hi;
  pageMapKeys(getPt(link->op->endTime), &lo, &hi);
  asyncOpListLink **p1 = map->map[hi];
  if (!p1) {
    p1 = (asyncOpListLink**)pageMapAlloc();
    if (!__pointer_atomic_compare_and_swap((void* volatile*)&map->map[hi], 0, p1)) {
      free(p1);
      p1 = map->map[hi];
    }
  }

  asyncOpListLink *current;
  do {
    current = link->next = p1[lo];
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
  if (!objectPoolGet(&asyncOpLinkListPool, (void**)&timerLink))
    timerLink = malloc(sizeof(asyncOpListLink));
  timerLink->op = op;
  timerLink->tag = opGetGeneration(op);
  pageMapAdd(&base->timerMap, timerLink);
  op->timerId = timerLink;
}

void processTimeoutQueue(asyncBase *base, time_t currentTime)
{
  // TODO: handle system date change
  if (base->lastCheckPoint >= currentTime || !__spinlock_try_acquire(&base->timerMapLock))
    return;

  // check timeout queue
  time_t begin = base->lastCheckPoint;
  for (; begin < currentTime; begin++) {
    asyncOpListLink *link = pageMapExtractAll(&base->timerMap, (uint64_t)begin);
    while (link) {
      asyncOpListLink *next = link->next;
      opCancel(link->op, link->tag, aosTimeout);
      if (!objectPoolPut(&asyncOpLinkListPool, link))
        free(link);
      link = next;
    }
  }

  base->lastCheckPoint = currentTime;
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
  object->exclusiveOp = 0;
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
    combinerPushCounter(object, COMBINER_TAG_ACCESS);
}

void objectDelete(aioObjectRoot *object)
{
  // Before the cancelIo push: the pass it triggers must already see the flag
  object->DeletePending = 1;
  cancelIo(object);
  objectDecrementReference(object, 1);
}

uintptr_t opGetGeneration(asyncOpRoot *op)
{
  return op->tag >> TAG_STATUS_SIZE;
}

AsyncOpStatus opGetStatus(asyncOpRoot *op)
{
  return op->tag & TAG_STATUS_MASK;
}

int opSetStatus(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status)
{
  return __uintptr_atomic_compare_and_swap(&op->tag,
                                          (generation<<TAG_STATUS_SIZE) | aosPending,
                                          (generation<<TAG_STATUS_SIZE) | (uintptr_t)status);
}

void opForceStatus(asyncOpRoot *op, AsyncOpStatus status)
{
  op->tag = (op->tag & TAG_GENERATION_MASK) | (uintptr_t)status;
}

uintptr_t opEncodeTag(asyncOpRoot *op, uintptr_t tag)
{
  return ((op->tag >> TAG_STATUS_SIZE) & ~((uintptr_t)TAGGED_POINTER_DATA_MASK)) | (tag & (uintptr_t)TAGGED_POINTER_DATA_MASK);
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
  op->tag = ((opGetGeneration(op)+1) << TAG_STATUS_SIZE) | aosPending;
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
      op->endTime = ((uint64_t)time(0)) * 1000000ULL + op->timeout;
      addToTimeoutQueue(base, op);
    }
  }
}

static void opRun(asyncOpRoot *op, List *list)
{
  eqPushBack(list, op);
  opArmTimer(op);
}

// An exclusive operation (connect) does not live in the read/write queues:
// it occupies the object's exclusiveOp slot, claimed by CAS at submission
// time — the ownership itself routes it to the exclusive path, there is no
// dedicated flag. Both queues do not start operations while the slot is
// busy. Leaving the slot kicks both queues; leaving it with a non-success
// status also cancels everything queued behind the exclusive with that
// status.
static void exclusiveRelease(asyncOpRoot *op, AsyncOpStatus status, uint32_t *needStart)
{
  aioObjectRoot *object = op->object;
  if (object->exclusiveOp == (uintptr_t)op)
    object->exclusiveOp = 0;
  opRelease(op, status, 0);
  if (status != aosSuccess) {
    cancelOperationList(&object->readQueue, status);
    cancelOperationList(&object->writeQueue, status);
  }
  if (needStart)
    *needStart |= IO_EVENT_READ | IO_EVENT_WRITE;
}

static void exclusiveTryComplete(asyncOpRoot *op, AsyncOpStatus status, uint32_t *needStart)
{
  if (status == aosPending) {
    op->running = arRunning;
    return;
  }

  // On a lost status race (operation timed out concurrently) the status
  // winner has pushed aaCancel, which performs the release. The state
  // machine has already consumed its child operations, so a cancelMethod
  // that aborts children (SSL/zmtp connect) would find nothing and nobody
  // would ever finish the operation - mark it not running so the pending
  // aaCancel releases it directly instead of cancelling
  if (opSetStatus(op, opGetGeneration(op), status))
    exclusiveRelease(op, status, needStart);
  else
    op->running = arCancelling;
}

// Re-drive a parked exclusive operation from a backend event handler
void processExclusiveOp(aioObjectRoot *object, uint32_t *needStart)
{
  asyncOpRoot *op = (asyncOpRoot*)object->exclusiveOp;
  if (!op || op->running != arRunning)
    return;
  exclusiveTryComplete(op, op->executeMethod(op), needStart);
}

static void cancelExclusiveOp(aioObjectRoot *object, AsyncOpStatus status)
{
  asyncOpRoot *op = (asyncOpRoot*)object->exclusiveOp;
  if (!op)
    return;
  if (opSetStatus(op, opGetGeneration(op), status)) {
    if (op->running == arRunning) {
      op->running = arCancelling;
      if (op->cancelMethod(op))
        exclusiveRelease(op, status, 0);
    }
    // arWaiting: aaStart is still queued in the combiner; it will observe the
    // status and release the slot
  }
}

void processAction(asyncOpRoot *opptr, AsyncOpActionTy actionType, uint32_t *needStart)
{
  List *list = 0;
  uint32_t tag = 0;
  aioObjectRoot *object = opptr->object;

  // Ownership of the exclusive slot (claimed by CAS at submission, e.g. in
  // aioConnect) is what routes an operation to the exclusive path; there is
  // no flag to spoof, any other operation goes to its read/write queue
  if (object->exclusiveOp == (uintptr_t)opptr) {
    switch (actionType) {
      case aaStart : {
        if (opGetStatus(opptr) != aosPending) {
          // cancelled between submission and start (cancelIo sweep); the
          // sweep does not push aaCancel for a not yet started operation,
          // releasing is on us
          exclusiveRelease(opptr, opGetStatus(opptr), needStart);
          break;
        }
        opArmTimer(opptr);
        exclusiveTryComplete(opptr, opptr->executeMethod(opptr), needStart);
        break;
      }

      case aaCancel : {
        if (opptr->running == arRunning) {
          opptr->running = arCancelling;
          if (opptr->cancelMethod(opptr))
            exclusiveRelease(opptr, opGetStatus(opptr), needStart);
        } else {
          exclusiveRelease(opptr, opGetStatus(opptr), needStart);
        }
        break;
      }

      case aaFinish : {
        exclusiveRelease(opptr, opGetStatus(opptr), needStart);
        break;
      }

      case aaContinue : {
        if (opptr->running == arRunning)
          exclusiveTryComplete(opptr, opptr->executeMethod(opptr), needStart);
        else
          exclusiveRelease(opptr, opGetStatus(opptr), needStart);
        break;
      }

      default :
        break;
    }

    return;
  }

  if (opptr->opCode & OPCODE_WRITE) {
    list = &object->writeQueue;
    tag = IO_EVENT_WRITE;
  } else {
    list = &object->readQueue;
    tag = IO_EVENT_READ;
  }

  switch (actionType) {
    case aaStart : {
      opRun(opptr, list);
      break;
    }

    case aaCancel : {
      if (opptr->running == arRunning) {
        opptr->running = arCancelling;
        if (opptr->cancelMethod(opptr))
          opRelease(opptr, opGetStatus(opptr), list);
      } else {
        opRelease(opptr, opGetStatus(opptr), list);
      }
      break;
    }

    case aaFinish : {
      opRelease(opptr, opGetStatus(opptr), list);
      break;
    }

    case aaContinue : {
      if (opptr->running == arRunning)
        opptr->running = arWaiting;
      else
        opRelease(opptr, opGetStatus(opptr), list);
      break;
    }

    default :
      break;
  }

  *needStart |= (list->head && list->head->running == arWaiting) ? tag : 0;
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
  // Both queues are frozen while an exclusive operation (connect) is in
  // flight; they are kicked when it leaves the slot
  if (op && op->object->exclusiveOp)
    return;

  while (op) {
    asyncOpRoot *next = op->executeQueue.next;
    AsyncOpStatus status = op->executeMethod(op);
    if (status == aosPending) {
      op->running = arRunning;
      break;
    }

    if (opSetStatus(op, opGetGeneration(op), status)) {
      opRelease(op, status, 0);
    } else {
      op->executeQueue.prev = op->executeQueue.next = 0;
    }
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
  asyncOpRoot *op = list->head;
  while (op) {
    asyncOpRoot *next = op->executeQueue.next;
    if (opSetStatus(op, opGetGeneration(op), status)) {
      if (op->running == arRunning) {
        op->running = arCancelling;
        if (op->cancelMethod(op))
          opRelease(op, opGetStatus(op), list);
        else
          // Waits for the backend cancel completion (iocp abort packet);
          // detach cleanly so the completion's release does not unlink a
          // wiped list through stale pointers
          eqRemove(list, op);
      } else {
        opRelease(op, opGetStatus(op), list);
      }
    } else {
      // Status race lost: the winner releases the operation, this walker
      // only detaches it so the list stays consistent for both parties
      eqRemove(list, op);
    }
    op = next;
  }

  list->head = 0;
  list->tail = 0;
}

void opCancel(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status)
{
  if (opSetStatus(op, generation, status))
    combinerPushOperation(op, aaCancel);
}

void resumeParent(asyncOpRoot *op, AsyncOpStatus status)
{
  if (status == aosSuccess) {
    combinerPushOperation(op, aaContinue);
  } else {
    opSetStatus(op, opGetGeneration(op), status);
    combinerPushOperation(op, aaFinish);
  }
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


static inline int combinerTaskHandlerCommon(aioObjectRoot *object, uint32_t tag)
{
  if (object->CancelIoFlag) {
    object->CancelIoFlag = 0;
    cancelExclusiveOp(object, aosCanceled);
    cancelOperationList(&object->readQueue, aosCanceled);
    cancelOperationList(&object->writeQueue, aosCanceled);
  }

  if (tag & COMBINER_TAG_DELETE) {
    cancelExclusiveOp(object, aosCanceled);
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
    AsyncOpActionTy opMethod;
    uint32_t tag;
    taggedAsyncOpDecode(forRun, &op, &opMethod, &tag);
    if (combinerTaskHandlerCommon(object, tag))
      return;
    combinerTaskHandler(object, op, opMethod);
  }

  for (;;) {
    AsyncOpTaggedPtr currentHead;
    while ( (currentHead.data = object->Head.data) == stackTop.data ) {
      // A dying object is swept once more at every ownership-release point:
      // this is the only position that is guaranteed to come after every
      // action of the captured chains, so a submission that slipped past the
      // CancelIoFlag pass cannot survive the delete and pin the object
      // (sticky flag; the sweep is idempotent, re-runs only cost a re-check)
      if (object->DeletePending) {
        cancelExclusiveOp(object, aosCanceled);
        cancelOperationList(&object->readQueue, aosCanceled);
        cancelOperationList(&object->writeQueue, aosCanceled);
      }
      if (__uintptr_atomic_compare_and_swap(&object->Head.data, stackTop.data, 0))
        return;
    }

    while (!__uintptr_atomic_compare_and_swap(&object->Head.data, currentHead.data, stackTop.data))
      currentHead = object->Head;

    // The captured chain is linked newest to oldest (each push points to the
    // previous head); running it as is would invert single-thread submission
    // order. Reverse the operation nodes first. A no-op node (stub or bare
    // counter tag) has no next field, so it can only be the chain tail; it
    // keeps running after the operations pushed on top of it.
    AsyncOpTaggedPtr reversed = taggedAsyncOpNull();
    AsyncOpTaggedPtr tail = taggedAsyncOpNull();
    while (currentHead.data && currentHead.data != stackTop.data) {
      asyncOpRoot *current;
      AsyncOpActionTy opMethod;
      uint32_t tag;
      taggedAsyncOpDecode(currentHead, &current, &opMethod, &tag);

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
      AsyncOpActionTy opMethod;
      uint32_t tag;
      taggedAsyncOpDecode(reversed, &current, &opMethod, &tag);
      reversed = current->next;
      if (combinerTaskHandlerCommon(object, tag))
        return;
      combinerTaskHandler(object, current, opMethod);
    }

    if (tail.data) {
      asyncOpRoot *current;
      AsyncOpActionTy opMethod;
      uint32_t tag;
      taggedAsyncOpDecode(tail, &current, &opMethod, &tag);
      if (current == (asyncOpRoot*)stubOp.data)
        current = 0;
      if (combinerTaskHandlerCommon(object, tag))
        return;
      combinerTaskHandler(object, current, opMethod);
    }
  }
}
