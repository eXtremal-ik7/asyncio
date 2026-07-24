#ifndef __ASYNCIO_ASYNCIOIMPL_H_
#define __ASYNCIO_ASYNCIOIMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include "events.h"
#include "lifetime.h"
#include "timerWheel.h"
#include <assert.h>
#include <stdint.h>

#define MAX_SYNCHRONOUS_FINISHED_OPERATION 32

typedef enum AsyncOpRunningTy {
  arWaiting = 0,
  arRunning,
  arCancelling
} AsyncOpRunningTy;

typedef struct List {
  asyncOpRoot *head;
  asyncOpRoot *tail;
} List;

typedef struct AsyncOpTaggedPtr {
  uintptr_t data;
} AsyncOpTaggedPtr;

typedef struct ListImpl {
  asyncOpRoot *prev;
  asyncOpRoot *next;
} ListImpl;

#define TAG_STATUS_SIZE 8
#define TAG_STATUS_MASK ((STATIC_CAST(uintptr_t, 1) << TAG_STATUS_SIZE) - 1)
#define TAG_GENERATION_MASK (~TAG_STATUS_MASK)

#define COMBINER_TAG_SIZE 6
#define COMBINER_TAG_PROGRESS_READ (1u << 0)
#define COMBINER_TAG_PROGRESS_WRITE (1u << 1)
#define COMBINER_TAG_ERROR (1u << 2)
#define COMBINER_TAG_CANCEL (1u << 3)
#define COMBINER_TAG_DELETE (1u << 4)
#define COMBINER_TAG_CANCELIO (1u << 5)
#define COMBINER_TAG_PROGRESS_MASK (COMBINER_TAG_PROGRESS_READ | COMBINER_TAG_PROGRESS_WRITE)

#define IO_EVENT_READ 1u
#define IO_EVENT_WRITE 2u
#define IO_EVENT_ERROR 4u

#define OPCODE_READ 0
#define OPCODE_WRITE 0x01000000
#define OPCODE_OTHER 0x02000000
#define OPCODE_INIT 0x04000000

struct aioObjectRoot {
  objectHeader header;
  uintptr_t refs;
  List readQueue;
  List writeQueue;
  volatile uint32_t DeletePending;
  asyncOpRoot *initializationOp;
  aioObjectDestructor *destructor;
  aioObjectDestructorCb *destructorCb;
  void *destructorCbArg;
};

struct asyncOpRoot {
  volatile uintptr_t tag;
  ConcurrentQueue *objectPool;
  aioExecuteProc *executeMethod;
  aioCancelProc *cancelMethod;
  aioFinishProc *finishMethod;
  aioReleaseProc *releaseMethod;
  ListImpl executeQueue;
  AsyncOpTaggedPtr next;
  aioObjectRoot *object;
  void *callback;
  void *arg;
  int opCode;
  AsyncFlags flags;
  void *timerId;
  union {
    uint64_t timeout;
    uint64_t deadlineTick;
    uint64_t endTime;
  };
  AsyncOpRunningTy running;
};

#ifdef __cplusplus
static_assert(offsetof(aioObjectRoot, header) == 0, "header must start aioObjectRoot");
static_assert(offsetof(asyncOpRoot, tag) == 0, "tag must start asyncOpRoot");
#elif defined(_MSC_VER) && !defined(__clang__)
typedef char headerMustStartRoot[offsetof(aioObjectRoot, header) == 0 ? 1 : -1];
typedef char tagMustStartOpRoot[offsetof(asyncOpRoot, tag) == 0 ? 1 : -1];
#else
_Static_assert(offsetof(aioObjectRoot, header) == 0, "header must start aioObjectRoot");
_Static_assert(offsetof(asyncOpRoot, tag) == 0, "tag must start asyncOpRoot");
#endif

typedef asyncOpRoot *newAsyncOpTy(asyncBase*, int, ConcurrentQueue*, ConcurrentQueue*);
typedef void initializeTimerTy(asyncBase*, asyncOpRoot*);
typedef void combinerTaskHandlerTy(aioObjectRoot*, asyncOpRoot*, uint32_t);
typedef void enqueueOperationTy(asyncBase*, asyncOpRoot*);
typedef void loopWakeupTy(asyncBase*);
typedef void nextFinishedOperationTy(asyncBase*);
typedef aioObject *newAioObjectTy(asyncBase*, IoObjectTy, void*);
typedef void startTimerTy(asyncOpRoot*);
typedef void stopTimerTy(asyncOpRoot*);
typedef int initializeUserEventTy(aioUserEvent*);
typedef int activateTy(aioUserEvent*);
typedef int updateEventTimerTy(aioUserEvent*, EventTimerUpdate, uint32_t, uint64_t);
typedef uint64_t consumeEventTimerTickTy(aioUserEvent*, uint64_t, uint32_t, uint64_t);
typedef void releaseUserEventTy(aioUserEvent*);

struct asyncImpl {
  combinerTaskHandlerTy *combinerTaskHandler;
  enqueueOperationTy *enqueue;
  nextFinishedOperationTy *nextFinishedOperation;
  newAioObjectTy *newAioObject;
  newAsyncOpTy *newAsyncOp;
  aioCancelProc *cancelAsyncOp;
  initializeTimerTy *initializeTimer;
  startTimerTy *startTimer;
  stopTimerTy *stopTimer;
  initializeUserEventTy *initializeUserEvent;
  activateTy *activate;
  aioExecuteProc *connect;
  aioExecuteProc *accept;
  aioExecuteProc *read;
  aioExecuteProc *write;
  aioExecuteProc *readMsg;
  aioExecuteProc *writeMsg;
  loopWakeupTy *wakeupLoop;
  updateEventTimerTy *updateEventTimer;
  consumeEventTimerTickTy *consumeEventTimerTick;
  releaseUserEventTy *releaseUserEvent;
};

struct asyncBase {
  struct asyncImpl methodImpl;
  ConcurrentQueue globalQueue;
  ConcurrentQueue eventPool;
  timerWheel timerWheel;
  uint8_t timerCloseCursorPadBefore[CACHE_LINE_SIZE - sizeof(uintptr_t)];
  uintptr_t timerCloseCursor;
  uint8_t timerCloseCursorPadAfter[CACHE_LINE_SIZE - sizeof(uintptr_t)];
  TimerLoopState *timerLoopStates;
  uintptr_t *loopThreadSlots;
  unsigned loopThreadSlotWords;
  unsigned loopThreadLimit;
  volatile unsigned messageLoopThreadCounter;
  volatile uintptr_t timerPreclearOverflow;
  uintptr_t quitRequested;
#ifndef NDEBUG
  int opsCount;
#endif
};

extern __tls unsigned currentFinishedSync;
extern __tls unsigned messageLoopThreadId;
extern __tls asyncBase *loopThreadBase;

void eqRemove(List *list, asyncOpRoot *op);
void eqPushBack(List *list, asyncOpRoot *op);

uintptr_t opGetGeneration(asyncOpRoot *op);
AsyncOpStatus opGetStatus(asyncOpRoot *op);
int opSetStatus(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status);
void opForceStatus(asyncOpRoot *op, AsyncOpStatus status);

void opRelease(asyncOpRoot *op, AsyncOpStatus status, List *executeList);
void startOperation(asyncOpRoot *op, uint32_t *needStart);
void reapObject(aioObjectRoot *object, uint32_t tag, uint32_t *needStart);
void processInitializationOp(aioObjectRoot *object, uint32_t *needStart);
void executeOperationList(List *list);
void cancelOperationList(List *list, AsyncOpStatus status);
int opCancel(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status, aioObjectRoot *object, uintptr_t objectGeneration);
void resumeParent(asyncOpRoot *op, AsyncOpStatus status);
void addToGlobalQueue(asyncOpRoot *op);
void executeGlobalQueue(asyncBase *base);

typedef asyncOpRoot *CreateAsyncOpProc(aioObjectRoot*, AsyncFlags, uint64_t, void*, void*, int, void*);
typedef asyncOpRoot *SyncImplProc(aioObjectRoot*, AsyncFlags, uint64_t, void*, void*, void*);
typedef void MakeResultProc(void*);
typedef void InitOpProc(asyncOpRoot*, void*);

void combiner(aioObjectRoot *object, AsyncOpTaggedPtr stackTop, AsyncOpTaggedPtr forRun);

static inline AsyncOpTaggedPtr taggedAsyncOpNull(void)
{
  AsyncOpTaggedPtr result = {0};
  return result;
}

static inline AsyncOpTaggedPtr taggedAsyncOpStub(void)
{
  AsyncOpTaggedPtr result;
  result.data = (~STATIC_CAST(uintptr_t, 0)) ^ ((STATIC_CAST(uintptr_t, 1) << COMBINER_TAG_SIZE) - 1);
  return result;
}

static inline AsyncOpTaggedPtr taggedAsyncOpMake(asyncOpRoot *op)
{
  AsyncOpTaggedPtr result = {REINTERPRET_CAST(uintptr_t, op)};
  return result;
}

static inline void taggedAsyncOpDecode(AsyncOpTaggedPtr ptr, asyncOpRoot **op, uint32_t *tag)
{
  const uintptr_t mask = (STATIC_CAST(uintptr_t, 1) << COMBINER_TAG_SIZE) - 1;
  *op = (asyncOpRoot*)(ptr.data & ~mask);
  *tag = STATIC_CAST(uint32_t, ptr.data & mask);
}

static inline int combinerPushValidated(aioObjectRoot *object, uint64_t generation, uint32_t tag)
{
  assert((tag & COMBINER_TAG_DELETE) == 0 && "DELETE must use the single-writer combinerPushDelete path");
  uint128 expected = {__uint64_atomic_load(&object->header.tag.low, amoRelaxed), generation};
  for (;;) {
    if (expected.high != generation)
      return 0;
    uint128 desired = {expected.low | tag, expected.high};
    if (__uint128_atomic_compare_and_swap(&object->header.tag, &expected, desired))
      break;
  }
  if (expected.low == 0)
    combiner(object, taggedAsyncOpStub(), taggedAsyncOpNull());
  return 1;
}

static inline asyncOpRoot *combinerAcquire(aioObjectRoot *object,
                                           List *queue,
                                           CreateAsyncOpProc *newAsyncOp,
                                           AsyncFlags flags,
                                           uint64_t usTimeout,
                                           void *callback,
                                           void *arg,
                                           int opCode,
                                           void *contextPtr)
{
  AsyncOpTaggedPtr head;
  AsyncOpTaggedPtr opTagged = taggedAsyncOpStub();
  AsyncOpTaggedPtr allocatedTagged;
  asyncOpRoot *allocated = 0;

  do {
    head.data = __uint64_atomic_load(&object->header.tag.low, amoRelaxed);
    if (head.data) {
      if (!allocated) {
        allocated = newAsyncOp(object, flags, usTimeout, callback, arg, opCode, contextPtr);
        allocatedTagged = taggedAsyncOpMake(allocated);
      }
      allocated->next = head;
      opTagged = allocatedTagged;
    } else {
      opTagged = taggedAsyncOpStub();
    }
  } while (!__uint64_atomic_compare_and_swap(&object->header.tag.low, head.data, opTagged.data, amoSeqCst));

  if (head.data)
    return allocated;

  if (queue->head || object->initializationOp ||
      __uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    if (!allocated) {
      allocated = newAsyncOp(object, flags, usTimeout, callback, arg, opCode, contextPtr);
      allocatedTagged = taggedAsyncOpMake(allocated);
    }
    combiner(object, taggedAsyncOpStub(), allocatedTagged);
    return allocated;
  }

  if (allocated) {
    if (allocated->releaseMethod)
      allocated->releaseMethod(allocated);
    releaseAsyncOp(allocated);
  }
  return 0;
}

static inline void combinerPushOperation(asyncOpRoot *op)
{
  aioObjectRoot *object = op->object;
  AsyncOpTaggedPtr opTagged = taggedAsyncOpMake(op);
  AsyncOpTaggedPtr newOp;
  AsyncOpTaggedPtr head;
  do {
    head.data = __uint64_atomic_load(&object->header.tag.low, amoRelaxed);
    if (head.data) {
      newOp = opTagged;
      op->next = head;
    } else {
      newOp = taggedAsyncOpStub();
    }
  } while (!__uint64_atomic_compare_and_swap(&object->header.tag.low, head.data, newOp.data, amoSeqCst));

  if (!head.data)
    combiner(object, taggedAsyncOpStub(), opTagged);
}

static inline void combinerPushCounter(aioObjectRoot *object, uint32_t tag)
{
  if (__uint64_atomic_fetch_or(&object->header.tag.low, tag, amoSeqCst) == 0)
    combiner(object, taggedAsyncOpStub(), taggedAsyncOpNull());
}

static inline void combinerPushDelete(aioObjectRoot *object)
{
  uintptr_t old = __uint64_atomic_fetch_and_add(&object->header.tag.low, COMBINER_TAG_DELETE, amoSeqCst);
  assert((old & COMBINER_TAG_DELETE) == 0 && "DELETE published more than once for one object incarnation");
  if (old == 0)
    combiner(object, taggedAsyncOpStub(), taggedAsyncOpNull());
}

static inline void combinerPushProgress(asyncOpRoot *op)
{
  uint32_t bit = (op->opCode & OPCODE_WRITE) ? COMBINER_TAG_PROGRESS_WRITE : COMBINER_TAG_PROGRESS_READ;
  combinerPushCounter(op->object, bit);
}

static inline void runAioOperation(aioObjectRoot *object,
                                   CreateAsyncOpProc *createAsyncOp,
                                   SyncImplProc *syncImpl,
                                   MakeResultProc *makeResult,
                                   InitOpProc *initOp,
                                   AsyncFlags flags,
                                   uint64_t usTimeout,
                                   void *callback,
                                   void *arg,
                                   int opCode,
                                   void *contextPtr)
{
  List *queue = !(opCode & OPCODE_WRITE) ? &object->readQueue : &object->writeQueue;
  if (!combinerAcquire(object, queue, createAsyncOp, flags, usTimeout, callback, arg, opCode, contextPtr)) {
    AsyncOpTaggedPtr forRun = taggedAsyncOpNull();
    asyncOpRoot *op = syncImpl(object, flags, usTimeout, callback, arg, contextPtr);
    if (!op) {
      if (callback == 0 || ((flags & afActiveOnce) &&
                            currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
        makeResult(contextPtr);
      } else {
        if (flags & afActiveOnce)
          currentFinishedSync = 0;
        op = createAsyncOp(object, flags | afNoCopy, usTimeout, callback, arg, opCode, contextPtr);
        initOp(op, contextPtr);
        opForceStatus(op, aosSuccess);
        addToGlobalQueue(op);
      }
    } else if (opGetStatus(op) != aosPending) {
      addToGlobalQueue(op);
    } else {
      forRun = taggedAsyncOpMake(op);
    }
    combiner(object, taggedAsyncOpStub(), forRun);
  }
}

static inline asyncOpRoot *runIoOperation(aioObjectRoot *object,
                                          CreateAsyncOpProc *createAsyncOp,
                                          SyncImplProc *syncImpl,
                                          InitOpProc *initOp,
                                          AsyncFlags flags,
                                          uint64_t usTimeout,
                                          int opCode,
                                          void *contextPtr)
{
  assert(!coroutineIsMain() && "Trying to run 'io' operation from main coroutine");
  List *queue = !(opCode & OPCODE_WRITE) ? &object->readQueue : &object->writeQueue;
  asyncOpRoot *op = combinerAcquire(object, queue, createAsyncOp, flags | afCoroutine,
                                    usTimeout, 0, 0, opCode, contextPtr);
  if (!op) {
    AsyncOpTaggedPtr forRun = taggedAsyncOpNull();
    op = syncImpl(object, flags | afCoroutine, usTimeout, 0, 0, contextPtr);
    if (!op) {
      if (!(++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
        currentFinishedSync = 0;
        op = createAsyncOp(object, flags | afCoroutine | afNoCopy, usTimeout, 0, 0, opCode, contextPtr);
        initOp(op, contextPtr);
        opForceStatus(op, aosSuccess);
        addToGlobalQueue(op);
      }
    } else if (opGetStatus(op) != aosPending) {
      addToGlobalQueue(op);
    } else {
      forRun = taggedAsyncOpMake(op);
    }
    combiner(object, taggedAsyncOpStub(), forRun);
  }
  if (op)
    coroutineYield();
  return op;
}

static inline uint32_t combinerSelectActiveIoEvents(int hasInitialization,
                                                    AsyncOpRunningTy initializationState,
                                                    int initializationIsWrite,
                                                    int hasReadQueue,
                                                    int hasWriteQueue)
{
  if (hasInitialization)
    return initializationState == arRunning ? (initializationIsWrite ? IO_EVENT_WRITE : IO_EVENT_READ) : 0;
  return (hasReadQueue ? IO_EVENT_READ : 0) | (hasWriteQueue ? IO_EVENT_WRITE : 0);
}

static inline uint32_t combinerActiveIoEvents(aioObjectRoot *object)
{
  asyncOpRoot *initialization = object->initializationOp;
  if (initialization)
    return combinerSelectActiveIoEvents(1, initialization->running,
                                        initialization->opCode & OPCODE_WRITE, 0, 0);
  return combinerSelectActiveIoEvents(0, arWaiting, 0,
                                      object->readQueue.head != 0,
                                      object->writeQueue.head != 0);
}

typedef enum CombinerInitializationAction {
  ciaNone,
  ciaExecute,
  ciaRelease
} CombinerInitializationAction;

static inline CombinerInitializationAction combinerSelectInitializationAction(AsyncOpRunningTy running, AsyncOpStatus status)
{
  switch (running) {
    case arRunning: return status == aosPending ? ciaExecute : ciaRelease;
    case arCancelling: return ciaRelease;
    case arWaiting:
    default: return ciaNone;
  }
}

typedef enum CombinerReapAction {
  craKeep,
  craCancel,
  craRelease
} CombinerReapAction;

static inline CombinerReapAction combinerSelectReapAction(AsyncOpRunningTy running, AsyncOpStatus status)
{
  if (status == aosPending)
    return craKeep;
  switch (running) {
    case arRunning: return craCancel;
    case arWaiting: return craRelease;
    case arCancelling:
    default: return craKeep;
  }
}

TimerLoopState *loopThreadEnter(asyncBase *base);
unsigned loopThreadExit(asyncBase *base);

static inline int enqueueNeedsDoorbell(asyncBase *base, asyncOpRoot *op)
{
  return !(op && base->loopThreadLimit == 1 && loopThreadBase == base);
}

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_ASYNCIOIMPL_H_
