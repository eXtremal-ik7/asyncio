#ifndef __ASYNCIO_ASYNCOP_H_
#define __ASYNCIO_ASYNCOP_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "coroutine.h"
#include "macro.h"
#include "atomic.h"
#include "ringBuffer.h"
#include "asyncio/asyncioTypes.h"


#define MAX_SYNCHRONOUS_FINISHED_OPERATION 32

typedef enum AsyncMethod {
  amOSDefault = 0,
  amEPoll,
  amKQueue,
  amIOCP,
} AsyncMethod;


typedef enum AsyncInitFlags {
  aiNone = 0,
  // Set the SIGPIPE disposition to ignored, process-wide (POSIX; no-op on
  // Windows). Opt-in because signal disposition is global policy owned by
  // the application: writes to a dead stdout pipeline stop killing the
  // process, and the ignored disposition is inherited by exec()ed children.
  // Without the flag the library still protects its own descriptors: sockets
  // per-fd (MSG_NOSIGNAL / SO_NOSIGPIPE), pipes via F_SETNOSIGPIPE where the
  // OS has it, elsewhere by masking SIGPIPE around pipe writes; the flag
  // additionally removes that per-write masking cost.
  aiIgnoreSigpipe = 1
} AsyncInitFlags;


typedef enum IoObjectTy {
  ioObjectSocket,
  ioObjectDevice,
  ioObjectTimer,
  ioObjectUserDefined
} IoObjectTy;


typedef enum AsyncOpStatus {
  aosUnknown = -1,
  aosSuccess = 0,
  aosPending,
  aosTimeout,
  aosDisconnected,
  aosCanceled,
  aosBufferTooSmall,
  aosUnknownError,
  aosLast
} AsyncOpStatus;


typedef enum AsyncFlags {
  afNone = 0,
  afWaitAll = 1,
  afNoCopy = 2,
  afRealtime = 4,
  afActiveOnce = 8,
  afRunning = 16,
  afCoroutine = 32
} AsyncFlags;

typedef enum AsyncOpActionTy {
  aaNone = 0,
  aaStart = 0,
  aaCancel,
  aaFinish,
  aaContinue
} AsyncOpActionTy;

typedef enum AsyncOpRunningTy {
  arWaiting = 0,
  arRunning,
  arCancelling
} AsyncOpRunningTy;

#ifdef __cplusplus
__NO_UNUSED_FUNCTION_BEGIN
static inline AsyncFlags operator|(AsyncFlags a, AsyncFlags b) {
  return static_cast<AsyncFlags>(static_cast<int>(a) | static_cast<int>(b));
}
__NO_UNUSED_FUNCTION_END
#endif

typedef struct ObjectPool ObjectPool;
typedef struct asyncBase asyncBase;
typedef struct aioObjectRoot aioObjectRoot;
typedef struct asyncOpRoot asyncOpRoot;
typedef struct asyncOpListLink asyncOpListLink;
typedef struct asyncOpAction asyncOpAction;
typedef struct coroutineTy coroutineTy;

typedef struct aioObject aioObject;
typedef struct aioUserEvent aioUserEvent;
typedef struct asyncOp asyncOp;

typedef struct List {
  asyncOpRoot *head;
  asyncOpRoot *tail;
} List;

typedef asyncOpRoot *newAsyncOpTy(asyncBase*, int, ConcurrentQueue*, ConcurrentQueue*);
typedef void initializeTimerTy(asyncBase*, asyncOpRoot*);
typedef AsyncOpStatus aioExecuteProc(asyncOpRoot*);
typedef int aioCancelProc(asyncOpRoot*);
typedef void aioFinishProc(asyncOpRoot*);
typedef void aioReleaseProc(asyncOpRoot*);
typedef void aioObjectDestructor(aioObjectRoot*);
typedef void aioObjectDestructorCb(aioObjectRoot*, void*);
typedef void userEventDestructorCb(aioUserEvent*, void*);

extern __tls unsigned currentFinishedSync;
extern __tls unsigned messageLoopThreadId;

#ifndef __cplusplus
#define STATIC_CAST(x, y) ((x)(y))
#define REINTERPRET_CAST(x, y) ((x)(y))
#else
#define STATIC_CAST(x, y) static_cast<x>(y)
#define REINTERPRET_CAST(x, y) reinterpret_cast<x>(y)
#endif

#define TAG_STATUS_SIZE 8
#define TAG_STATUS_MASK ((STATIC_CAST(uintptr_t, 1) << TAG_STATUS_SIZE)-1)
#define TAG_GENERATION_MASK (~TAG_STATUS_MASK)

#define COMBINER_TAG_SIZE     5
#define COMBINER_TAG_ACCESS   (1u)
#define COMBINER_TAG_DELETE   (1u << 2)

typedef struct AsyncOpTaggedPtr {
  uintptr_t data;
} AsyncOpTaggedPtr;

#define IO_EVENT_READ     1u
#define IO_EVENT_WRITE    2u
#define IO_EVENT_ERROR    4u

// Event tag area
#if defined(OS_32)
#define TAG_EVENT_OP          STATIC_CAST(uintptr_t, 0x00010000U)
#define TAG_EVENT_DELETE      STATIC_CAST(uintptr_t, 0x10000000U)
#define TAG_EVENT_OP_MASK     STATIC_CAST(uintptr_t, 0xFFFF0000U)
#define TAG_EVENT_MASK        STATIC_CAST(uintptr_t, 0x0FFFFFFFU)
#define TAG_EVENT_DELETE_MASK STATIC_CAST(uintptr_t, 0xF0000000U)
#elif defined(OS_64)
#define TAG_EVENT_OP          STATIC_CAST(uintptr_t, 0x0000000100000000ULL)
#define TAG_EVENT_DELETE      STATIC_CAST(uintptr_t, 0x1000000000000000ULL)
#define TAG_EVENT_OP_MASK     STATIC_CAST(uintptr_t, 0xFFFFFFFF00000000ULL)
#define TAG_EVENT_MASK        STATIC_CAST(uintptr_t, 0x0FFFFFFFFFFFFFFFULL)
#define TAG_EVENT_DELETE_MASK STATIC_CAST(uintptr_t, 0xF000000000000000ULL)
#else
#error Configution incomplete
#endif

#define OPCODE_READ 0
#define OPCODE_WRITE 0x01000000
#define OPCODE_OTHER 0x02000000

uintptr_t objectIncrementReference(aioObjectRoot *object, uintptr_t count);
uintptr_t objectDecrementReference(aioObjectRoot *object, uintptr_t count);
uintptr_t eventIncrementReference(aioUserEvent *event, uintptr_t tag);
uintptr_t eventDecrementReference(aioUserEvent *event, uintptr_t tag);
int eventTryActivate(aioUserEvent *event);
void eventDeactivate(aioUserEvent *event);

void *alignedMalloc(size_t size, size_t alignment);
void alignedFree(void *ptr);
void *__tagged_pointer_make(void *ptr, uintptr_t data);
void __tagged_pointer_decode(void *ptr, void **outPtr, uintptr_t *outData);

// Recycling pools for objects. Pooled memory is never returned to the
// allocator, so a sanitizer sees use-after-destruction as ordinary access
// to valid memory. With INSTRUMENTED_POOLS (cmake option
// BUILD_INSTRUMENTED_POOLS) every acquisition misses and objectPoolPut
// reports "not pooled": the structure lives one malloc/free cycle and the
// caller frees it with the matching deallocator - the caller, because
// pooled structures own nested buffers the generic code cannot release.
// The switch deliberately covers objects only. Operations cannot leave
// their pools: the timeout grid keeps op pointers past the operation's
// lifetime and lazily cancels through them when their second arrives -
// that is safe exactly because recycled operation memory stays type-stable
// and the generation tag rejects the stale cancel. User events own their
// platform timer, freeing them would leak the timer descriptor.
static inline int objectPoolGet(ConcurrentQueue *pool, void **result)
{
#ifdef INSTRUMENTED_POOLS
  (void)pool;
  *result = 0;
  return 0;
#else
  return concurrentQueuePop(pool, result);
#endif
}

static inline int objectPoolPut(ConcurrentQueue *pool, void *object)
{
#ifdef INSTRUMENTED_POOLS
  (void)pool;
  (void)object;
  return 0;
#else
  concurrentQueuePush(pool, object);
  return 1;
#endif
}

void eqRemove(List *list, asyncOpRoot *op);
void eqPushBack(List *list, asyncOpRoot *op);

typedef struct asyncOpListLink {
  asyncOpRoot *op;
  uintptr_t tag;
  asyncOpListLink *next;
} asyncOpListLink;

typedef struct asyncOpAction {
  asyncOpRoot *op;
  AsyncOpActionTy actionType;
  asyncOpAction *next;
} asyncOpAction;

typedef struct ListImpl {
  asyncOpRoot *prev;
  asyncOpRoot *next;
} ListImpl;

typedef struct pageMap {
  asyncOpListLink ***map;
  unsigned lock;
} pageMap;


struct aioObjectRoot {
  AsyncOpTaggedPtr Head;
  asyncBase *base;
  uintptr_t refs;
  List readQueue;
  List writeQueue;

  volatile uint32_t CancelIoFlag;
  // Set by objectDelete, never cleared: the object is dying, every combiner
  // pass sweeps the queues once more before releasing ownership. A plain
  // CancelIoFlag pass is positionally sloppy - it runs at the node its tag
  // landed on, so a submission parked later in the same captured chain
  // starts after the sweep; with no timeout armed such an operation would
  // hold its object reference forever and the DELETE tag would never fire
  volatile uint32_t DeletePending;
  // Exclusive operation slot (connect): claimed by CAS at submission time,
  // cleared under the object's combiner when the operation leaves the slot
  volatile uintptr_t exclusiveOp;

  IoObjectTy type;
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
    uint64_t endTime;
  };
  AsyncOpRunningTy running;
};

void initObjectRoot(aioObjectRoot *object, asyncBase *base, IoObjectTy type, aioObjectDestructor destructor);
void objectSetDestructorCb(aioObjectRoot *object, aioObjectDestructorCb callback, void *arg);
void eventSetDestructorCb(aioUserEvent *event, userEventDestructorCb callback, void *arg);

void cancelIo(aioObjectRoot *object);
void objectDelete(aioObjectRoot *object);

uintptr_t opGetGeneration(asyncOpRoot *op);
AsyncOpStatus opGetStatus(asyncOpRoot *op);
int opSetStatus(asyncOpRoot *op, uintptr_t tag, AsyncOpStatus status);
void opForceStatus(asyncOpRoot *op, AsyncOpStatus status);
uintptr_t opEncodeTag(asyncOpRoot *op, uintptr_t tag);

void opRelease(asyncOpRoot *op, AsyncOpStatus status, List *executeList);
void processAction(asyncOpRoot *opptr, AsyncOpActionTy actionType, uint32_t *needStart);
void processExclusiveOp(aioObjectRoot *object, uint32_t *needStart);
void executeOperationList(List *list);
void cancelOperationList(List *list, AsyncOpStatus status);

void opCancel(asyncOpRoot *op, uintptr_t generation, AsyncOpStatus status);
void resumeParent(asyncOpRoot *op, AsyncOpStatus status);

void addToGlobalQueue(asyncOpRoot *op);
int executeGlobalQueue(asyncBase *base);

typedef asyncOpRoot *CreateAsyncOpProc(aioObjectRoot*, AsyncFlags, uint64_t, void*, void*, int, void*);
typedef asyncOpRoot *SyncImplProc(aioObjectRoot*, AsyncFlags, uint64_t, void*, void*, void*);
typedef void MakeResultProc(void*);
typedef void InitOpProc(asyncOpRoot*, void*);

int asyncOpAlloc(asyncBase *base, size_t size, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool, asyncOpRoot **result);
void releaseAsyncOp(asyncOpRoot *op);

void initAsyncOpRoot(asyncOpRoot *op,
                     aioExecuteProc *startMethod,
                     aioCancelProc *cancelMethod,
                     aioFinishProc *finishMethod,
                     aioReleaseProc *deleteMethod,
                     aioObjectRoot *object,
                     void *callback,
                     void *arg,
                     AsyncFlags flags,
                     int opCode,
                     uint64_t timeout);



void combiner(aioObjectRoot *object, AsyncOpTaggedPtr stackTop, AsyncOpTaggedPtr forRun);

__NO_UNUSED_FUNCTION_BEGIN
static inline AsyncOpTaggedPtr taggedAsyncOpNull()
{
  AsyncOpTaggedPtr result;
  result.data = 0;
  return result;
}

static inline AsyncOpTaggedPtr taggedAsyncOpStub()
{
  AsyncOpTaggedPtr result;
  result.data = (~STATIC_CAST(uintptr_t, 0)) ^ ((STATIC_CAST(uintptr_t, 1) << COMBINER_TAG_SIZE) -1);
  return result;
}

static inline AsyncOpTaggedPtr taggedAsyncOpMake(asyncOpRoot *op, AsyncOpActionTy opMethod, uint32_t tag)
{
  AsyncOpTaggedPtr result;
  result.data = REINTERPRET_CAST(uintptr_t, op) | (opMethod << 3) | tag;
  return result;
}

static inline void taggedAsyncOpDecode(AsyncOpTaggedPtr ptr, asyncOpRoot **op, AsyncOpActionTy *opMethod, uint32_t *tag)
{
  uintptr_t COMBINER_TAG_MASK = (STATIC_CAST(uintptr_t, 1) << COMBINER_TAG_SIZE) - 1;
  *op = (asyncOpRoot*)(ptr.data & (~COMBINER_TAG_MASK));
  *opMethod = STATIC_CAST(AsyncOpActionTy, (ptr.data >> 3) & 0x3);
  *tag = ptr.data & 0x7;
}

static inline asyncOpRoot *combinerAcquire(aioObjectRoot *object,
                                           List *queue,
                                           AsyncOpActionTy actionType,
                                           CreateAsyncOpProc *newAsyncOp,
                                           AsyncFlags flags,
                                           uint64_t usTimeout,
                                           void *callback,
                                           void *arg,
                                           int opCode,
                                           void *contextPtr) {
  AsyncOpTaggedPtr head;
  AsyncOpTaggedPtr opTagged = taggedAsyncOpStub();
  AsyncOpTaggedPtr allocatedTagged;
  asyncOpRoot *allocated = 0;

  do {
    head = object->Head;
    if (head.data) {
      if (!allocated) {
        allocated = newAsyncOp(object, flags, usTimeout, callback, arg, opCode, contextPtr);
        allocatedTagged = taggedAsyncOpMake(allocated, actionType, 0);
      }

      allocated->next = head;
      opTagged = allocatedTagged;
    } else {
      opTagged = taggedAsyncOpStub();
    }
  } while (!__uintptr_atomic_compare_and_swap(&object->Head.data, head.data, opTagged.data));

  if (!head.data) {
    // This thread entered a combiner
    if (queue->head || object->exclusiveOp) {
      // Object has operations in queue or an exclusive operation in flight
      // Put operation to queue end and try exit combiner
      if (!allocated) {
        allocated = newAsyncOp(object, flags, usTimeout, callback, arg, opCode, contextPtr);
        allocatedTagged = taggedAsyncOpMake(allocated, actionType, 0);
      }

      combiner(object, taggedAsyncOpStub(), allocatedTagged);
      return allocated;
    } else {
      if (allocated)
        releaseAsyncOp(allocated);
      return 0;
    }
  } else {
    return allocated;
  }
}

static inline void combinerPushOperation(asyncOpRoot *op, AsyncOpActionTy actionType)
{
  aioObjectRoot *object = op->object;
  AsyncOpTaggedPtr opTagged = taggedAsyncOpMake(op, actionType, 0);
  AsyncOpTaggedPtr newOp;
  AsyncOpTaggedPtr head;
  do {
    head = object->Head;
    if (head.data) {
      newOp = opTagged;
      op->next = head;
    } else {
      newOp = taggedAsyncOpStub();
    }
  } while (!__uintptr_atomic_compare_and_swap(&object->Head.data, head.data, newOp.data));

  if (!head.data)
    combiner(object, taggedAsyncOpStub(), opTagged);
}

static inline void combinerPushCounter(aioObjectRoot *object, uint32_t tag) {
  if (__uintptr_atomic_fetch_and_add(&object->Head.data, tag) == 0)
    combiner(object, taggedAsyncOpMake(0, aaNone, tag), taggedAsyncOpMake(0, aaNone, tag));
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
  if (!combinerAcquire(object, !(opCode & OPCODE_WRITE) ? &object->readQueue : &object->writeQueue, aaStart, createAsyncOp, flags, usTimeout, callback, arg, opCode, contextPtr)) {
    // Object locked by current operation
    AsyncOpTaggedPtr forRun = taggedAsyncOpNull();
    asyncOpRoot *op = syncImpl(object, flags, usTimeout, callback, arg, contextPtr);
    if (!op) {
      // Fire-and-forget has no completion channel: the inline return value is
      // the only way the caller ever learns the result, so it must not depend
      // on the budget. The budget only pushes afActiveOnce callers back to
      // callback delivery and is not consumed by operations that cannot
      // return inline
      if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
        makeResult(contextPtr);
      } else {
        // Budget exhausted: this completion goes through the loop, the inline
        // window restarts - without the reset a thread that never drains the
        // queues would lose the fast path forever after the first 32 calls
        if (flags & afActiveOnce)
          currentFinishedSync = 0;
        asyncOpRoot *op = createAsyncOp(object, flags, usTimeout, callback, arg, opCode, contextPtr);
        initOp(op, contextPtr);
        opForceStatus(op, aosSuccess);
        addToGlobalQueue(op);
      }
    } else if (opGetStatus(op) != aosPending) {
      // Operation finished already
      addToGlobalQueue(op);
    } else {
      forRun = taggedAsyncOpMake(op, aaStart, 0);
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
  asyncOpRoot *op = combinerAcquire(object, !(opCode & OPCODE_WRITE) ? &object->readQueue : &object->writeQueue, aaStart, createAsyncOp, flags | afCoroutine, usTimeout, 0, 0, opCode, contextPtr);
  if (!op) {
    // Object locked by current operation
    AsyncOpTaggedPtr forRun = taggedAsyncOpNull();
    op = syncImpl(object, flags | afCoroutine, usTimeout, 0, 0, contextPtr);
    if (!op) {
      if (!(++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
        op = createAsyncOp(object, afCoroutine, usTimeout, 0, 0, opCode, contextPtr);
        initOp(op, contextPtr);
        opForceStatus(op, aosSuccess);
        addToGlobalQueue(op);
      }
    } else if (opGetStatus(op) != aosPending) {
      // Operation finished already
      addToGlobalQueue(op);
    } else {
      forRun = taggedAsyncOpMake(op, aaStart, 0);
    }

    combiner(object, taggedAsyncOpStub(), forRun);
  }

  if (op)
    coroutineYield();
  return op;
}

__NO_UNUSED_FUNCTION_END

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_ASYNCOP_H_
