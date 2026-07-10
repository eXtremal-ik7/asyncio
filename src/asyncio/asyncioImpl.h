#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/api.h"
#include "asyncio/ringBuffer.h"
#ifndef OS_WINDOWS
#include <signal.h>
#endif

#define TAGGED_POINTER_DATA_SIZE 6
#define TAGGED_POINTER_ALIGNMENT (((intptr_t)1) << TAGGED_POINTER_DATA_SIZE)
#define TAGGED_POINTER_DATA_MASK (TAGGED_POINTER_ALIGNMENT-1)
#define TAGGED_POINTER_PTR_MASK (~TAGGED_POINTER_DATA_MASK)

// Upper bound on concurrently running loop threads per base, 64 bytes of
// asyncBase each; the reclaim scan is bounded by the high-water mark of
// slots actually claimed, so the headroom costs memory only
#define GRACE_LOOP_THREAD_LIMIT 256

// One quiescence stamp per loop thread, padded to a cache line so the
// per-iteration stamps of neighbour threads do not false-share
typedef struct GraceSlot {
  volatile uintptr_t seen;
  uintptr_t pad[7];
} GraceSlot;

typedef enum IoActionTy {
  actAccept = OPCODE_READ,
  actRead,
  actReadMsg,
  actConnect = OPCODE_WRITE,
  actWrite,
  actWriteMsg,
  actUserEvent = OPCODE_OTHER,
} IoActionTy;

// (object, startOp, sig): startOp != 0 is an op-node to start; sig is the
// accumulated signal bits (COMBINER_TAG_MOVE_*/CANCEL) to reconcile for this
// node. Either may be empty.
typedef void combinerTaskHandlerTy(aioObjectRoot*, asyncOpRoot*, uint32_t);
typedef void enqueueOperationTy(asyncBase*, asyncOpRoot*);
typedef void postEmptyOperationTy(asyncBase*);
typedef void nextFinishedOperationTy(asyncBase*);
typedef aioObject *newAioObjectTy(asyncBase*, IoObjectTy, void*);
typedef void deleteObjectTy(aioObject*);
typedef void startTimerTy(asyncOpRoot*);
typedef void stopTimerTy(asyncOpRoot*);
typedef void deleteTimerTy(asyncOpRoot*);
typedef void activateTy(aioUserEvent*);

struct asyncImpl {
  combinerTaskHandlerTy *combinerTaskHandler;
  enqueueOperationTy *enqueue;
  postEmptyOperationTy *postEmptyOperation;
  nextFinishedOperationTy *nextFinishedOperation;
  newAioObjectTy *newAioObject;
  newAsyncOpTy *newAsyncOp;
  aioCancelProc *cancelAsyncOp;
  deleteObjectTy *deleteObject;
  initializeTimerTy *initializeTimer;
  startTimerTy *startTimer;
  stopTimerTy *stopTimer;
  deleteTimerTy *deleteTimer;
  activateTy *activate;
  aioExecuteProc *connect;
  aioExecuteProc *accept;
  aioExecuteProc *read;
  aioExecuteProc *write;
  aioExecuteProc *readMsg;
  aioExecuteProc *writeMsg;
};

struct asyncBase {
  enum AsyncMethod method;
  struct asyncImpl methodImpl;
  struct ConcurrentQueue globalQueue;
  struct pageMap timerMap;
  uint64_t lastCheckPoint;
  volatile unsigned messageLoopThreadCounter;
  volatile unsigned timerMapLock;

  // Grace-period reclamation for dead aio objects. epoll_wait/kevent batches
  // hold raw object pointers that cannot be recalled, so releasing the memory
  // right in the destructor lets a stale batch entry touch the pool's next
  // incarnation of the object (spurious disconnect, lost event) - a use after
  // free veiled by the type-stable pool, or a plain one with instrumented
  // pools. A dead object waits in the limbo list instead until every loop
  // thread passes the top of its message loop: batches copied before the
  // retirement are fully dispatched by then. Threads block reclamation for
  // at most one wait timeout - both backends wake periodically. Windows is
  // not involved: IOCP packets reference operations, and operation pools are
  // permanently type-stable.
  volatile uintptr_t graceEpoch;
  volatile unsigned graceFrozen;    // slots exhausted or id collision: reclamation is off, the limbo only grows
  volatile unsigned graceSlotCount; // high-water mark of claimed slots, bounds the reclaim scan
  unsigned graceLimboLock;
  aioObjectRoot *graceLimbo;        // newest first, epochs decrease along the list
  GraceSlot graceSeen[GRACE_LOOP_THREAD_LIMIT];

#ifndef NDEBUG
  int opsCount;
#endif
};

struct ioBuffer {
  void *ptr;
  size_t totalSize;
  size_t dataSize;
  size_t offset;
};

struct aioObject {
  aioObjectRoot root;
  union {
    iodevTy hDevice;
    socketTy hSocket;
  };

  struct ioBuffer buffer;
  // Nonzero only for device descriptors that can raise SIGPIPE (pipes) on
  // platforms without per-fd suppression (no F_SETNOSIGPIPE): write paths
  // must mask SIGPIPE around the syscall unless sigpipeIgnored is set.
  int needSigpipeGuard;
};

// Pure decision kernel for the readiness backends. During one-shot transport
// initialization ordinary queues may already contain later submissions, but
// they are not active yet. Keeping this selection separate from epoll/kqueue
// syscalls makes the state table explicit and exhaustively unit-testable.
static inline uint32_t combinerSelectActiveIoEvents(int hasInitialization,
                                                    AsyncOpRunningTy initializationState,
                                                    int initializationIsWrite,
                                                    int hasReadQueue,
                                                    int hasWriteQueue)
{
  if (hasInitialization)
    return initializationState == arRunning
      ? (initializationIsWrite ? IO_EVENT_WRITE : IO_EVENT_READ)
      : 0;

  return (hasReadQueue ? IO_EVENT_READ : 0) |
         (hasWriteQueue ? IO_EVENT_WRITE : 0);
}

static inline uint32_t combinerActiveIoEvents(aioObjectRoot *object)
{
  asyncOpRoot *initialization = (asyncOpRoot*)__uintptr_atomic_load(&object->initializationOp, amoRelaxed);
  if (initialization)
    return combinerSelectActiveIoEvents(1,
                                        initialization->running,
                                        initialization->opCode & OPCODE_WRITE,
                                        0,
                                        0);

  return combinerSelectActiveIoEvents(0,
                                      arWaiting,
                                      0,
                                      object->readQueue.head != 0,
                                      object->writeQueue.head != 0);
}

typedef enum CombinerInitializationAction {
  ciaNone,
  ciaExecute,
  ciaRelease
} CombinerInitializationAction;

// Status and position select one mutually exclusive initialization action;
// execution itself remains outside this pure function.
static inline CombinerInitializationAction combinerSelectInitializationAction(AsyncOpRunningTy running,
                                                                               AsyncOpStatus status)
{
  switch (running) {
    case arRunning:
      return status == aosPending ? ciaExecute : ciaRelease;
    case arCancelling:
      return ciaRelease;
    case arWaiting:
    default:
      return ciaNone;
  }
}

typedef enum CombinerReapAction {
  craKeep,
  craCancel,
  craRelease
} CombinerReapAction;

static inline CombinerReapAction combinerSelectReapAction(AsyncOpRunningTy running,
                                                           AsyncOpStatus status)
{
  if (status == aosPending)
    return craKeep;

  switch (running) {
    case arRunning:
      return craCancel;
    case arWaiting:
      return craRelease;
    case arCancelling:
    default:
      return craKeep;
  }
}

#ifndef OS_WINDOWS
// Set once by initializeAsyncIo(aiIgnoreSigpipe); write paths then skip the
// SIGPIPE masking below.
extern int sigpipeIgnored;

// Masks SIGPIPE around a pipe write on platforms without per-fd suppression
// (libpq pattern). The mask is per-thread, so this is combiner-safe. Usage:
//   sigpipeGuardEnter(&guard);
//   <write syscalls>
//   sigpipeGuardLeave(&guard, <last write failed with EPIPE>);
// Leave consumes the SIGPIPE generated by our own write (unless one was
// already pending before entry) and preserves errno.
struct SigpipeGuard {
  sigset_t savedMask;
  int wasPending;
};

void sigpipeGuardEnter(struct SigpipeGuard *guard);
void sigpipeGuardLeave(struct SigpipeGuard *guard, int consumeSigpipe);
#endif

struct asyncOp {
  asyncOpRoot root;
  int state;
  void *buffer;
  size_t transactionSize;
  size_t bytesTransferred;
  socketTy acceptSocket;
  HostAddress host;

  void *internalBuffer;
  size_t internalBufferSize;
};

struct aioUserEvent {
  asyncOpRoot root;
  uintptr_t tag;
  asyncBase *base;
  int counter;
  int isSemaphore;
  int pendingActivations;
  userEventDestructorCb *destructorCb;
  void *destructorCbArg;
};

void pageMapInit(pageMap *map);
void addToTimeoutQueue(asyncBase *base, asyncOpRoot *op);
void processTimeoutQueue(asyncBase *base, uint64_t currentTime);

// Monotonic (wall-clock-independent) seconds for the timeout grid; see the
// definition in asyncioImpl.c for why the grid must not use time(0). Returns an
// unsigned 64-bit tick count, not calendar time, so it is intentionally not a
// time_t (whose width is platform-dependent and which connotes wall time).
uint64_t getMonotonicSeconds(void);

// Grace period (see the asyncBase comment). graceThreadEnter runs once per
// loop thread right after the messageLoopThreadId assignment and freezes
// reclamation on slot overflow or on an id collision. graceQuiesce stamps
// the calling loop thread as quiescent and reclaims ripe limbo objects;
// call it at the top of the message loop, right before blocking in the
// kernel wait. graceRetire parks a dead object in the limbo list;
// memoryRelease is the memory half of its destructor and runs once the
// grace period elapses. graceReclaim alone is for the loop exit path,
// after the thread stamped itself out with UINTPTR_MAX: the last thread
// out drains the list.
void graceThreadEnter(asyncBase *base);
void graceQuiesce(asyncBase *base);
void graceRetire(asyncBase *base, aioObjectRoot *object, aioObjectDestructor *memoryRelease);
void graceReclaim(asyncBase *base);

int copyFromBuffer(void *dst, size_t *offset, struct ioBuffer *src, size_t size);
#ifdef __cplusplus
}

#endif
