#ifndef __ASYNCIO_ASYNCIOIMPL_H_
#define __ASYNCIO_ASYNCIOIMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/api.h"
#include "asyncio/ringBuffer.h"
#include "atomic128.h"
#ifndef OS_WINDOWS
#include <signal.h>
#endif

#define TAGGED_POINTER_DATA_SIZE 6
#define TAGGED_POINTER_ALIGNMENT (((intptr_t)1) << TAGGED_POINTER_DATA_SIZE)
#define TAGGED_POINTER_DATA_MASK (TAGGED_POINTER_ALIGNMENT-1)
#define TAGGED_POINTER_PTR_MASK (~TAGGED_POINTER_DATA_MASK)

// Timeout grid: an exact cascading hierarchical timer wheel. The tick is a
// compile-time constant: changing the resolution later renumbers deadlines
// but does not touch the architecture.
#define TIMER_TICK_MICROSECONDS 125000
#define TIMER_WHEEL_LEVEL_BITS 10
#define TIMER_WHEEL_SLOTS (1u << TIMER_WHEEL_LEVEL_BITS)
#define TIMER_WHEEL_LEVELS 4

// Slot = atomic {head, baseTick} pair changed only by DWCAS: drain and reopen
// are one transition, and a link can never be published into a foreign slot
// incarnation unnoticed - the pair CAS validates baseTick together with the
// head. head is an intrusive LIFO of asyncOpListLink (low word), baseTick is
// the absolute tick starting the slot's current window (high word); baseTick
// only grows, in steps of the level's rotation period, which kills ABA for
// the whole 64-bit range. Level L covers windows of 2^(10L) ticks; deadlines
// beyond level 3 clamp into its farthest slot and re-cascade on visit.
typedef struct timerWheel {
  uint128Pair slots[TIMER_WHEEL_LEVELS][TIMER_WHEEL_SLOTS];
} timerWheel;

#define CACHE_LINE_SIZE 64

// One quiescence stamp per loop thread, padded to a cache line so the
// per-iteration stamps of neighbour threads do not false-share; the padding
// is byte-sized so 32-bit targets get the full line too
typedef struct GraceSlot {
  volatile uintptr_t seen;
  uint8_t pad[CACHE_LINE_SIZE - sizeof(uintptr_t)];
} GraceSlot;

// Published sleep horizon of one loop thread: the absolute grid tick it will
// wake at on its own, UINTPTR_MAX while awake. A producer arming a deadline
// no sleeper would meet in time kicks one loop through the backend wakeup.
// Padded like GraceSlot: the slot is rewritten around every wait syscall
typedef struct TimerSleepSlot {
  volatile uintptr_t wakeTick;
  uint8_t pad[CACHE_LINE_SIZE - sizeof(uintptr_t)];
} TimerSleepSlot;

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
typedef void loopWakeupTy(asyncBase*);
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
  // Pure kick of one sleeping loop thread (no queue traffic, unlike
  // postEmptyOperation whose empty node is the quit marker): eventfd write /
  // EVFILT_USER trigger / posted completion with a byte count
  loopWakeupTy *wakeupLoop;
};

struct asyncBase {
  enum AsyncMethod method;
  struct asyncImpl methodImpl;
  struct ConcurrentQueue globalQueue;
  struct timerWheel timerWheel;
  // First monotonic tick whose sweep is not confirmed yet. Moves only by the
  // exact tick->tick+1 CAS in timerWheelSweepTick, performed by whichever
  // thread sweeps the tick first (helping), so it is monotonic without any
  // lock. uintptr_t matches the plain atomic helpers; the wheel already
  // requires a 64-bit target (atomic128.h), so the full 64-bit tick range is
  // available and wrap is out of scope. The byte pads give the cursor a cache
  // line of its own whatever the base allocation's alignment: the confirm CAS
  // must not ping-pong the lines holding the wheel's last slots or the
  // per-iteration loads of messageLoopThreadCounter
  uint8_t timerCloseCursorPadBefore[CACHE_LINE_SIZE - sizeof(uintptr_t)];
  uintptr_t timerCloseCursor;
  uint8_t timerCloseCursorPadAfter[CACHE_LINE_SIZE - sizeof(uintptr_t)];
  // Number of links parked in the wheel (transitional, until the occupancy
  // bitmap): non-zero caps every loop sleep at one tick, so a parked deadline
  // is never behind a wait longer than the grid step. Lazy cancellation keeps
  // it non-zero until stale links expire - the price of the transitional
  // scheme. The RMW on it is also the full barrier of the sleep handshake
  // (see timerLoopPrepareSleep); own cache line, it is hit by every arm
  uintptr_t timerOutstanding;
  uint8_t timerOutstandingPad[CACHE_LINE_SIZE - sizeof(uintptr_t)];
  // One published sleep horizon per loop thread, graceSlotLimit entries;
  // allocated with the base (freed at 0.6 teardown, like graceSeen)
  TimerSleepSlot *timerSleep;
  volatile unsigned messageLoopThreadCounter;

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
  volatile unsigned graceFrozen;    // slots exhausted or id collision: reclamation is off, the limbo only grows
  volatile unsigned graceSlotCount; // high-water mark of claimed slots, bounds the batch capture
  unsigned graceSlotLimit;          // slot array capacity, from the createAsyncBase loopThreads argument
  volatile unsigned graceScanning;  // non-blocking single-scanner gate: losers leave, nobody waits
  aioObjectRoot *graceLimbo;        // lock-free intrusive stack of fresh retirements (CAS push, exchange detach)
  aioObjectRoot *gracePending;      // scanner-owned batch waiting out its grace period; read-only NULL check elsewhere
  unsigned gracePendingSlots;       // slot high-water captured together with the pending batch
  uintptr_t *gracePendingSeen;      // per-slot counters captured together with the pending batch, scanner-owned
  GraceSlot *graceSeen;             // cache-line aligned, graceSlotLimit entries; alignedFree with the base (0.6 teardown)

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

// Single-threaded wheel lifecycle. Init may run on zeroed or reused storage
// and seeds every slot's baseTick for the given cursor; teardown recycles
// whatever links are still parked in the slots into the global link pool
// without delivering them - the loop threads must already be stopped.
void timerWheelInit(asyncBase *base, uint64_t currentTick);
void timerWheelTeardown(asyncBase *base);

// Publish a link into the exact slot incarnation covering its deadline; the
// pair CAS validates head and baseTick together. cursor is a routing hint and
// must not be ahead of the confirmed sweep (stale is fine - it only costs
// descend iterations). Returns non-zero when published (the link then belongs
// to the wheel and may be delivered/recycled immediately); zero when the
// deadline's window is already swept - the link stays with the caller, which
// must expire the operation instead of parking the link a rotation late.
int timerWheelInsert(asyncBase *base, asyncOpListLink *link, uint64_t cursor);

// Lock-free sweep protocol, safe to run from any number of threads. Detach
// atomically takes the whole chain of the window starting at windowStart and
// reopens the slot for the next rotation (0 = already visited or empty); the
// winner delivers/drops/re-cascades its private chain with
// timerWheelProcessDetached. timerWheelSweepTick composes the level visits of
// one tick in the required order and confirms the cursor with the exact
// tick->tick+1 CAS - any thread sweeping the same tick helps a stalled one.
asyncOpListLink *timerWheelDetach(asyncBase *base, unsigned level, uint64_t windowStart);
void timerWheelProcessDetached(asyncBase *base, asyncOpListLink *link, uint64_t windowStart);
void timerWheelSweepTick(asyncBase *base, uint64_t tick);

void addToTimeoutQueue(asyncBase *base, asyncOpRoot *op);
void processTimeoutQueue(asyncBase *base, uint64_t currentTick);

// Sleep handshake of the message loops with the timeout grid. Before
// blocking, a loop publishes the tick it will wake at on its own and gets its
// wait bounded to one grid tick while any timer is parked in the wheel; the
// publication and the outstanding-counter re-read are ordered by one seq-cst
// RMW, pairing with the increment in addToTimeoutQueue - either the loop sees
// the fresh link and shortens the sleep, or the producer sees the published
// horizon and wakes the loop through methodImpl.wakeupLoop. After the wait
// returns, the slot is parked at UINTPTR_MAX (awake threads sweep on their
// own and must not attract kicks). Returns the wait bound in milliseconds.
uint32_t timerLoopPrepareSleep(asyncBase *base, unsigned threadId, uint32_t fallbackMs);
void timerLoopCancelSleep(asyncBase *base, unsigned threadId);

// Monotonic (wall-clock-independent) 125 ms ticks for the timeout grid; see
// the definition in asyncioImpl.c for why the grid must not use time(0).
// Returns an unsigned 64-bit tick count, not calendar time, so it is
// intentionally not a time_t (whose width is platform-dependent and which
// connotes wall time).
uint64_t getMonotonicTicks(void);

// Deadline in grid ticks: rounds the microsecond endTime up so a timeout
// never fires before its deadline.
__NO_UNUSED_FUNCTION_BEGIN
static inline uint64_t timerDeadlineTick(uint64_t endTime)
{
  return endTime / TIMER_TICK_MICROSECONDS + (endTime % TIMER_TICK_MICROSECONDS != 0);
}
__NO_UNUSED_FUNCTION_END

// Grace period (see the asyncBase comment). graceThreadEnter runs once per
// loop thread right after the messageLoopThreadId assignment and freezes
// reclamation on slot overflow or on an id collision. graceQuiesce ticks
// the calling thread's quiescence counter and drives reclamation; call it
// at the top of the message loop, right before blocking in the kernel
// wait. graceRetire parks a dead object in the limbo stack; memoryRelease
// is the memory half of its destructor and runs once every loop thread
// active at the batch capture has passed a quiescent point.
// graceThreadExit is the loop exit path: it stamps the slot out with
// UINTPTR_MAX and drains via graceReclaim - the last thread out drains both
// the pending batch and the stack. Call it BEFORE decrementing the loop
// thread counter: once the counter drops, a future loop thread may adopt
// this id, and a stale live stamp would shield its batches from the grace
// period.
void graceThreadEnter(asyncBase *base);
void graceQuiesce(asyncBase *base);
void graceThreadExit(asyncBase *base);
void graceRetire(asyncBase *base, aioObjectRoot *object, aioObjectDestructor *memoryRelease);
void graceReclaim(asyncBase *base);

int copyFromBuffer(void *dst, size_t *offset, struct ioBuffer *src, size_t size);
#ifdef __cplusplus
}

#endif

#endif //__ASYNCIO_ASYNCIOIMPL_H_
