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
  // Occupancy bitmap, one bit per slot (64-bit words - the wheel already
  // requires a 64-bit target). The loops derive their sleep horizon from it
  // instead of polling at the grid tick while links are parked. Protocol:
  // a producer sets the bit with a seq-cst RMW after publishing into a slot
  // it observed empty (activation pays the extra RMW, not every link); a
  // visitor clears the bit right before its drain CAS, so a publication into
  // the reopened incarnation is ordered after the clear and its set wins, and
  // an idempotent visitor that lost the drain race re-sets the bit when it
  // still observes a chain. Invariant: a slot holding a chain has its bit set
  // (or the publisher is still between the publish and the set, which the
  // wakeup kick in addToTimeoutQueue covers); a set bit over an empty slot is
  // legal and only costs a spurious wakeup - the next visit clears it.
  uintptr_t occupancy[TIMER_WHEEL_LEVELS][TIMER_WHEEL_SLOTS / 64];
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
typedef int activateTy(aioUserEvent*);

typedef enum EventTimerUpdate {
  etuStart,
  etuStop,
  etuRearm,
  // Reactor-only readiness probe. The backend consumes its timer channel
  // under the common applier and returns 2 for a real tick, 1 for stale/EAGAIN.
  etuConsume
} EventTimerUpdate;

// User-event timers have a separate serialized control plane. The common
// event applier is the sole caller; backend callbacks only publish ticks.
// generation is meaningful for start/rearm and travels back with a tick.
typedef int updateEventTimerTy(aioUserEvent*, EventTimerUpdate, uintptr_t, uint64_t);
typedef void releaseUserEventTy(aioUserEvent*);

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
  updateEventTimerTy *updateEventTimer;
  releaseUserEventTy *releaseUserEvent;
};

struct asyncBase {
  enum AsyncMethod method;
  struct asyncImpl methodImpl;
  struct ConcurrentQueue globalQueue;
  // User-event storage is base-local. A pooled slot can therefore never keep
  // a timer resource or grace domain belonging to another multiplexor.
  struct ConcurrentQueue eventPool;
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

  // Producer/applier hot line. Operation storage is cache-line aligned, and
  // the compile-time checks below keep every field touched by activation,
  // timer-signal publication and mailbox election in one cache line. A timer
  // tick therefore takes ownership of one line instead of bouncing tag and
  // timerSignals on two different lines.
  volatile uintptr_t tag;
  asyncBase *base;

  // {encoded signal count, armed generation}. low = count<<1 | probeKind:
  // kqueue/IOCP publish confirmed ticks (kind 0), epoll publishes readiness
  // probes (kind 1) whose timerfd read is serialized by the applier. A signal
  // may increment only the installed generation; stop/restart changes the
  // pair first, making a late publisher fail its CAS instead of touching the
  // next schedule. One backend uses exactly one kind, so both fit one pair.
  volatile uint128Pair timerSignals;

  // {waiter coroutine pointer, metadata}. With a waiter, metadata is the
  // ioSleep timer generation (0 for ioWaitUserEvent); without one it is the
  // accumulated activation-credit count. Changed only by DWCAS.
  volatile uint128Pair waiter;

  // Coalesced control task state. Its queue node is event->root with the free
  // low pointer bit set (all operation storage is 64-byte aligned), avoiding a
  // second full asyncOpRoot in every event.
  volatile unsigned timerControlState;
  int isSemaphore;
  int timerRemaining;
  unsigned timerState;

  // Cold configuration/lifetime line. Timer Start/Stop has one external
  // controller; sequence is even while stable and odd only during the
  // forbidden overlapping-writer window. Every snapshot field is atomic so
  // even an observing retry is valid C and TSan-clean.
  userEventDestructorCb *destructorCb;
  void *destructorCbArg;
  volatile uintptr_t timerConfigSequence;
  volatile uintptr_t timerConfigPeriod;

  // Consumer-owned applied state (only eventTimerProcess touches it).
  uintptr_t timerAppliedGeneration;
  uint64_t timerAppliedPeriod;
  volatile unsigned timerConfigCounter;
};

typedef char aioUserEventMustStayFourCacheLines[
  sizeof(struct aioUserEvent) == 4 * CACHE_LINE_SIZE ? 1 : -1];
typedef char aioUserEventHotFieldsMustShareOneCacheLine[
  offsetof(struct aioUserEvent, tag) / CACHE_LINE_SIZE ==
  offsetof(struct aioUserEvent, timerState) / CACHE_LINE_SIZE ? 1 : -1];

enum EventTimerStateFlags {
  etsArmed = 1,
  etsNeedsRearm = 2,
  etsHasReference = 4
};

enum EventControlStateFlags {
  ecsRunning = 1,
  ecsDirty = 2
};

// Drop a protocol-local claim while another owner is known to pin the event.
// Unlike a general release this decrement cannot be final and publishes no
// payload, so release ordering would only add an unnecessary `l` suffix to
// ARM64 ldadd. Keep uses limited to paths whose ownership invariant proves
// refs > count independently of the value observed here.
static inline void eventReferenceDropNonFinal(aioUserEvent *event, uintptr_t count)
{
  assert(count > 0 && count <= TAG_EVENT_REF_MASK);
  uintptr_t old = __uintptr_atomic_fetch_and_add(&event->tag,
                                                  (uintptr_t)0 - count,
                                                  amoRelaxed);
  (void)old;
  assert((old & TAG_EVENT_REF_MASK) > count &&
         "A supposedly non-final event reference was the last one");
}

static inline int eventReferenceMarkDeleting(aioUserEvent *event)
{
  uintptr_t old = __uintptr_atomic_fetch_or(&event->tag,
                                             TAG_EVENT_DELETE,
                                             amoRelaxed);
  assert((old & TAG_EVENT_REF_MASK) != 0 && "Deleting an already released event");
  return (old & TAG_EVENT_DELETE) == 0;
}

static inline int eventReferenceIsDeleting(aioUserEvent *event)
{
  // DELETE is only a sticky control bit; no payload is published through it.
  return (__uintptr_atomic_load(&event->tag, amoRelaxed) & TAG_EVENT_DELETE) != 0;
}

static inline int eventReferenceTryActivate(aioUserEvent *event)
{
  if (event->isSemaphore) {
    // The caller owns another reference, so rolling back a terminal optimistic
    // retain cannot itself recycle the event.
    uintptr_t old = __uintptr_atomic_fetch_and_add(&event->tag, 1, amoRelaxed);
    if (old & TAG_EVENT_DELETE) {
      __uintptr_atomic_fetch_and_add(&event->tag, (uintptr_t)0 - 1, amoRelaxed);
      return 0;
    }
#ifndef NDEBUG
    uintptr_t refs = old & TAG_EVENT_REF_MASK;
    assert(refs != 0 && "Activation without an owned event reference");
    assert(refs != TAG_EVENT_REF_MASK && "Event reference overflow");
#endif
    return 1;
  }

  uintptr_t old = __uintptr_atomic_load(&event->tag, amoRelaxed);
  for (;;) {
    if (old & (TAG_EVENT_DELETE | TAG_EVENT_OP))
      return 0;
#ifndef NDEBUG
    uintptr_t refs = old & TAG_EVENT_REF_MASK;
    assert(refs != 0 && "Activation without an owned event reference");
    assert(refs != TAG_EVENT_REF_MASK && "Event reference overflow");
#endif
    uintptr_t desired = (old + 1) | TAG_EVENT_OP;
    if (__uintptr_atomic_compare_exchange(&event->tag,
                                           &old,
                                           desired,
                                           amoRelaxed))
      return 1;
  }
}

static inline void eventReferenceDeactivate(aioUserEvent *event)
{
  if (event->isSemaphore)
    return;
  uintptr_t old = __uintptr_atomic_fetch_and(&event->tag,
                                              ~TAG_EVENT_OP,
                                              amoRelaxed);
  (void)old;
  assert((old & TAG_EVENT_OP) && "Deactivating an event without a pending delivery");
}

enum { eventTimerControlTag = 1 };

static inline asyncOpRoot *eventTimerControlNode(aioUserEvent *event)
{
  return (asyncOpRoot*)((uintptr_t)&event->root | eventTimerControlTag);
}

static inline int eventTimerIsControlNode(asyncOpRoot *node)
{
  return ((uintptr_t)node & eventTimerControlTag) != 0;
}

static inline aioUserEvent *eventTimerControlEvent(asyncOpRoot *node)
{
  return (aioUserEvent*)((uintptr_t)node & ~(uintptr_t)eventTimerControlTag);
}

void eventTimerSignalTick(aioUserEvent *event, uintptr_t generation);
void eventTimerSignalProbe(aioUserEvent *event, uintptr_t generation);
void eventTimerProcess(aioUserEvent *event);

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
// blocking, a loop publishes the fallback horizon (currentTick + the fallback
// in ticks), then derives the real wake tick from the occupancy bitmap: the
// nearest occupied level-0 slot ahead of the sweep and the next visit
// boundary of an occupied upper-level slot (a cascade must not oversleep its
// boundary). Every bitmap word the decision rests on is read with a seq-cst
// RMW, pairing with the producer's set of the same word - either the scan
// sees the fresh bit and the sleep shrinks to the exact wake tick, or the
// producer sees the published horizon and wakes one loop through
// methodImpl.wakeupLoop. An empty window keeps the full fallback: an idle
// base wakes at the backend period, never at the grid tick (tickless).
// After the wait returns, the slot is parked at UINTPTR_MAX (awake threads
// sweep on their own and must not attract kicks). Returns the wait bound in
// milliseconds; currentTick is the caller's getMonotonicTicks() reading.
uint32_t timerLoopPrepareSleep(asyncBase *base, unsigned threadId, uint64_t currentTick, uint32_t fallbackMs);
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
