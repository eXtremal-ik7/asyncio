#ifndef __ASYNCIO_ASYNCIOIMPL_H_
#define __ASYNCIO_ASYNCIOIMPL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/api.h"
#include "asyncio/ringBuffer.h"
#include "atomic128.h"
#include <stdlib.h>
#ifndef OS_WINDOWS
#include <errno.h>
#include <signal.h>
#endif

#define TAGGED_POINTER_DATA_SIZE 6
#define TAGGED_POINTER_ALIGNMENT (((intptr_t)1) << TAGGED_POINTER_DATA_SIZE)
#define TAGGED_POINTER_DATA_MASK (TAGGED_POINTER_ALIGNMENT - 1)
#define TAGGED_POINTER_PTR_MASK (~TAGGED_POINTER_DATA_MASK)

// Timeout grid: an exact cascading hierarchical timer wheel. The tick is a
// compile-time constant: changing the resolution later renumbers deadlines
// but does not touch the architecture.
#define TIMER_TICK_MICROSECONDS 125000
#ifndef TIMER_WHEEL_LEVEL_BITS
#define TIMER_WHEEL_LEVEL_BITS 10
#endif
#define TIMER_WHEEL_SLOTS (1u << TIMER_WHEEL_LEVEL_BITS)
#ifndef TIMER_WHEEL_LEVELS
#define TIMER_WHEEL_LEVELS 4
#endif

// Slot = atomic {head, baseTick} pair changed only by DWCAS: drain and reopen
// are one transition, and a link can never be published into a foreign slot
// incarnation unnoticed - the pair CAS validates baseTick together with the
// head. head is an intrusive LIFO of asyncOpListLink (low word), baseTick is
// the absolute tick starting the slot's current window (high word); baseTick
// only grows, in steps of the level's rotation period, which kills ABA for
// the whole 64-bit range. Level L covers windows of 2^(10L) ticks; deadlines
// beyond level 3 clamp into its farthest slot and re-cascade on visit.
typedef struct timerWheel {
  uint128 slots[TIMER_WHEEL_LEVELS][TIMER_WHEEL_SLOTS];
  // Occupancy bitmap, one bit per slot (64-bit words - the wheel already
  // requires a 64-bit target). The loops derive their sleep horizon from it
  // instead of polling at the grid tick while links are parked. Protocol:
  // every producer sets the bit with a seq-cst RMW after its publish CAS -
  // unconditionally, not only on the empty -> non-empty transition, so an
  // arm stacked onto a live chain never depends on the first publisher
  // (possibly stalled between its CAS and its set) for visibility; a
  // sleeper publishes its horizon seq-cst before scanning the words with
  // seq-cst loads, and the producer reads those horizons seq-cst after the
  // bit set. The resulting store/load order closes the lost-wakeup race
  // without a locked fetch-add(0) for every scanned bitmap word. A
  // visitor clears the bit right before its drain CAS, so a publication into
  // the reopened incarnation is ordered after the clear and its set wins, and
  // an idempotent visitor that lost the drain race re-sets the bit when it
  // still observes a chain. Invariant: a slot holding a chain published by a
  // returned insert has its bit set - a producer still inside the insert has
  // completed nothing anyone waits on, and its own set runs before it
  // returns - EXCEPT while some visit sits between its bit clear and its
  // drain CAS. The bit carries no incarnation number, so a stale visitor can
  // clear the bit of the chain living in the slot now; each visitor keeps a
  // pre-clear bracket open for that window (TimerSleepSlot.preclearSequence
  // odd / overflow word active) and timerLoopPrepareSleep validates its scan
  // against the brackets. A set bit over an empty slot is legal and only
  // costs a spurious wakeup - the next visit clears it.
  uintptr_t occupancy[TIMER_WHEEL_LEVELS][TIMER_WHEEL_SLOTS / 64];
} timerWheel;

#define CACHE_LINE_SIZE 64

// Published sleep horizon of one loop thread: the absolute grid tick it will
// wake at on its own, UINTPTR_MAX while awake, timerSleepEternal while
// blocked in a wait without a timeout. A producer arming a deadline no
// sleeper would meet in time kicks one loop through the backend wakeup; the
// eternal sentinel is farther than any real tick, so any published deadline
// finds it late and kicks. Cache-line padding prevents neighbouring loop
// threads from false-sharing their per-wait publications.
typedef struct TimerSleepSlot {
  volatile uintptr_t wakeTick;
  // Pre-clear bracket of this loop thread's window visits: odd between the
  // occupancy pre-clear and the drain CAS resolution, +2 per visit.
  // Single-writer plain stores; the open rides on the seq-cst clear that
  // follows it, the close is a release over the bit repair.
  volatile uintptr_t preclearSequence;
  uint8_t pad[CACHE_LINE_SIZE - 2 * sizeof(uintptr_t)];
} TimerSleepSlot;

static const uintptr_t timerSleepEternal = UINTPTR_MAX - 1;

// Pre-clear bracket word for visitors without an own TimerSleepSlot: low
// half = visitors inside a bracket, high half = brackets ever entered.
static const uintptr_t timerPreclearOverflowEntry = ((uintptr_t)1 << 32) + 1;
static const uintptr_t timerPreclearOverflowActiveMask = ((uintptr_t)1 << 32) - 1;

typedef enum IoActionTy {
  actAccept = OPCODE_READ,
  actRead,
  actReadMsg,
  actConnect = OPCODE_WRITE,
  actWrite,
  actWriteMsg
} IoActionTy;

// (object, startOp, sig): startOp != 0 is an op-node to start; sig is the
// accumulated signal bits (COMBINER_TAG_MOVE_*/CANCEL) to reconcile for this
// node. Either may be empty.
typedef void combinerTaskHandlerTy(aioObjectRoot*, asyncOpRoot*, uint32_t);
typedef void enqueueOperationTy(asyncBase*, asyncOpRoot*);
typedef void loopWakeupTy(asyncBase*);
typedef void nextFinishedOperationTy(asyncBase*);
typedef aioObject *newAioObjectTy(asyncBase*, IoObjectTy, void*);
typedef void startTimerTy(asyncOpRoot*);
typedef void stopTimerTy(asyncOpRoot*);
typedef int initializeUserEventTy(aioUserEvent*);
typedef int activateTy(aioUserEvent*);

typedef enum EventTimerUpdate {
  etuStart,
  etuStop
} EventTimerUpdate;

// User-event timers have a separate serialized control plane. The thread
// which acquires timerControl.OWNER is the sole backend caller; another
// publisher updates the same DWCAS word and lets that owner reconcile it.
// generation is meaningful for start and travels back with a tick.
typedef int updateEventTimerTy(aioUserEvent*, EventTimerUpdate, uint32_t, uint64_t);
// Consume kernel inputs already published in timerControl. epoll returns the
// accumulated timerfd count, while kqueue/IOCP return the published count.
// A zero published count asks the backend to drain/rearm an input discarded by
// a USER-owner. IOCP uses the OWNER-only remaining count when deciding rearm.
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
  // Pure kick of one sleeping loop thread, no queue traffic: eventfd write /
  // EVFILT_USER trigger / posted completion packet. Serves both the timer
  // producers and the quit doorbell; async-signal-safe (one write syscall)
  loopWakeupTy *wakeupLoop;
  updateEventTimerTy *updateEventTimer;
  consumeEventTimerTickTy *consumeEventTimerTick;
  releaseUserEventTy *releaseUserEvent;
};

struct asyncBase {
  struct asyncImpl methodImpl;
  struct ConcurrentQueue globalQueue;
  // User-event storage is base-local. A pooled slot can therefore never keep
  // a timer resource belonging to another multiplexor.
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
  // per-iteration timer-wheel traffic
  uint8_t timerCloseCursorPadBefore[CACHE_LINE_SIZE - sizeof(uintptr_t)];
  uintptr_t timerCloseCursor;
  uint8_t timerCloseCursorPadAfter[CACHE_LINE_SIZE - sizeof(uintptr_t)];
  // One published sleep horizon per loop thread. loopThreadSlots owns the
  // same index space and hands every concurrent asyncLoop invocation a unique
  // slot with one fetch-or at entry and one fetch-and at exit. Entry is
  // rejected before either array is indexed when the declared limit is full.
  TimerSleepSlot *timerSleep;
  uintptr_t *loopThreadSlots;
  unsigned loopThreadSlotWords;
  unsigned loopThreadLimit;
  volatile unsigned messageLoopThreadCounter;
  // Shared pre-clear bracket for visitors without an own TimerSleepSlot
  // (layout at timerPreclearOverflowEntry)
  volatile uintptr_t timerPreclearOverflow;
  // Sticky level-triggered quit (postQuitOperation sets, resetQuitOperation
  // clears in quiescence). Every loop iteration reads it after draining the
  // global queue and exits when set; the exiting thread re-rings wakeupLoop
  // while other threads remain registered, so one doorbell reaches every
  // sleeper. A late entrant cannot miss it: if the last exiting thread saw
  // remaining == 0 (and skipped the re-ring), the entrant's counter increment
  // reads-from that exit's decrement, which is program-ordered after the
  // exiting thread's load of this word - the entrant's own first-iteration
  // load happens-after the poster's seq-cst set through that RMW chain. A
  // thread already inside the kernel wait is covered by the doorbell instead:
  // the write either precedes the wait entry (the fd stays readable, the wait
  // returns at once) or wakes it
  uintptr_t quitRequested;

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

// Default read-ahead capacity of a stream object: reads smaller than the
// capacity are served from one large recv instead of a syscall each. Pooled
// objects keep the largest capacity ever requested across recycles.
#define DEFAULT_SOCKET_BUFFER_SIZE 16384

__NO_UNUSED_FUNCTION_BEGIN
static inline void ioBufferEnsureCapacity(struct ioBuffer *buffer, size_t size)
{
  if (size > buffer->totalSize) {
    buffer->ptr = realloc(buffer->ptr, size);
    buffer->totalSize = size;
    poolCacheHandoff(buffer->ptr);
  }
}
__NO_UNUSED_FUNCTION_END

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
    return initializationState == arRunning ? (initializationIsWrite ? IO_EVENT_WRITE : IO_EVENT_READ) : 0;

  return (hasReadQueue ? IO_EVENT_READ : 0) | (hasWriteQueue ? IO_EVENT_WRITE : 0);
}

static inline uint32_t combinerActiveIoEvents(aioObjectRoot *object)
{
  asyncOpRoot *initialization = (asyncOpRoot*)__uintptr_atomic_load(&object->initializationOp, amoRelaxed);
  if (initialization)
    return combinerSelectActiveIoEvents(1, initialization->running, initialization->opCode & OPCODE_WRITE, 0, 0);

  return combinerSelectActiveIoEvents(0, arWaiting, 0, object->readQueue.head != 0, object->writeQueue.head != 0);
}

typedef enum CombinerInitializationAction {
  ciaNone,
  ciaExecute,
  ciaRelease
} CombinerInitializationAction;

// Status and position select one mutually exclusive initialization action;
// execution itself remains outside this pure function.
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

// Map the errno of a failed stream/datagram socket syscall (recv/send/read/
// write/recvmsg/sendto) to an operation status. EAGAIN means the socket buffer
// is momentarily empty or full: the operation stays pending and is retried on
// the next readiness event. EPIPE (peer gone, broken pipe) and ENOTCONN (the
// socket was never connected) both normalize to aosNotConnected, so callers can
// tell an absent/lost connection from an opaque failure; ECONNRESET (TCP RST)
// is aosDisconnected, matching the kqueue EV_EOF and iocp WSAECONNRESET paths;
// every other errno collapses to aosUnknownError (errno is not otherwise
// surfaced by the API).
static inline AsyncOpStatus socketStatusFromErrno(int error)
{
  switch (error) {
    case EAGAIN: return aosPending;
    case EPIPE:
    case ENOTCONN: return aosNotConnected;
    case ECONNRESET: return aosDisconnected;
    default: return aosUnknownError;
  }
}
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

__NO_UNUSED_FUNCTION_BEGIN
// Lazily (re)creates the backend timer cell backing op->timerId; the cell is
// pooled together with the operation, so a successful creation is permanent.
// Returns the cell, or null when the backend could not allocate one - the
// caller must fail the operation instead of arming it.
static inline void *opEnsureTimerCell(asyncOpRoot *op)
{
  if (!op->timerId) {
    asyncBase *base = op->object->header.base;
    base->methodImpl.initializeTimer(base, op);
    poolCacheHandoff(op->timerId);
  }
  return op->timerId;
}

// Grows an operation-owned scratch buffer to at least `required` bytes. A
// null buffer always has zero size, so plain realloc covers the first
// allocation; the buffer never shrinks and is reused by pooled operations.
static inline void asyncOpEnsureInternalBuffer(void **buffer, size_t *bufferSize, size_t required)
{
  if (*bufferSize < required) {
    *buffer = realloc(*buffer, required);
    *bufferSize = required;
    poolCacheHandoff(*buffer);
  }
}
__NO_UNUSED_FUNCTION_END

typedef struct aioTimer aioTimer;

typedef struct aioTimerUserEventState {
  uint32_t remaining;
  uint32_t armed;
  // The ACTIVE control word no longer carries the period; the owner saves it
  // here at apply time (IOCP re-arms one-shot timers from it).
  uint64_t period;
} aioTimerUserEventState;

struct aioUserEvent {
  // Common kernel-visible prefix. tag.low contains references, DELETE and the
  // coroutine-waiter rendezvous bit; tag.high is the full monotonic
  // incarnation. A harvested kernel envelope may inspect only the tag and
  // immutable type until it has atomically claimed a matching reference.
  objectHeader header;

  // The rest of the first cache line is the complete manual/callback path.
  // Timer and coroutine state starts on the second line below.
  void *callback;
  void *arg;

  // Manual activations only. Semaphore mode counts every activation;
  // non-semaphore mode is a 0/1 pending gate. Timer delivery state lives in
  // the lazy backend timer and never contends on this hot producer word.
  volatile uintptr_t signalState;

  // Backend-owned manual activation identity: eventfd on epoll, the unique
  // EVFILT_USER ident on kqueue, unused on IOCP where pointer and full
  // generation occupy separate completion-packet fields.
  uint64_t activationId;

  // One DWCAS word is both the latest logical timer state and the owner
  // mailbox. It is a tagged union of two variants sharing the generation and
  // OWNER fields (bit 0 is the least-significant bit):
  //
  //   high 0..31  schedule generation (both variants)
  //        32..62 CONFIG: finite counter payload; ACTIVE: zero
  //        63     OWNER (both variants)
  //   low         CONFIG: period in microseconds (0..59, below 2^60)
  //               ACTIVE: full 64-bit pending-tick counter
  //
  // A user publication installs CONFIG with generation+1 and either acquires
  // a free OWNER or leaves the existing owner in place. The owner extracts
  // the parameters into aioTimerUserEventState, flips CONFIG to ACTIVE with
  // zero ticks in one CAS, and only then arms the backend. Ticks of a
  // generation therefore exist only while the word is ACTIVE of that same
  // generation, so every observer decides the variant by comparing
  // generations alone - no discriminator bit is stored. A tick publication
  // adds its whole batch to the low word in one CAS; the owner harvests it by
  // CASing low back to zero, exact at full 64-bit width. The release CAS
  // detects every concurrent publication, config and tick alike. Terminal
  // gating lives in header.tag's DELETE bit, checked in the publish loop.
  volatile uint128 timerControl;

  // Null until the first timer start. POSIX keeps this kernel-visible timer
  // paired with the pooled event cell; IOCP clears it after closing the timer.
  aioTimer *volatile timer;

  // One-word coroutine rendezvous. Tags identify ordinary/ioSleep waiters;
  // aligned nonzero values encode credits and zero is empty. Coroutine
  // pointers are at least four-byte aligned. Lifetime while installed belongs
  // to the helper's caller-owned strong reference, not to this word.
  volatile uintptr_t waiter;

  userEventDestructorCb *destructorCb;
  void *destructorCbArg;
};

typedef char aioUserEventMustFitTwoCacheLines[sizeof(struct aioUserEvent) <= 2 * CACHE_LINE_SIZE ? 1 : -1];
typedef char aioUserEventTimerStateStartsOnSecondCacheLine[offsetof(struct aioUserEvent, timerControl) == CACHE_LINE_SIZE ? 1 : -1];

static inline uint64_t eventHandleGeneration(aioUserEvent *event)
{
  return objectHeaderGeneration(&event->header);
}

static inline aioTimer *eventTimerLoad(aioUserEvent *event, AtomicMemoryOrder order)
{
  return (aioTimer*)__pointer_atomic_load((void* volatile*)&event->timer, order);
}

static inline void eventTimerStore(aioUserEvent *event, aioTimer *timer, AtomicMemoryOrder order)
{
  __pointer_atomic_store((void* volatile*)&event->timer, timer, order);
}

static inline aioTimerUserEventState *eventTimerState(aioTimer *timer)
{
  return (aioTimerUserEventState*)((uint8_t*)timer + sizeof(objectHeader));
}

// OWNER and the CONFIG counter live in the high word; the low word is the
// whole variant payload (period or pending ticks).
#define EVENT_TIMER_OWNER (1ULL << 63)
#define EVENT_TIMER_COUNTER_SHIFT 32

// CONFIG-variant accessors: valid only before the owner's ACTIVE flip.
static inline uint64_t eventTimerControlPeriod(uint128 control)
{
  return control.low;
}

static inline uint32_t eventTimerControlCounter(uint128 control)
{
  return (uint32_t)((control.high >> EVENT_TIMER_COUNTER_SHIFT) & 0x7FFFFFFF);
}

// ACTIVE-variant accessor.
static inline uint64_t eventTimerControlPendingTicks(uint128 control)
{
  return control.low;
}

static inline uint32_t eventTimerControlGeneration(uint128 control)
{
  return (uint32_t)control.high;
}

static inline uint32_t eventTimerGeneration(aioUserEvent *event)
{
  return (uint32_t)__uint64_atomic_load(&event->timerControl.high, amoRelaxed);
}

enum EventWaiterTag {
  ewtCredits = 0,
  ewtUser = 1,
  ewtSleep = 2,
  ewtDeleted = 3,
  ewtMask = 3,
  ewtCreditUnit = 4
};

static inline int eventReferenceIsDeleting(aioUserEvent *event)
{
  // DELETE is only a sticky control bit; no payload is published through it.
  return (__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_DELETE) != 0;
}

enum {
  // Queue pointers are 64-byte aligned. Tag 1 requests loop-context waiter
  // cancellation.
  eventQueueCancellationTag = 1,
  eventQueueTagMask = 3
};

static inline asyncOpRoot *eventCancellationNode(aioUserEvent *event)
{
  return (asyncOpRoot*)((uintptr_t)event | eventQueueCancellationTag);
}

static inline int eventIsQueueTask(asyncOpRoot *node)
{
  return ((uintptr_t)node & eventQueueTagMask) == eventQueueCancellationTag;
}

static inline aioUserEvent *eventQueueTaskEvent(asyncOpRoot *node)
{
  return (aioUserEvent*)((uintptr_t)node & ~(uintptr_t)eventQueueTagMask);
}

void eventTimerSignal(aioUserEvent *event, uint32_t timerGeneration, uint64_t eventGeneration, uint64_t tickCount);
// Executes waiter cancellation and releases its queue-owned reference.
void eventExecuteQueuedTask(asyncOpRoot *node);
void eventManualReady(aioUserEvent *event);
int eventTimerTryClaimReference(aioUserEvent *event, uint64_t eventGeneration);

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
// blocking, a loop publishes the eternal-sleep sentinel (the worst case the
// call may commit to), then derives the real wake tick from the occupancy
// bitmap: the earliest upcoming visit of an occupied slot across all levels
// (a cascade must not oversleep its boundary). Every bitmap word the
// decision rests on is read with a seq-cst load, pairing with the producer's
// seq-cst set of the same word - either the scan sees the fresh bit and the
// sleep shrinks to the exact wake tick, or the producer sees the published
// horizon and wakes one loop through methodImpl.wakeupLoop. The scan runs
// between pre-clear snapshots: an open visitor bracket caps the wait at one
// grid tick, a bracket closed mid-scan repeats the scan; only a clean empty
// scan makes the wait unbounded - UINT32_MAX, which the backends pass to the
// kernel as "no timeout".
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

// Shrinks a planned kernel-wait by the time elapsed since sleepFromTick: a
// preemption between planning and syscall must not extend the sleep past the
// wake. UINT32_MAX passes through. The instructions up to the syscall stay
// unguarded - closing that needs absolute kernel timers.
static inline uint32_t timerSleepShrinkElapsed(uint64_t sleepFromTick, uint32_t sleepMs)
{
  if (sleepMs == UINT32_MAX)
    return sleepMs;
  uint64_t elapsedMs = (getMonotonicTicks() - sleepFromTick) * (TIMER_TICK_MICROSECONDS / 1000);
  return elapsedMs < sleepMs ? sleepMs - (uint32_t)elapsedMs : 0;
}
__NO_UNUSED_FUNCTION_END

// Claims/releases the unique timerSleep slot for one asyncLoop invocation.
// Enter returns zero when callers exceed createAsyncBase(loopThreads);
// release returns the number of loop invocations still active after exit.
int loopThreadEnter(asyncBase *base);
unsigned loopThreadExit(asyncBase *base);

// The base whose message loop this thread is currently running (set between
// loopThreadEnter and loopThreadExit), zero on any other thread.
extern __tls asyncBase *loopThreadBase;

// An enqueue doorbell syscall exists to wake a sleeping loop thread. When the
// base can only ever have one loop thread and that thread is the caller, the
// wakeup is wasted: every backend loop fully drains the global queue right
// before it can go to sleep, so a completion the loop thread pushes itself is
// always consumed first, and the doorbell would only buy an extra empty loop
// turn. The quit marker (op == 0) always rings: it is posted from arbitrary
// threads, has to reach a sleeper, and is too rare to matter.
__NO_UNUSED_FUNCTION_BEGIN
static inline int enqueueNeedsDoorbell(asyncBase *base, asyncOpRoot *op)
{
  return !(op && base->loopThreadLimit == 1 && loopThreadBase == base);
}
__NO_UNUSED_FUNCTION_END

int copyFromBuffer(void *dst, size_t *offset, struct ioBuffer *src, size_t size);
#ifdef __cplusplus
}

#endif

#endif //__ASYNCIO_ASYNCIOIMPL_H_
