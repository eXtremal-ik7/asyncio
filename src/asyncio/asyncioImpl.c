#include "asyncioImpl.h"
#include "asyncio/coroutine.h"
#include "atomic.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#ifndef OS_WINDOWS
#include <signal.h>
extern int sigpipeIgnored;
#endif

__tls unsigned currentFinishedSync;
__tls unsigned messageLoopThreadId;
__tls asyncBase *loopThreadBase;
static __tls unsigned loopThreadSlotOwned;

#ifdef OS_WINDOWS
asyncBase *iocpNewAsyncBase(void);
#endif
#ifdef OS_LINUX
asyncBase *epollNewAsyncBase(void);
#endif
#if defined(OS_DARWIN) || defined(OS_FREEBSD)
asyncBase *kqueueNewAsyncBase(void);
#endif

int initializeAsyncIo(AsyncInitFlags flags)
{
#ifdef OS_WINDOWS
  (void)flags;
  WSADATA wsadata;
  return WSAStartup(MAKEWORD(2, 2), &wsadata);
#else
  if (flags & aiIgnoreSigpipe) {
    struct sigaction ignoreAction;
    memset(&ignoreAction, 0, sizeof(ignoreAction));
    ignoreAction.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &ignoreAction, 0) == -1)
      return -1;
    sigpipeIgnored = 1;
  }
  return 0;
#endif
}

asyncBase *createAsyncBase(AsyncMethod method, unsigned loopThreads)
{
  unsigned loopThreadLimit = loopThreads ? loopThreads : 1;
  const size_t wordBits = sizeof(uintptr_t) * 8;
  size_t loopThreadSlotWords = ((size_t)loopThreadLimit + wordBits - 1) / wordBits;
  TimerLoopState *timerLoopStates = (TimerLoopState*)alignedMalloc(sizeof(TimerLoopState) * (size_t)loopThreadLimit, CACHE_LINE_SIZE);
  uintptr_t *loopThreadSlots = (uintptr_t*)calloc(loopThreadSlotWords, sizeof(uintptr_t));
  if (!timerLoopStates || !loopThreadSlots) {
    alignedFree(timerLoopStates);
    free(loopThreadSlots);
    return 0;
  }

  // Each supported OS has exactly one backend, so the method parameter cannot
  // select anything different from the OS default; it stays in the signature
  // for a platform with alternative backends.
  __UNUSED(method);
#if defined(OS_WINDOWS)
  asyncBase *base = iocpNewAsyncBase();
#elif defined(OS_LINUX)
  asyncBase *base = epollNewAsyncBase();
#elif defined(OS_DARWIN) || defined(OS_FREEBSD)
  asyncBase *base = kqueueNewAsyncBase();
#else
#error "Unsupported OS: no I/O multiplexor backend available"
#endif

  if (!base) {
    alignedFree(timerLoopStates);
    free(loopThreadSlots);
    return 0;
  }

#ifndef NDEBUG
  base->opsCount = 0;
#endif
  timerWheelInit(base, getMonotonicTicks());
  memset(&base->globalQueue, 0, sizeof(base->globalQueue));
  memset(&base->eventPool, 0, sizeof(base->eventPool));
  base->messageLoopThreadCounter = 0;
  base->quitRequested = 0;
  base->timerLoopStates = timerLoopStates;
  base->loopThreadSlots = loopThreadSlots;
  base->loopThreadSlotWords = (unsigned)loopThreadSlotWords;
  base->loopThreadLimit = loopThreadLimit;
  base->timerPreclearOverflow = 0;
  for (unsigned i = 0; i < loopThreadLimit; i++) {
    base->timerLoopStates[i].wakeTick = UINTPTR_MAX;
    base->timerLoopStates[i].preclearSequence = 0;
  }
  return base;
}

void asyncLoop(asyncBase *base)
{
  base->methodImpl.nextFinishedOperation(base);
}

void postQuitOperation(asyncBase *base)
{
  // Level, not edge: the sticky word stops the threads already looping, the
  // ones asleep in the kernel wait and the ones that have not entered
  // asyncLoop yet (see the litmus at asyncBase.quitRequested). One doorbell
  // is enough - each exiting thread re-rings while others remain. The
  // RMW-plus-write pair keeps this callable from a signal handler: no queue
  // traffic, no allocation, no lock.
  __uintptr_atomic_fetch_or(&base->quitRequested, 1, amoSeqCst);
  base->methodImpl.wakeupLoop(base);
}

void resetQuitOperation(asyncBase *base)
{
  // Quiescent-only by contract: every asyncLoop returned and no concurrent
  // postQuitOperation/asyncLoop/reset. The store stays atomic, so a racing
  // quit is not UB - it lands on the old round (erased here) or on the next
  // one, unspecified which. Everything else the loops touched is already in
  // its entry state after a clean exit (slots and counter zero, horizons
  // parked at UINTPTR_MAX, leftover queue items and armed timers are next
  // round's legitimate work; a residual doorbell costs one spurious wakeup).
  assert(__uint_atomic_load(&base->messageLoopThreadCounter, amoSeqCst) == 0 &&
         "resetQuitOperation while asyncLoop threads are still registered");
#ifndef NDEBUG
  for (unsigned i = 0; i < base->loopThreadSlotWords; i++)
    assert(__uintptr_atomic_load(&base->loopThreadSlots[i], amoSeqCst) == 0 &&
           "resetQuitOperation while a loop slot is still owned");
#endif
  __uintptr_atomic_store(&base->quitRequested, 0, amoRelaxed);
}

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

static void opArmTimer(asyncOpRoot *op)
{
  if (op->timeout) {
    asyncBase *base = op->object->header.base;
    if (op->flags & afRealtime) {
      // start timer for this operation
      base->methodImpl.startTimer(op);
    } else {
      // Keep the grid path in ticks throughout. Match the former
      // saturating-microsecond semantics: UINT64_MAX us rounded up is the
      // largest representable deadline, not UINT64_MAX grid ticks.
      uint64_t nowTick = getMonotonicTicks();
      uint64_t timeout = op->timeout;
      uint64_t deltaTick = timeout / TIMER_TICK_MICROSECONDS +
                           (timeout % TIMER_TICK_MICROSECONDS != 0);
      const uint64_t deadlineLimit =
        UINT64_MAX / TIMER_TICK_MICROSECONDS +
        (UINT64_MAX % TIMER_TICK_MICROSECONDS != 0);
      op->deadlineTick = nowTick >= deadlineLimit ||
                         deltaTick > deadlineLimit - nowTick
                           ? deadlineLimit : nowTick + deltaTick;
      addToTimeoutQueue(base, op);
    }
  }
}

static void opRun(asyncOpRoot *op, List *list)
{
  eqPushBack(list, op);
  opArmTimer(op);
}

// Complete an operation whose start node has not armed a timer or issued I/O.
// opRelease() deliberately is not used: a realtime operation owns a timer cell
// from allocation onward, but stopTimer is only legal after that cell was
// actually armed. The operation-specific release hook still has to run before
// the completion is published (large captured buffers may otherwise leak).
static void opReleaseUnstarted(asyncOpRoot *op, AsyncOpStatus status)
{
  opSetStatus(op, opGetGeneration(op), status);
  assert(opGetStatus(op) != aosPending);
  if (op->releaseMethod)
    op->releaseMethod(op);
  addToGlobalQueue(op);
}

// One-shot transport initialization does not live in the read/write queues.
// Its slot is assigned by the combiner while processing the init start node;
// ordinary operations submitted afterwards may queue, but cannot start
// until initialization leaves the slot. Leaving the slot kicks both queues; a
// failure also cancels everything queued behind it with the same status.
static void initializationRelease(asyncOpRoot *op, AsyncOpStatus status, uint32_t *needStart)
{
  aioObjectRoot *object = op->object;
  if (object->initializationOp == op)
    object->initializationOp = 0;
  opRelease(op, status, 0);
  if (status != aosSuccess) {
    cancelOperationList(&object->readQueue, status);
    cancelOperationList(&object->writeQueue, status);
  }
  if (needStart)
    *needStart |= IO_EVENT_READ | IO_EVENT_WRITE;
}

// Same slot teardown before opArmTimer(): no backend timer exists to stop.
static void initializationReleaseUnstarted(asyncOpRoot *op, AsyncOpStatus status, uint32_t *needStart)
{
  aioObjectRoot *object = op->object;
  if (object->initializationOp == op)
    object->initializationOp = 0;
  opReleaseUnstarted(op, status);
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
  asyncOpRoot *op = object->initializationOp;
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
  asyncOpRoot *op = object->initializationOp;
  if (!op)
    return;
  if (opSetStatus(op, opGetGeneration(op), status)) {
    if (op->running == arRunning) {
      op->running = arCancelling;
      if (op->cancelMethod(op))
        initializationRelease(op, status, 0);
    } else if (op->running == arWaiting) {
      // The slot is assigned only while its start node is being processed, so
      // a visible waiting operation has no timer or I/O in flight and is safe
      // to finish immediately.
      initializationReleaseUnstarted(op, status, 0);
    }
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

  // Central rejection for a genuinely unstarted node. arRunning operations
  // may already own child/kernel I/O and must enter their normal queue/slot so
  // the existing cancel path can retain them until the late completion.
  if (op->running == arWaiting &&
      __uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    opReleaseUnstarted(op, aosCanceled);
    return;
  }

  if (op->opCode & OPCODE_INIT) {
    // Only the combiner owner touches the slot. A duplicate init request is a
    // client error, but its operation lifecycle still has to complete safely.
    if (object->initializationOp) {
      opReleaseUnstarted(op, aosUnknownError);
      return;
    }
    object->initializationOp = op;

    if (opGetStatus(op) != aosPending) {
      // A direct operation cancellation won before its start node was handled.
      initializationReleaseUnstarted(op, opGetStatus(op), needStart);
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
  if (opGetStatus(op) != aosPending) {
    reapObject(object, COMBINER_TAG_CANCEL, needStart);
    return;
  }
  if (list->head && list->head->running == arWaiting)
    *needStart |= tag;
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

  asyncOpRoot *ex = object->initializationOp;
  if (ex) {
    AsyncOpStatus status = opGetStatus(ex);
    switch (combinerSelectReapAction(ex->running, status)) {
      case craCancel:
        ex->running = arCancelling;
        if (ex->cancelMethod(ex))
          initializationRelease(ex, status, needStart);
        break;

      case craRelease:
        if (ex->running == arWaiting)
          initializationReleaseUnstarted(ex, status, needStart);
        else
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
  if (op && op->object->initializationOp)
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

TimerLoopState *loopThreadEnter(asyncBase *base)
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
      return &base->timerLoopStates[id];
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
  __uintptr_atomic_store(&base->timerLoopStates[id].wakeTick, UINTPTR_MAX,
                         amoRelaxed);
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
static inline int combinerDispatch(aioObjectRoot *object, combinerTaskHandlerTy *taskHandler, asyncOpRoot *op, uint32_t tag)
{
  if (combinerTaskHandlerCommon(object, tag))
    return 1;
  // Pair a child/kernel completion with the sticky delete sweep before the
  // backend consumes its sole progress signal. A retained cancelling head is
  // then released by that same pass instead of waiting for a wake already used.
  if ((tag & COMBINER_TAG_PROGRESS_MASK) &&
      __uint_atomic_load(&object->DeletePending, amoRelaxed))
    cancelAllObjectOperations(object);
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
