#pragma once

#include "reactor.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <utility>
#include <vector>

constexpr size_t kCombinerAlignment = size_t{1} << COMBINER_TAG_SIZE;

inline void destroyConcurrentQueue(ConcurrentQueue *queue)
{
  for (ConcurrentQueuePartition &partition: queue->Partitions) {
    free(partition.queue);
    partition.queue = nullptr;
    partition.enqueuePos = 0;
    partition.dequeuePos = 0;
  }
  queue->ReadPartition = 0;
  queue->WritePartition = 0;
}

struct TestBackend;

struct TestObject {
  aioObjectRoot root{};
  TestBackend *backend;
  unsigned destructorCallbacks = 0;
  uintptr_t destructorGeneration = 0;
  unsigned resourceDestructors = 0;
  unsigned memoryReleases = 0;

  explicit TestObject(TestBackend &owner);

  static void resourceDestructor(aioObjectRoot *root);
  static void destructorCallback(aioObjectRoot*, void *arg);
  static void memoryRelease(aioObjectRoot *root);
};

struct alignas(kCombinerAlignment) TestOp {
  asyncOpRoot root{};
  TestObject *owner;
  std::vector<AsyncOpStatus> results{aosPending};
  size_t resultIndex = 0;
  unsigned executeCalls = 0;
  unsigned cancelCalls = 0;
  unsigned releaseCalls = 0;
  unsigned finishCalls = 0;
  int cancelResult = 1;
  AsyncOpStatus callbackStatus = aosUnknown;
  std::function<void(TestOp&)> executeHook;
  std::function<void(TestOp&)> cancelHook;
  std::function<void(TestOp&)> finishHook;

  explicit TestOp(TestObject &object, int opCode = OPCODE_READ, AsyncFlags flags = afNone, uint64_t timeout = 0, bool hasCallback = true);

  void setResults(std::initializer_list<AsyncOpStatus> values) {
    results.assign(values);
    resultIndex = 0;
  }

  static AsyncOpStatus execute(asyncOpRoot *root);
  static int cancel(asyncOpRoot *root);
  static void finish(asyncOpRoot *root);
  static void release(asyncOpRoot *root);
};

static_assert(alignof(TestOp) >= kCombinerAlignment, "combiner nodes must be tag-aligned");
static_assert(offsetof(TestObject, root) == 0, "aioObjectRoot must be the first member");

struct TestBackend: asyncBase {
  asyncBase &base;
  ConcurrentQueue operationPool{};
  std::vector<asyncOpRoot*> completions;
  std::vector<asyncOpRoot*> recycled;
  std::vector<asyncOpRoot*> started;
  std::vector<aioTimer*> eventTimers;
  std::vector<uint32_t> handledSignals;
  unsigned startTimerCalls = 0;
  unsigned stopTimerCalls = 0;
  unsigned consumeTimerCalls = 0;
  unsigned initializeTimerCalls = 0;
  unsigned wakeupCalls = 0;
  unsigned activateCalls = 0;
  unsigned activateFailures = 0;
  uintptr_t nextTimerTag = 1;
  uint32_t lastEventTimerGeneration = 0;
  uint64_t lastEventTimerPeriod = 0;
  std::function<int(aioUserEvent*, EventTimerUpdate, uint32_t, uint64_t)> eventTimerHook;
  std::function<uint64_t(aioUserEvent*, uint64_t, uint32_t, uint64_t)> eventTimerConsumeHook;
  // Backs the loop-slot bitmap and timerSleep array that createAsyncBase
  // allocates for production bases.
  std::array<uintptr_t, 1> loopSlots{};
  alignas(CACHE_LINE_SIZE) std::array<TimerSleepSlot, 8> sleepSlots{};

  TestBackend() :
    asyncBase{},
    base(*this) {
    // Cursor 0 keeps the wheel's first rotation aligned with the small
    // absolute ticks the tests use as checkpoints
    timerWheelInit(&base, 0);
    base.methodImpl.combinerTaskHandler = taskHandler;
    base.methodImpl.enqueue = enqueue;
    base.methodImpl.initializeTimer = initializeTimer;
    base.methodImpl.startTimer = startTimer;
    base.methodImpl.stopTimer = stopTimer;
    base.methodImpl.initializeUserEvent = initializeUserEvent;
    base.methodImpl.activate = activateEvent;
    base.methodImpl.wakeupLoop = wakeupLoop;
    base.methodImpl.updateEventTimer = updateEventTimer;
    base.methodImpl.consumeEventTimerTick = consumeEventTimerTick;
    base.methodImpl.releaseUserEvent = releaseUserEvent;
    base.timerSleep = sleepSlots.data();
    base.loopThreadSlots = loopSlots.data();
    base.loopThreadSlotWords = static_cast<unsigned>(loopSlots.size());
    base.loopThreadLimit = static_cast<unsigned>(sleepSlots.size());
    for (TimerSleepSlot &slot: sleepSlots)
      slot.wakeTick = UINTPTR_MAX;
    currentFinishedSync = 0;
    messageLoopThreadId = 0;
  }

  ~TestBackend() {
    if (!completions.empty())
      ADD_FAILURE() << completions.size() << " completion(s) were not drained by the test";
    destroyConcurrentQueue(&operationPool);
    destroyConcurrentQueue(&base.globalQueue);
    destroyConcurrentQueue(&base.eventPool);
    timerWheelTeardown(&base);
    for (aioTimer *timer: eventTimers)
      alignedFree(timer);
  }

  void drainCompletions() {
    size_t index = 0;
    while (index < completions.size()) {
      asyncOpRoot *op = completions[index++];
      if (((uintptr_t)op & eventQueueTagMask) == eventQueueTagMask) {
        aioUserEvent *event = (aioUserEvent*)((uintptr_t)op & ~(uintptr_t)eventQueueTagMask);
        currentFinishedSync = 0;
        eventManualReady(event);
        eventDecrementReference(event, 1);
        continue;
      }
      if (eventIsQueueTask(op)) {
        eventExecuteQueuedTask(op);
        continue;
      }
      if (op->callback)
        op->finishMethod(op);
      releaseAsyncOp(op);

      void *item = nullptr;
      if (concurrentQueuePop(op->objectPool, &item))
        recycled.push_back(static_cast<asyncOpRoot*>(item));
      else
        ADD_FAILURE() << "released operation was not returned to its pool";
    }
    completions.clear();
  }

  static TestBackend &from(asyncBase *base) { return *static_cast<TestBackend*>(base); }

  static void enqueue(asyncBase *base, asyncOpRoot *op) { from(base).completions.push_back(op); }

  static void taskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t signals) {
    TestBackend &backend = from(object->header.base);
    backend.handledSignals.push_back(signals);
    uint32_t progress = signals & COMBINER_TAG_PROGRESS_MASK;
    uint32_t needStart = progress;

    if (op) {
      backend.started.push_back(op);
      startOperation(op, &needStart);
    }
    if (progress && __uintptr_atomic_load(&object->initializationOp, amoRelaxed))
      processInitializationOp(object, &needStart);
    if (signals & (COMBINER_TAG_CANCEL | COMBINER_TAG_CANCELIO))
      reapObject(object, signals, &needStart);

    if (needStart & IO_EVENT_READ)
      executeOperationList(&object->readQueue);
    if (needStart & IO_EVENT_WRITE)
      executeOperationList(&object->writeQueue);
  }

  static void initializeTimer(asyncBase *base, asyncOpRoot *op) {
    TestBackend &backend = from(base);
    backend.initializeTimerCalls++;
    op->timerId = reinterpret_cast<void*>(backend.nextTimerTag++);
  }

  static asyncBase *timerBase(asyncOpRoot *op) { return op->object->header.base; }

  static void startTimer(asyncOpRoot *op) { from(timerBase(op)).startTimerCalls++; }

  static void stopTimer(asyncOpRoot *op) { from(timerBase(op)).stopTimerCalls++; }

  static int initializeUserEvent(aioUserEvent*) { return 1; }

  static int activateEvent(aioUserEvent *event) {
    TestBackend &backend = from(event->header.base);
    backend.activateCalls++;
    // A rejected kernel post claims nothing: no envelope, no reference.
    if (backend.activateFailures) {
      backend.activateFailures--;
      return 0;
    }
    // Model a kernel readiness envelope that successfully claimed the live
    // generation. Its reference spans the queued record and delivery.
    eventIncrementReference(event, 1);
    enqueue(event->header.base, (asyncOpRoot*)((uintptr_t)event | eventQueueTagMask));
    return 1;
  }

  static int updateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period) {
    TestBackend &backend = from(event->header.base);
    if (update == etuStart && !eventTimerLoad(event, amoRelaxed)) {
      aioTimer *timer = static_cast<aioTimer*>(alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT));
      if (!timer)
        return 0;
      timerInitialize(timer);
      timer->header.base = event->header.base;
      timer->header.timer.kind = tkUserEvent;
      timer->fd = -1;
      timer->event.userEvent = event;
      eventTimerStore(event, timer, amoRelaxed);
      backend.eventTimers.push_back(timer);
    }
    backend.lastEventTimerGeneration = generation;
    backend.lastEventTimerPeriod = period;
    if (update == etuStart)
      backend.startTimerCalls++;
    else if (update == etuStop)
      backend.stopTimerCalls++;
    if (backend.eventTimerHook)
      return backend.eventTimerHook(event, update, generation, period);
    return 1;
  }

  static uint64_t consumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period) {
    TestBackend &backend = from(event->header.base);
    backend.consumeTimerCalls++;
    if (backend.eventTimerConsumeHook)
      return backend.eventTimerConsumeHook(event, published, generation, period);
    return published;
  }

  static void releaseUserEvent(aioUserEvent *event) { objectFree(&event->header.base->eventPool, event, sizeof(aioUserEvent)); }

  static void wakeupLoop(asyncBase *base) { from(base).wakeupCalls++; }
};

inline TestObject::TestObject(TestBackend &owner) :
  backend(&owner)
{
  initObjectRoot(&root, &owner.base, ioObjectUserDefined, resourceDestructor);
  objectSetDestructorCb(&root, destructorCallback, this);
}

inline void TestObject::resourceDestructor(aioObjectRoot *root)
{
  reinterpret_cast<TestObject*>(root)->resourceDestructors++;
}

inline void TestObject::destructorCallback(aioObjectRoot*, void *arg)
{
  TestObject *object = static_cast<TestObject*>(arg);
  object->destructorGeneration = __uint64_atomic_load(&object->root.header.tag.high, amoRelaxed);
  object->destructorCallbacks++;
}

inline void TestObject::memoryRelease(aioObjectRoot *root)
{
  reinterpret_cast<TestObject*>(root)->memoryReleases++;
}

inline TestOp::TestOp(TestObject &object, int opCode, AsyncFlags flags, uint64_t timeout, bool hasCallback) :
  owner(&object)
{
  initAsyncOpRoot(&root, execute, cancel, finish, release, &object.root, hasCallback ? this : nullptr, this, flags, opCode, timeout);
  root.objectPool = &object.backend->operationPool;
}

inline AsyncOpStatus TestOp::execute(asyncOpRoot *root)
{
  TestOp &op = *static_cast<TestOp*>(root->arg);
  op.executeCalls++;
  if (op.executeHook)
    op.executeHook(op);
  if (op.results.empty())
    return aosPending;
  size_t index = op.resultIndex < op.results.size() ? op.resultIndex++ : op.results.size() - 1;
  return op.results[index];
}

inline int TestOp::cancel(asyncOpRoot *root)
{
  TestOp &op = *static_cast<TestOp*>(root->arg);
  op.cancelCalls++;
  if (op.cancelHook)
    op.cancelHook(op);
  return op.cancelResult;
}

inline void TestOp::finish(asyncOpRoot *root)
{
  TestOp &op = *static_cast<TestOp*>(root->arg);
  op.finishCalls++;
  op.callbackStatus = opGetStatus(root);
  if (op.finishHook)
    op.finishHook(op);
}

inline void TestOp::release(asyncOpRoot *root)
{
  static_cast<TestOp*>(root->arg)->releaseCalls++;
}

inline std::vector<asyncOpRoot*> listContents(const List &list)
{
  std::vector<asyncOpRoot*> result;
  for (asyncOpRoot *op = list.head; op; op = op->executeQueue.next)
    result.push_back(op);
  return result;
}

inline void cancelAndDrain(TestBackend &backend, TestObject &object)
{
  cancelIo(&object.root);
  backend.drainCompletions();
}

inline void deleteOwner(TestBackend &backend, TestObject &object)
{
  objectDelete(&object.root);
  backend.drainCompletions();
}
