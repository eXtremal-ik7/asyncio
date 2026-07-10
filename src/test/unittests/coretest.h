#pragma once

#include "asyncioImpl.h"

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <utility>
#include <vector>

extern "C" {
asyncOpListLink *pageMapExtractAll(pageMap *map, uint64_t tm);
void pageMapAdd(pageMap *map, asyncOpListLink *link);
}

constexpr size_t kCombinerAlignment = size_t{1} << COMBINER_TAG_SIZE;
constexpr size_t kPageMapSize = size_t{1} << 16;

inline void destroyConcurrentQueue(ConcurrentQueue *queue)
{
  for (ConcurrentQueuePartition &partition : queue->Partitions) {
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

  explicit TestOp(TestObject &object,
                  int opCode = OPCODE_READ,
                  AsyncFlags flags = afNone,
                  uint64_t timeout = 0,
                  bool hasCallback = true);

  void setResults(std::initializer_list<AsyncOpStatus> values)
  {
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
static_assert(offsetof(TestOp, root) == 0, "asyncOpRoot must be the first member");

struct TestBackend : asyncBase {
  asyncBase &base;
  ConcurrentQueue operationPool{};
  std::vector<asyncOpRoot*> completions;
  std::vector<asyncOpRoot*> recycled;
  std::vector<asyncOpRoot*> started;
  std::vector<uint32_t> handledSignals;
  unsigned startTimerCalls = 0;
  unsigned stopTimerCalls = 0;
  unsigned initializeTimerCalls = 0;
  unsigned deleteTimerCalls = 0;
  uintptr_t nextTimerTag = 1;

  TestBackend() : asyncBase{}, base(*this)
  {
    base.methodImpl.combinerTaskHandler = taskHandler;
    base.methodImpl.enqueue = enqueue;
    base.methodImpl.initializeTimer = initializeTimer;
    base.methodImpl.startTimer = startTimer;
    base.methodImpl.stopTimer = stopTimer;
    base.methodImpl.deleteTimer = deleteTimer;
    for (GraceSlot &slot : base.graceSeen)
      slot.seen = UINTPTR_MAX;
    currentFinishedSync = 0;
    messageLoopThreadId = 0;
  }

  ~TestBackend()
  {
    if (!completions.empty())
      ADD_FAILURE() << completions.size() << " completion(s) were not drained by the test";
    destroyConcurrentQueue(&operationPool);
    destroyConcurrentQueue(&base.globalQueue);
    destroyPageMap();
  }

  void initializePageMap()
  {
    if (!base.timerMap.map)
      pageMapInit(&base.timerMap);
  }

  void destroyPageMap()
  {
    if (!base.timerMap.map)
      return;
    for (size_t i = 0; i < kPageMapSize; ++i)
      free(base.timerMap.map[i]);
    free(base.timerMap.map);
    base.timerMap.map = nullptr;
  }

  void drainCompletions()
  {
    size_t index = 0;
    while (index < completions.size()) {
      asyncOpRoot *op = completions[index++];
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

  static TestBackend &from(asyncBase *base)
  {
    return *static_cast<TestBackend*>(base);
  }

  static void enqueue(asyncBase *base, asyncOpRoot *op)
  {
    from(base).completions.push_back(op);
  }

  static void taskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t signals)
  {
    TestBackend &backend = from(object->base);
    backend.handledSignals.push_back(signals);
    uint32_t progress = signals & COMBINER_TAG_PROGRESS_MASK;
    uint32_t needStart = progress;

    if (op) {
      backend.started.push_back(op);
      startOperation(op, &needStart);
    }
    if (progress && __uintptr_atomic_load(&object->initializationOp, amoRelaxed))
      processInitializationOp(object, &needStart);
    if (signals & COMBINER_TAG_CANCEL)
      reapObject(object, &needStart);

    if (needStart & IO_EVENT_READ)
      executeOperationList(&object->readQueue);
    if (needStart & IO_EVENT_WRITE)
      executeOperationList(&object->writeQueue);
  }

  static void initializeTimer(asyncBase *base, asyncOpRoot *op)
  {
    TestBackend &backend = from(base);
    backend.initializeTimerCalls++;
    op->timerId = reinterpret_cast<void*>(backend.nextTimerTag++);
  }

  static void startTimer(asyncOpRoot *op)
  {
    from(op->object->base).startTimerCalls++;
  }

  static void stopTimer(asyncOpRoot *op)
  {
    from(op->object->base).stopTimerCalls++;
  }

  static void deleteTimer(asyncOpRoot *op)
  {
    from(op->object->base).deleteTimerCalls++;
  }
};

inline TestObject::TestObject(TestBackend &owner) : backend(&owner)
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
  static_cast<TestObject*>(arg)->destructorCallbacks++;
}

inline void TestObject::memoryRelease(aioObjectRoot *root)
{
  reinterpret_cast<TestObject*>(root)->memoryReleases++;
}

inline TestOp::TestOp(TestObject &object,
                      int opCode,
                      AsyncFlags flags,
                      uint64_t timeout,
                      bool hasCallback)
  : owner(&object)
{
  initAsyncOpRoot(&root,
                  execute,
                  cancel,
                  finish,
                  release,
                  &object.root,
                  hasCallback ? this : nullptr,
                  this,
                  flags,
                  opCode,
                  timeout);
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
