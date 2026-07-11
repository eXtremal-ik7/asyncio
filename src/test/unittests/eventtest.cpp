// White-box tests of the aioUserEvent protocol: lifetime/activation tag
// arithmetic, global-queue delivery and the newUserEvent pool cycle, all on
// the TestBackend mock. The `event` family are regression markers: each pins
// one known defect and is expected to stay RED until the matching fix lands;
// green companions pin behavior the fixes must not break.

#include "coretest.h"

#include "asyncio/asyncio.h"

#include <atomic>
#include <thread>
#include <vector>

namespace {

struct EventContext {
  unsigned finishes = 0;
  unsigned destructors = 0;
};

void eventFinish(asyncOpRoot *root)
{
  static_cast<EventContext*>(root->arg)->finishes++;
}

void eventDestructor(aioUserEvent*, void *arg)
{
  static_cast<EventContext*>(arg)->destructors++;
}

TEST(core_user_event, nonsemaphore_activation_coalesces_until_deactivated)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.root.arg = &context;
  event.root.finishMethod = eventFinish;
  event.root.opCode = OPCODE_OTHER;
  event.tag = 1;
  event.isSemaphore = 0;
  eventSetDestructorCb(&event, eventDestructor, &context);

  EXPECT_TRUE(eventTryActivate(&event));
  EXPECT_FALSE(eventTryActivate(&event));
  eventDeactivate(&event);
  EXPECT_TRUE(eventTryActivate(&event));
  eventDeactivate(&event);

  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 2u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 1u);
  EXPECT_EQ(context.destructors, 1u);

  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, &event);
}

TEST(core_user_event, semaphore_counts_each_activation)
{
  aioUserEvent event{};
  event.tag = 1;
  event.isSemaphore = 1;

  EXPECT_TRUE(eventTryActivate(&event));
  EXPECT_TRUE(eventTryActivate(&event));
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
  eventDeactivate(&event);
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
}

TEST(core_user_event, reference_increment_and_final_release_without_destructor_callback)
{
  TestBackend backend;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.tag = 1;

  EXPECT_EQ(eventIncrementReference(&event, 2), 1u);
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(eventDecrementReference(&event, 2) & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 1u);

  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, &event);
}

TEST(core_global_queue, user_event_branch_deactivates_and_returns_when_empty)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.opCode = actUserEvent;
  event.root.finishMethod = eventFinish;
  event.root.arg = &context;
  event.tag = TAG_EVENT_OP + 1;
  event.isSemaphore = 0;
  concurrentQueuePush(&backend.base.globalQueue, &event.root);

  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(event.tag, 1u);
}

// ---- regression markers for known defects -----------------------------------

// Supersedes the artificial-tag setup of the test above once fixed: a
// manual activation drained through the global queue must release its
// delivery reference, otherwise the owner's delete never reaches the
// destructor and the storage never recycles.
TEST(event, manual_delivery_releases_reference_and_delete_recycles)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.root.opCode = actUserEvent;
  event.root.finishMethod = eventFinish;
  event.root.arg = &context;
  event.tag = 1;
  event.isSemaphore = 0;
  eventSetDestructorCb(&event, eventDestructor, &context);

  ASSERT_TRUE(eventTryActivate(&event));  // real protocol: tag = TAG_EVENT_OP + 2
  concurrentQueuePush(&backend.base.globalQueue, &event.root);
  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(event.tag, 1u) << "delivery reference was not released after the callback";

  // deleteUserEvent's terminal transition, minus the backend timer stop
  eventDecrementReference(&event, 1 - TAG_EVENT_DELETE);
  EXPECT_EQ(context.destructors, 1u) << "leaked delivery reference blocks the destructor forever";
  void *recycled = nullptr;
  EXPECT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled))
    << "event storage never returns to the pool";
  if (recycled)
    EXPECT_EQ(recycled, static_cast<void*>(&event));
}

// Activation must be rejected once deletion has begun. The semaphore branch
// ignores the DELETE bit, so a stale tick resurrects the reference count of
// an already destroyed event and drives a second destructor plus a second
// push of the same address.
TEST(event, semaphore_activation_after_final_delete_is_rejected)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.root.opCode = actUserEvent;
  event.root.finishMethod = eventFinish;
  event.root.arg = &context;
  event.tag = 1;
  event.isSemaphore = 1;
  eventSetDestructorCb(&event, eventDestructor, &context);

  // Sole owner deletes: the final release runs the destructor and recycles
  eventDecrementReference(&event, 1 - TAG_EVENT_DELETE);
  ASSERT_EQ(context.destructors, 1u);
  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));

  uintptr_t terminal = event.tag;
  int activated = eventTryActivate(&event);
  EXPECT_FALSE(activated) << "semaphore branch accepts activation after delete";
  EXPECT_EQ(event.tag, terminal);

  if (activated) {
    // Complete the delivery protocol the loop would run: the resurrection
    // reaches a second destructor and recycles the same address twice
    eventDeactivate(&event);
    event.root.finishMethod(&event.root);
    eventDecrementReference(&event, 1);
    EXPECT_EQ(context.destructors, 1u) << "second destructor after resurrection";
    void *again = nullptr;
    EXPECT_FALSE(concurrentQueuePop(&backend.operationPool, &again))
      << "same address recycled twice";
  }
}

// Green companion: the non-semaphore branch already rejects activation on a
// deleting event and rolls the tag back intact - the semaphore fix above
// must keep this.
TEST(event, nonsemaphore_activation_after_delete_leaves_tag_intact)
{
  aioUserEvent event{};
  event.isSemaphore = 0;
  event.tag = TAG_EVENT_DELETE;       // terminal: final release already ran
  EXPECT_FALSE(eventTryActivate(&event));
  EXPECT_EQ(event.tag, TAG_EVENT_DELETE);

  event.tag = TAG_EVENT_DELETE + 1;   // delete with one delivery still in flight
  EXPECT_FALSE(eventTryActivate(&event));
  EXPECT_EQ(event.tag, TAG_EVENT_DELETE + 1);
}

// A failed non-semaphore activation is two separate RMWs (+OP+1, then the
// rollback), and TAG_EVENT_OP sits inside TAG_EVENT_MASK - a final release
// landing between them does not recognize the final count, and the rollback
// never re-checks it. Every iteration rebuilds "delete done, one
// delivery in flight" (tag = DELETE+1) and races that delivery's release
// against a rejected stale tick. Probabilistic per iteration, strict by
// invariant: every terminal release must produce exactly one destructor and
// exactly one recycle.
TEST(event, final_release_survives_concurrent_failed_activation)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.isSemaphore = 0;
  eventSetDestructorCb(&event, eventDestructor, &context);

  constexpr int kIterations = 100000;
  std::atomic<int> go{-1};
  std::atomic<unsigned> done{0};
  std::atomic<bool> stop{false};
  std::atomic<bool> activationAccepted{false};

  auto worker = [&](bool releaser) {
    for (int i = 0; i < kIterations; i++) {
      while (go.load(std::memory_order_acquire) < i) {
        if (stop.load(std::memory_order_relaxed))
          return;
      }
      if (stop.load(std::memory_order_acquire))
        return;
      if (releaser) {
        // Phase sweep: walk the release across the activation burst
        for (volatile int spin = 0; spin < (i & 255); spin++)
          ;
        eventDecrementReference(&event, 1);
      } else {
        // A burst of rejected activations keeps the tag inside the two-RMW
        // transient a large fraction of the burst, so the single release
        // lands in the window within a few iterations
        for (int burst = 0; burst < 32; burst++) {
          if (eventTryActivate(&event))
            activationAccepted.store(true, std::memory_order_relaxed);
        }
      }
      done.fetch_add(1, std::memory_order_acq_rel);
    }
  };
  std::thread releaseThread(worker, true);
  std::thread activateThread(worker, false);

  int failedIteration = -1;
  for (int i = 0; i < kIterations; i++) {
    __uintptr_atomic_store(&event.tag, TAG_EVENT_DELETE + 1, amoRelaxed);
    go.store(i, std::memory_order_release);
    while (done.load(std::memory_order_acquire) < 2u * (static_cast<unsigned>(i) + 1))
      ;
    void *recycled = nullptr;
    if (!concurrentQueuePop(&backend.operationPool, &recycled) ||
        context.destructors != static_cast<unsigned>(i) + 1) {
      failedIteration = i;
      break;
    }
  }

  stop.store(true, std::memory_order_release);
  go.store(kIterations, std::memory_order_release);
  releaseThread.join();
  activateThread.join();

  EXPECT_FALSE(activationAccepted.load(std::memory_order_relaxed))
    << "activation accepted on a deleting event";
  EXPECT_EQ(failedIteration, -1)
    << "final release was masked by the failed-activation transient: no destructor, storage lost";
}

// newUserEvent must reinitialize every root field the event paths can ever
// read; today a recycled slot keeps whatever the previous incarnation (or
// fresh malloc garbage) left there - a stale root.object in particular sends
// paths that dereference it into a foreign (or freed) object.
TEST(event, new_user_event_reinitializes_root_fields)
{
  TestBackend backend;
  aioUserEvent *event = newUserEvent(&backend.base, 0, nullptr, nullptr);
  ASSERT_NE(event, nullptr);
  deleteUserEvent(event);  // the slot parks in the process-global event pool

  // Pool storage is type-stable and events are not poisoned: sabotage the
  // fields a new incarnation is obliged to rewrite
  event->root.object = reinterpret_cast<aioObjectRoot*>(uintptr_t{0xDEADu});
  event->root.flags = static_cast<AsyncFlags>(~0u);
  event->root.running = arCancelling;

  // The global pool may hold slots parked by other tests: cycle until ours
  // returns (FIFO order bounds the hunt by the pool population)
  std::vector<aioUserEvent*> extras;
  aioUserEvent *second = nullptr;
  for (int i = 0; i < 4096 && !second; i++) {
    aioUserEvent *candidate = newUserEvent(&backend.base, 0, nullptr, nullptr);
    ASSERT_NE(candidate, nullptr);
    if (candidate == event)
      second = candidate;
    else
      extras.push_back(candidate);
  }
  ASSERT_NE(second, nullptr) << "sabotaged slot never came back from the pool";

  EXPECT_EQ(second->root.object, nullptr) << "stale object pointer survives reuse";
  EXPECT_EQ(second->root.flags, afNone) << "stale flags survive reuse";
  EXPECT_EQ(second->root.running, arWaiting) << "stale running state survives reuse";

  deleteUserEvent(second);
  for (aioUserEvent *extra : extras)
    deleteUserEvent(extra);
}

// The actUserEvent branch must reset the afActiveOnce inline budget at the
// callback boundary the way the default branch does; today the counter
// leaks through and pushes the first sync operation inside an event callback
// off the inline fast path.
TEST(event, user_event_delivery_resets_sync_budget)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.opCode = actUserEvent;
  event.root.finishMethod = eventFinish;
  event.root.arg = &context;
  event.tag = 1;
  event.isSemaphore = 0;

  ASSERT_TRUE(eventTryActivate(&event));
  concurrentQueuePush(&backend.base.globalQueue, &event.root);
  currentFinishedSync = 17;
  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(currentFinishedSync, 0u) << "sync budget leaks through the event callback boundary";
  currentFinishedSync = 0;  // keep the sabotage out of the following tests
}

} // namespace
