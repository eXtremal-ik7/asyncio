// aioUserEvent contract tests. The core_* suites exercise the protocol's
// decision machinery on TestBackend; user_event* suites exercise the public
// API on the real backend. The `event` family are regression markers: each
// pins one known defect and is expected to stay RED until the matching fix
// lands; green companions pin behavior the fixes must not break.

#include "coretest.h"
#include "unittest.h"

#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
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

using namespace std::chrono_literals;

struct PublicEventProbe {
  std::mutex mutex;
  std::condition_variable changed;
  unsigned callbacks = 0;
  unsigned destructors = 0;
  std::thread::id callbackThread;
};

void publicEventCb(aioUserEvent*, void *arg)
{
  PublicEventProbe *probe = static_cast<PublicEventProbe*>(arg);
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->callbacks;
    probe->callbackThread = std::this_thread::get_id();
  }
  probe->changed.notify_all();
}

void publicEventDestructor(aioUserEvent*, void *arg)
{
  PublicEventProbe *probe = static_cast<PublicEventProbe*>(arg);
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->destructors;
  }
  probe->changed.notify_all();
}

unsigned publicEventCallbackCount(PublicEventProbe &probe)
{
  std::lock_guard<std::mutex> lock(probe.mutex);
  return probe.callbacks;
}

unsigned publicEventDestructorCount(PublicEventProbe &probe)
{
  std::lock_guard<std::mutex> lock(probe.mutex);
  return probe.destructors;
}

bool waitForEventCallbacks(PublicEventProbe &probe,
                           unsigned expected,
                           std::chrono::milliseconds timeout = 1500ms)
{
  std::unique_lock<std::mutex> lock(probe.mutex);
  return probe.changed.wait_for(lock, timeout, [&]() {
    return probe.callbacks >= expected;
  });
}

class EventLoopThread {
public:
  explicit EventLoopThread(asyncBase *baseArg) : base(baseArg)
  {
    loop = std::thread([this]() {
      loopThreadId = std::this_thread::get_id();
      started.store(true, std::memory_order_release);
      asyncLoop(base);
    });
    while (!started.load(std::memory_order_acquire))
      std::this_thread::yield();
  }

  ~EventLoopThread()
  {
    stop();
  }

  EventLoopThread(const EventLoopThread&) = delete;
  EventLoopThread &operator=(const EventLoopThread&) = delete;

  void stop()
  {
    if (loop.joinable()) {
      postQuitOperation(base);
      loop.join();
    }
  }

  std::thread::id id() const
  {
    return loopThreadId;
  }

private:
  asyncBase *base;
  std::thread loop;
  std::atomic<bool> started{false};
  std::thread::id loopThreadId;
};

void drainQueuedUserEvents(asyncBase *base)
{
  // The quit marker is appended after every activation already submitted.
  // asyncLoop therefore drains the finite prefix and returns deterministically.
  postQuitOperation(base);
  asyncLoop(base);
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

TEST(core_user_event, strong_references_delay_final_release_after_delete)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.tag = 1;
  eventSetDestructorCb(&event, eventDestructor, &context);

  EXPECT_EQ(eventIncrementReference(&event, 2), 1u);
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
  // deleteUserEvent's terminal transition, minus the backend timer stop:
  // publish DELETE and consume exactly one strong reference.
  EXPECT_EQ(eventDecrementReference(&event, 1 - TAG_EVENT_DELETE) & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(context.destructors, 0u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 2u);
  EXPECT_EQ(context.destructors, 0u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 1u);
  EXPECT_EQ(context.destructors, 1u);

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

// ---- public activation contract -------------------------------------------

TEST(user_event, nonsemaphore_activations_coalesce_while_delivery_is_pending)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  for (unsigned i = 0; i < 128; ++i)
    userEventActivate(event);

  drainQueuedUserEvents(gBase);
  EXPECT_EQ(publicEventCallbackCount(probe), 1u);
  deleteUserEvent(event);
}

TEST(user_event, semaphore_delivers_every_activation)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 1, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  constexpr unsigned kActivations = 128;
  for (unsigned i = 0; i < kActivations; ++i)
    userEventActivate(event);

  drainQueuedUserEvents(gBase);
  EXPECT_EQ(publicEventCallbackCount(probe), kActivations);
  deleteUserEvent(event);
}

TEST(user_event, concurrent_nonsemaphore_activations_coalesce_before_drain)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  constexpr unsigned kThreads = 4;
  // static: lambdas below read it without a capture (MSVC refuses to capture
  // a constexpr local implicitly, C3493)
  static constexpr unsigned kActivationsPerThread = 128;
  std::thread producers[kThreads];
  for (std::thread &producer : producers) {
    eventIncrementReference(event, 1);
    producer = std::thread([event]() {
      for (unsigned i = 0; i < kActivationsPerThread; ++i)
        userEventActivate(event);
      eventDecrementReference(event, 1);
    });
  }
  for (std::thread &producer : producers)
    producer.join();

  drainQueuedUserEvents(gBase);
  EXPECT_EQ(publicEventCallbackCount(probe), 1u);
  deleteUserEvent(event);
}

TEST(user_event, concurrent_semaphore_activations_are_not_lost)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 1, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  constexpr unsigned kThreads = 4;
  // static: lambdas below read it without a capture (MSVC refuses to capture
  // a constexpr local implicitly, C3493)
  static constexpr unsigned kActivationsPerThread = 128;
  std::thread producers[kThreads];
  for (std::thread &producer : producers) {
    eventIncrementReference(event, 1);
    producer = std::thread([event]() {
      for (unsigned i = 0; i < kActivationsPerThread; ++i)
        userEventActivate(event);
      eventDecrementReference(event, 1);
    });
  }
  for (std::thread &producer : producers)
    producer.join();

  drainQueuedUserEvents(gBase);
  EXPECT_EQ(publicEventCallbackCount(probe), kThreads * kActivationsPerThread);
  deleteUserEvent(event);
}

struct SelfReactivationProbe : PublicEventProbe {
  unsigned target = 0;
};

void selfReactivatingEventCb(aioUserEvent *event, void *arg)
{
  SelfReactivationProbe *probe = static_cast<SelfReactivationProbe*>(arg);
  unsigned call;
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    call = ++probe->callbacks;
    probe->callbackThread = std::this_thread::get_id();
  }
  probe->changed.notify_all();

  // The non-semaphore pending gate must already be open at callback entry.
  if (call < probe->target)
    userEventActivate(event);
}

TEST(user_event, activation_from_callback_schedules_the_next_delivery)
{
  SelfReactivationProbe probe;
  probe.target = 4;
  aioUserEvent *event = newUserEvent(gBase, 0, selfReactivatingEventCb, &probe);
  ASSERT_NE(event, nullptr);

  EventLoopThread loop(gBase);
  userEventActivate(event);
  bool completed = waitForEventCallbacks(probe, probe.target);
  loop.stop();

  EXPECT_TRUE(completed);
  EXPECT_EQ(publicEventCallbackCount(probe), probe.target);
  deleteUserEvent(event);
}

TEST(user_event, cross_thread_activation_runs_callback_on_the_loop_thread)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  EventLoopThread loop(gBase);
  eventIncrementReference(event, 1);
  std::thread producer([event]() {
    userEventActivate(event);
    eventDecrementReference(event, 1);
  });
  producer.join();
  bool delivered = waitForEventCallbacks(probe, 1);
  loop.stop();

  EXPECT_TRUE(delivered);
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.callbackThread, loop.id());
  }
  deleteUserEvent(event);
}

// ---- public lifetime contract ---------------------------------------------

struct EventLifetimeProbe {
  std::mutex mutex;
  std::condition_variable changed;
  unsigned sequence = 0;
  unsigned callbacks = 0;
  unsigned completedCallbacks = 0;
  unsigned destructors = 0;
  unsigned lastCallbackSequence = 0;
  unsigned destructorSequence = 0;
  bool destructorSawAllCallbacksComplete = false;
  bool callbackEntered = false;
  bool allowCallbackReturn = false;
};

void lifetimeContractEventCb(aioUserEvent*, void *arg)
{
  EventLifetimeProbe *probe = static_cast<EventLifetimeProbe*>(arg);
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->callbacks;
    probe->lastCallbackSequence = ++probe->sequence;
    ++probe->completedCallbacks;
  }
  probe->changed.notify_all();
}

void lifetimeContractDestructor(aioUserEvent*, void *arg)
{
  EventLifetimeProbe *probe = static_cast<EventLifetimeProbe*>(arg);
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->destructors;
    probe->destructorSequence = ++probe->sequence;
    probe->destructorSawAllCallbacksComplete =
      probe->completedCallbacks == probe->callbacks;
  }
  probe->changed.notify_all();
}

void deleteInsideEventCb(aioUserEvent *event, void *arg)
{
  EventLifetimeProbe *probe = static_cast<EventLifetimeProbe*>(arg);
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->callbacks;
    probe->lastCallbackSequence = ++probe->sequence;
  }

  deleteUserEvent(event);

  // This is application work after deleteUserEvent returns. The accepted
  // delivery reference must keep both event storage and application state
  // alive until this callback actually finishes.
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->completedCallbacks;
  }
  probe->changed.notify_all();
}

void blockingLifetimeEventCb(aioUserEvent*, void *arg)
{
  EventLifetimeProbe *probe = static_cast<EventLifetimeProbe*>(arg);
  std::unique_lock<std::mutex> lock(probe->mutex);
  ++probe->callbacks;
  probe->lastCallbackSequence = ++probe->sequence;
  probe->callbackEntered = true;
  probe->changed.notify_all();
  probe->changed.wait(lock, [&]() { return probe->allowCallbackReturn; });
  ++probe->completedCallbacks;
}

TEST(user_event_lifetime, delete_without_deliveries_calls_destructor_once)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, lifetimeContractEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  deleteUserEvent(event);

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 0u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, 1u);
}

TEST(user_event_lifetime, strong_reference_keeps_terminal_event_alive_until_release)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, lifetimeContractEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  // This reference represents an independently using producer. The owner may
  // close the event first; the producer still owns valid storage, observes the
  // terminal state and releases its reference when finished.
  eventIncrementReference(event, 1);
  deleteUserEvent(event); // closes the event and consumes one strong reference
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.destructors, 0u);
  }

  // Like copying a shared_ptr, an existing holder may copy its lifetime
  // reference after close. This must neither reopen the event nor be confused
  // with the exactly-once close transition already performed by delete.
  EXPECT_EQ(eventIncrementReference(event, 1) & TAG_EVENT_MASK, 1u);
  userEventActivate(event); // safe no-op through the retained reference
  drainQueuedUserEvents(gBase);
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.callbacks, 0u);
    EXPECT_EQ(probe.destructors, 0u);
  }

  eventDecrementReference(event, 1);
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.destructors, 0u);
  }
  eventDecrementReference(event, 1);
  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 0u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, 1u);
}

TEST(user_event_lifetime, producer_threads_release_their_references_after_delete)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, lifetimeContractEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  constexpr unsigned kProducers = 4;
  std::mutex barrierMutex;
  std::condition_variable barrierChanged;
  unsigned ready = 0;
  unsigned observedDelete = 0;
  bool mayActivate = false;
  bool mayRelease = false;
  std::thread producers[kProducers];

  for (std::thread &producer : producers) {
    // Retain before publishing the pointer to the new thread.
    eventIncrementReference(event, 1);
    producer = std::thread([&]() {
      {
        std::unique_lock<std::mutex> lock(barrierMutex);
        ++ready;
        barrierChanged.notify_all();
        barrierChanged.wait(lock, [&]() { return mayActivate; });
      }

      // The owner has closed the event, but this thread's strong reference
      // keeps the pointer valid and makes the operation a safe terminal no-op.
      userEventActivate(event);

      {
        std::unique_lock<std::mutex> lock(barrierMutex);
        ++observedDelete;
        barrierChanged.notify_all();
        barrierChanged.wait(lock, [&]() { return mayRelease; });
      }
      eventDecrementReference(event, 1);
    });
  }

  {
    std::unique_lock<std::mutex> lock(barrierMutex);
    barrierChanged.wait(lock, [&]() {
      return ready == kProducers;
    });
  }

  deleteUserEvent(event);
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.destructors, 0u);
  }

  {
    std::unique_lock<std::mutex> lock(barrierMutex);
    mayActivate = true;
    barrierChanged.notify_all();
    barrierChanged.wait(lock, [&]() {
      return observedDelete == kProducers;
    });
    mayRelease = true;
  }
  barrierChanged.notify_all();

  for (std::thread &producer : producers)
    producer.join();

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 0u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, 1u);
}

TEST(user_event_lifetime, accepted_delivery_survives_external_delete)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, lifetimeContractEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  userEventActivate(event);
  deleteUserEvent(event);
  drainQueuedUserEvents(gBase);

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 1u);
  EXPECT_EQ(probe.completedCallbacks, 1u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.lastCallbackSequence, 1u);
  EXPECT_EQ(probe.destructorSequence, 2u);
  EXPECT_TRUE(probe.destructorSawAllCallbacksComplete);
}

TEST(user_event_lifetime, accepted_semaphore_deliveries_survive_external_delete)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 1, lifetimeContractEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  constexpr unsigned kDeliveries = 8;
  for (unsigned i = 0; i < kDeliveries; ++i)
    userEventActivate(event);
  deleteUserEvent(event);
  drainQueuedUserEvents(gBase);

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, kDeliveries);
  EXPECT_EQ(probe.completedCallbacks, kDeliveries);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, kDeliveries + 1);
  EXPECT_TRUE(probe.destructorSawAllCallbacksComplete);
}

TEST(user_event_lifetime, delete_from_callback_defers_destructor_until_callback_finishes)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, deleteInsideEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  userEventActivate(event);
  drainQueuedUserEvents(gBase);

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 1u);
  EXPECT_EQ(probe.completedCallbacks, 1u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, 2u);
  EXPECT_TRUE(probe.destructorSawAllCallbacksComplete);
}

TEST(user_event_lifetime, external_delete_waits_for_running_callback)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, blockingLifetimeEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  EventLoopThread loop(gBase);
  userEventActivate(event);
  bool entered;
  {
    std::unique_lock<std::mutex> lock(probe.mutex);
    entered = probe.changed.wait_for(lock, 1500ms, [&]() {
      return probe.callbackEntered;
    });
  }
  EXPECT_TRUE(entered);

  if (!entered) {
    {
      std::lock_guard<std::mutex> lock(probe.mutex);
      probe.allowCallbackReturn = true;
    }
    probe.changed.notify_all();
    deleteUserEvent(event);
    loop.stop();
    return;
  }

  deleteUserEvent(event);
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.destructors, 0u)
      << "destructor ran while its accepted callback was still executing";
    probe.allowCallbackReturn = true;
  }
  probe.changed.notify_all();
  loop.stop();

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 1u);
  EXPECT_EQ(probe.completedCallbacks, 1u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, 2u);
  EXPECT_TRUE(probe.destructorSawAllCallbacksComplete);
}

enum class LateCallbackAction {
  activate,
  startTimer
};

struct LateCallbackProbe {
  std::mutex mutex;
  std::condition_variable changed;
  LateCallbackAction action = LateCallbackAction::activate;
  unsigned callbacks = 0;
  unsigned completedCallbacks = 0;
  unsigned destructors = 0;
  bool callbackEntered = false;
  bool allowCallbackReturn = false;
  bool destructorSawAllCallbacksComplete = false;
};

void lateActionEventCb(aioUserEvent *event, void *arg)
{
  LateCallbackProbe *probe = static_cast<LateCallbackProbe*>(arg);
  LateCallbackAction action = LateCallbackAction::activate;
  bool firstCall;
  {
    std::unique_lock<std::mutex> lock(probe->mutex);
    firstCall = ++probe->callbacks == 1;
    if (firstCall) {
      probe->callbackEntered = true;
      probe->changed.notify_all();
      probe->changed.wait(lock, [&]() { return probe->allowCallbackReturn; });
      action = probe->action;
    }
  }

  if (firstCall) {
    // deleteUserEvent has already returned in the external thread. A callback
    // accepted before that transition may finish, but must not resurrect the
    // event or publish a fresh timer schedule.
    if (action == LateCallbackAction::activate)
      userEventActivate(event);
    else
      userEventStartTimer(event, 5000, 1);
  }

  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->completedCallbacks;
  }
  probe->changed.notify_all();
}

void lateActionEventDestructor(aioUserEvent*, void *arg)
{
  LateCallbackProbe *probe = static_cast<LateCallbackProbe*>(arg);
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    ++probe->destructors;
    probe->destructorSawAllCallbacksComplete =
      probe->completedCallbacks == probe->callbacks;
  }
  probe->changed.notify_all();
}

void verifyLateCallbackActionIsRejected(LateCallbackAction action)
{
  LateCallbackProbe probe;
  probe.action = action;
  aioUserEvent *event = newUserEvent(gBase, 1, lateActionEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lateActionEventDestructor, &probe);

  EventLoopThread loop(gBase);
  userEventActivate(event);
  bool entered;
  {
    std::unique_lock<std::mutex> lock(probe.mutex);
    entered = probe.changed.wait_for(lock, 1500ms, [&]() {
      return probe.callbackEntered;
    });
  }
  EXPECT_TRUE(entered);

  if (!entered) {
    {
      std::lock_guard<std::mutex> lock(probe.mutex);
      probe.allowCallbackReturn = true;
    }
    probe.changed.notify_all();
    deleteUserEvent(event);
    loop.stop();
    return;
  }

  deleteUserEvent(event);
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    probe.allowCallbackReturn = true;
  }
  probe.changed.notify_all();

  // A broken implementation produces a second callback immediately (manual
  // action) or on the 5 ms tick. A correct one reaches its destructor after
  // the first callback. Either outcome makes this wait deterministic.
  {
    std::unique_lock<std::mutex> lock(probe.mutex);
    probe.changed.wait_for(lock, 250ms, [&]() {
      return probe.callbacks > 1 || probe.destructors == 1;
    });
  }
  loop.stop();

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 1u);
  EXPECT_EQ(probe.completedCallbacks, 1u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_TRUE(probe.destructorSawAllCallbacksComplete);
}

TEST(user_event_lifetime, running_callback_cannot_reactivate_after_external_delete)
{
  verifyLateCallbackActionIsRejected(LateCallbackAction::activate);
}

TEST(user_event_lifetime, running_callback_cannot_restart_timer_after_external_delete)
{
  verifyLateCallbackActionIsRejected(LateCallbackAction::startTimer);
}

// ---- public timer contract -------------------------------------------------

void verifyFiniteTimerCounter(int isSemaphore)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, isSemaphore, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  EventLoopThread loop(gBase);
  constexpr unsigned kDeliveries = 3;
  userEventStartTimer(event, 5000, static_cast<int>(kDeliveries));
  bool delivered = waitForEventCallbacks(probe, kDeliveries);
  userEventStopTimer(event);
  loop.stop();

  EXPECT_TRUE(delivered);
  EXPECT_EQ(publicEventCallbackCount(probe), kDeliveries);
  deleteUserEvent(event);
}

TEST(user_event_timer, finite_counter_is_exact_for_nonsemaphore_event)
{
  verifyFiniteTimerCounter(0);
}

TEST(user_event_timer, finite_counter_is_exact_for_semaphore_event)
{
  verifyFiniteTimerCounter(1);
}

TEST(user_event_timer, zero_and_negative_counters_repeat_until_stopped)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 1, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  EventLoopThread loop(gBase);
  for (int counter : {0, -1}) {
    unsigned baseline = publicEventCallbackCount(probe);
    userEventStartTimer(event, 5000, counter);
    bool repeated = waitForEventCallbacks(probe, baseline + 3);
    userEventStopTimer(event);

    // Stop does not retract a tick already accepted by the loop. Let that
    // bounded tail drain, then require the count to become stable.
    std::this_thread::sleep_for(40ms);
    unsigned settled = publicEventCallbackCount(probe);
    std::this_thread::sleep_for(40ms);

    EXPECT_TRUE(repeated);
    EXPECT_GE(settled, baseline + 3);
    EXPECT_EQ(publicEventCallbackCount(probe), settled);
  }

  userEventStopTimer(event); // stop is idempotent
  loop.stop();
  deleteUserEvent(event);
}

TEST(user_event_timer, stop_before_first_expiration_suppresses_callback)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  EventLoopThread loop(gBase);
  userEventStartTimer(event, 150000, 1);
  userEventStopTimer(event);
  userEventStopTimer(event); // the public contract makes stop idempotent
  std::this_thread::sleep_for(250ms);
  loop.stop();

  EXPECT_EQ(publicEventCallbackCount(probe), 0u);
  deleteUserEvent(event);
}

TEST(user_event_timer, delete_disarms_timer_and_prevents_future_callback)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, publicEventDestructor, &probe);

  EventLoopThread loop(gBase);
  userEventStartTimer(event, 150000, 1);
  deleteUserEvent(event);
  std::this_thread::sleep_for(250ms);
  loop.stop();

  EXPECT_EQ(publicEventCallbackCount(probe), 0u);
  EXPECT_EQ(publicEventDestructorCount(probe), 1u);
}

TEST(user_event_timer, later_start_replaces_period_and_counter)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  EventLoopThread loop(gBase);
  // The first schedule would fire during the observation window if it were
  // still current. Only the two callbacks from the replacement are allowed.
  userEventStartTimer(event, 200000, 1);
  userEventStartTimer(event, 5000, 2);
  bool delivered = waitForEventCallbacks(probe, 2);
  std::this_thread::sleep_for(250ms);
  userEventStopTimer(event);
  loop.stop();

  EXPECT_TRUE(delivered);
  EXPECT_EQ(publicEventCallbackCount(probe), 2u);
  deleteUserEvent(event);
}

TEST(user_event_timer, stop_does_not_cancel_pending_manual_delivery)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  userEventActivate(event);
  userEventStopTimer(event);
  drainQueuedUserEvents(gBase);

  EXPECT_EQ(publicEventCallbackCount(probe), 1u);
  deleteUserEvent(event);
}

TEST(user_event_timer, manual_activation_does_not_consume_timer_counter)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  // Keep the manual delivery pending while the finite timer is armed. The
  // manual callback is not one of the timer's two counted deliveries.
  userEventActivate(event);
  userEventStartTimer(event, 5000, 2);
  EventLoopThread loop(gBase);
  bool delivered = waitForEventCallbacks(probe, 3);
  userEventStopTimer(event);
  loop.stop();

  EXPECT_TRUE(delivered);
  EXPECT_EQ(publicEventCallbackCount(probe), 3u);
  deleteUserEvent(event);
}

TEST(user_event_timer, manual_activation_may_overlap_serialized_timer_control)
{
  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 1, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  // Timer control has exactly one application-side writer. Activations remain
  // fully concurrent with that writer and must not be lost; the long period
  // prevents the schedules themselves from contributing callbacks.
  constexpr unsigned kControlIterations = 32;
  constexpr unsigned kProducerThreads = 4;
  constexpr unsigned kActivationsPerThread = 64;
  std::atomic<bool> go{false};
  eventIncrementReference(event, 1);
  std::thread controller([&]() {
    while (!go.load(std::memory_order_acquire))
      std::this_thread::yield();
    for (unsigned i = 0; i < kControlIterations; ++i) {
      userEventStartTimer(event, 60000000, 1);
      userEventStopTimer(event);
    }
    eventDecrementReference(event, 1);
  });

  std::thread producers[kProducerThreads];
  for (std::thread &producer : producers) {
    eventIncrementReference(event, 1);
    producer = std::thread([&]() {
      while (!go.load(std::memory_order_acquire))
        std::this_thread::yield();
      for (unsigned i = 0; i < kActivationsPerThread; ++i)
        userEventActivate(event);
      eventDecrementReference(event, 1);
    });
  }

  go.store(true, std::memory_order_release);
  controller.join();
  for (std::thread &producer : producers)
    producer.join();

  drainQueuedUserEvents(gBase);
  EXPECT_EQ(publicEventCallbackCount(probe),
            kProducerThreads * kActivationsPerThread);
  deleteUserEvent(event);
}

#ifdef OS_COMMONUNIX
TEST(user_event_timer, recycled_event_timer_belongs_to_its_new_base)
{
  asyncBase *otherBase = createAsyncBase(amOSDefault, 1);
  ASSERT_NE(otherBase, nullptr);

  // Put at least one timer-bearing slot into the old base's pool. With a
  // correct per-base pool the next allocation below is fresh; with the
  // process-global pool it reuses a timer still registered on gBase.
  aioUserEvent *oldEvent = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(oldEvent, nullptr);
  deleteUserEvent(oldEvent);

  PublicEventProbe probe;
  aioUserEvent *event = newUserEvent(otherBase, 0, publicEventCb, &probe);
  ASSERT_NE(event, nullptr);

  EventLoopThread loop(otherBase);
  userEventStartTimer(event, 5000, 1);
  bool delivered = waitForEventCallbacks(probe, 1, 500ms);
  userEventStopTimer(event);
  loop.stop();

  EXPECT_TRUE(delivered) << "recycled timer remained attached to its old asyncBase";
  EXPECT_EQ(publicEventCallbackCount(probe), 1u);
  if (delivered) {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.callbackThread, loop.id());
  }
  deleteUserEvent(event);
}
#endif

// ---- public coroutine-helper contract -------------------------------------

struct CoroutineEventProbe {
  aioUserEvent *event = nullptr;
  std::atomic<unsigned> phase{0};
  unsigned waits = 1;
};

bool waitForCoroutinePhase(CoroutineEventProbe &probe,
                           unsigned expected,
                           std::chrono::milliseconds timeout = 1500ms)
{
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (probe.phase.load(std::memory_order_acquire) < expected) {
    if (std::chrono::steady_clock::now() >= deadline)
      return false;
    std::this_thread::sleep_for(1ms);
  }
  return true;
}

void waitOnceCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  ioWaitUserEvent(probe->event);
  probe->phase.store(1, std::memory_order_release);
}

void waitManyCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  for (unsigned i = 0; i < probe->waits; ++i) {
    ioWaitUserEvent(probe->event);
    probe->phase.store(i + 1, std::memory_order_release);
  }
}

void sleepOnceCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  ioSleep(probe->event, 20000);
  probe->phase.store(1, std::memory_order_release);
}

void externallyWokenSleepCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  ioSleep(probe->event, 100000);
  probe->phase.store(1, std::memory_order_release);
  ioWaitUserEvent(probe->event);
  probe->phase.store(2, std::memory_order_release);
}

void longSleepCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  ioSleep(probe->event, 60000000);
  probe->phase.store(1, std::memory_order_release);
}

TEST(user_event_coroutine, activation_before_wait_is_consumed_as_credit)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  userEventActivate(probe.event);
  drainQueuedUserEvents(gBase);

  coroutineTy *coroutine = coroutineNew(waitOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  int finished = coroutineCall(coroutine);

  EXPECT_EQ(finished, 1);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  if (!finished)
    coroutineDelete(coroutine);
  deleteUserEvent(probe.event);
}

TEST(user_event_coroutine, activation_resumes_an_installed_waiter)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(waitOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  eventIncrementReference(probe.event, 1);
  std::thread producer([&]() {
    userEventActivate(probe.event);
    eventDecrementReference(probe.event, 1);
  });
  producer.join();
  drainQueuedUserEvents(gBase);

  bool finished = probe.phase.load(std::memory_order_acquire) == 1;
  EXPECT_TRUE(finished);
  if (!finished)
    coroutineDelete(coroutine);
  deleteUserEvent(probe.event);
}

TEST(user_event_coroutine, semaphore_activations_preserve_every_wait_credit)
{
  CoroutineEventProbe probe;
  probe.waits = 16;
  probe.event = newUserEvent(gBase, 1, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  for (unsigned i = 0; i < probe.waits; ++i)
    userEventActivate(probe.event);
  drainQueuedUserEvents(gBase);

  coroutineTy *coroutine = coroutineNew(waitManyCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  int finished = coroutineCall(coroutine);

  EXPECT_EQ(finished, 1);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), probe.waits);
  if (!finished)
    coroutineDelete(coroutine);
  deleteUserEvent(probe.event);
}

TEST(user_event_coroutine, sleep_returns_on_its_timer)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(sleepOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  EventLoopThread loop(gBase);
  bool woke = waitForCoroutinePhase(probe, 1);
  userEventStopTimer(probe.event);
  loop.stop();

  EXPECT_TRUE(woke);
  bool finished = probe.phase.load(std::memory_order_acquire) == 1;
  if (!finished)
    coroutineDelete(coroutine);
  deleteUserEvent(probe.event);
}

TEST(user_event_coroutine, pending_credit_makes_sleep_return_without_arming)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  userEventActivate(probe.event);
  drainQueuedUserEvents(gBase);

  coroutineTy *coroutine = coroutineNew(longSleepCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  int finished = coroutineCall(coroutine);

  EXPECT_EQ(finished, 1);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  if (!finished) {
    userEventStopTimer(probe.event);
    coroutineDelete(coroutine);
  }
  deleteUserEvent(probe.event);
}

TEST(user_event_coroutine, external_wake_cancels_sleep_without_crediting_next_wait)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(externallyWokenSleepCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  EventLoopThread loop(gBase);
  std::this_thread::sleep_for(10ms);
  userEventActivate(probe.event);
  bool externallyWoken = waitForCoroutinePhase(probe, 1);

  // Wait well past the original sleep deadline. Its canceled generation must
  // not resume the second wait or leave it a credit.
  std::this_thread::sleep_for(200ms);
  EXPECT_TRUE(externallyWoken);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u)
    << "late tick from the externally-woken sleep resumed the next wait";

  if (probe.phase.load(std::memory_order_acquire) < 2)
    userEventActivate(probe.event);
  bool secondWake = waitForCoroutinePhase(probe, 2);
  userEventStopTimer(probe.event);
  loop.stop();

  EXPECT_TRUE(secondWake);
  bool finished = probe.phase.load(std::memory_order_acquire) == 2;
  if (!finished)
    coroutineDelete(coroutine);
  deleteUserEvent(probe.event);
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
