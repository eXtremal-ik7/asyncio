// aioUserEvent contract tests. The core_* suites exercise the protocol's
// decision machinery on TestBackend; user_event* suites exercise the public
// API on the real backend. The `event` family keeps focused regressions for
// defects found while implementing the contract.

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

void coreEventCallback(aioUserEvent*, void *arg)
{
  static_cast<EventContext*>(arg)->finishes++;
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

  EXPECT_TRUE(eventReferenceTryActivate(&event));
  EXPECT_FALSE(eventReferenceTryActivate(&event));
  eventReferenceDeactivate(&event);
  EXPECT_TRUE(eventReferenceTryActivate(&event));
  eventReferenceDeactivate(&event);

  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 2u);
  ASSERT_TRUE(eventReferenceMarkDeleting(&event));
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

  EXPECT_TRUE(eventReferenceTryActivate(&event));
  EXPECT_TRUE(eventReferenceTryActivate(&event));
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
  eventReferenceDeactivate(&event);
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
  ASSERT_TRUE(eventReferenceMarkDeleting(&event));
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(context.destructors, 0u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 2u);
  EXPECT_EQ(context.destructors, 0u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 1u);
  EXPECT_EQ(context.destructors, 1u);

  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, &event);
}

TEST(core_user_event, stale_timer_generation_cannot_touch_restarted_schedule)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 1, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  userEventStartTimer(event, 100, 2);
  uintptr_t firstGeneration = backend.lastEventTimerGeneration;
  ASSERT_NE(firstGeneration, 0u);
  EXPECT_EQ(backend.startTimerCalls, 1u);

  eventTimerSignalTick(event, firstGeneration);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(backend.rearmTimerCalls, 1u);

  userEventStartTimer(event, 200, 1);
  uintptr_t secondGeneration = backend.lastEventTimerGeneration;
  ASSERT_NE(secondGeneration, firstGeneration);
  EXPECT_EQ(backend.startTimerCalls, 2u);
  EXPECT_EQ(backend.stopTimerCalls, 1u);

  // The bucket generation changed before backend restart. Even a kernel
  // delivery which had already copied the old event pointer cannot enqueue a
  // control pass or spend the new counter.
  eventTimerSignalTick(event, firstGeneration);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);

  eventTimerSignalTick(event, secondGeneration);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 2u);
  EXPECT_EQ(backend.stopTimerCalls, 2u); // finite counter exhausted

  deleteUserEvent(event);
  EXPECT_EQ(context.destructors, 1u);
}

// A POSIX harvested timer envelope may still hold physically grace-protected
// timer/event storage after stop released the last logical reference. Signal
// publication must fail instead of retaining 0 -> 1 and queuing a second
// terminal control pass.
TEST(core_user_event, retired_timer_signal_cannot_resurrect_zero_ref_event)
{
  TestBackend backend;
  aioUserEvent event{};
  event.base = &backend.base;
  event.root.objectPool = &backend.operationPool;
  event.root.opCode = actUserEvent;
  event.tag = TAG_EVENT_DELETE;
  event.timerSignals.high = 42;

  eventTimerSignalTick(&event, 42);

  EXPECT_EQ(event.tag, TAG_EVENT_DELETE);
  EXPECT_EQ(event.timerSignals.low, 0u);
  EXPECT_EQ(event.timerControlState, 0u);
  EXPECT_TRUE(backend.completions.empty());
}

TEST(core_user_event, delete_reconciles_a_start_already_inside_backend_arm)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  std::mutex mutex;
  std::condition_variable changed;
  bool insideStart = false;
  bool allowStartReturn = false;
  backend.eventTimerHook = [&](aioUserEvent*, EventTimerUpdate update, uintptr_t, uint64_t) {
    if (update == etuStart) {
      std::unique_lock<std::mutex> lock(mutex);
      insideStart = true;
      changed.notify_all();
      changed.wait(lock, [&]() { return allowStartReturn; });
    }
    return 1;
  };

  eventIncrementReference(event, 1);
  std::thread controller([&]() {
    userEventStartTimer(event, 100, 1);
    eventDecrementReference(event, 1);
  });

  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(changed.wait_for(lock, 1500ms, [&]() { return insideStart; }));
  }
  deleteUserEvent(event);
  EXPECT_EQ(context.destructors, 0u);

  {
    std::lock_guard<std::mutex> lock(mutex);
    allowStartReturn = true;
  }
  changed.notify_all();
  controller.join();

  EXPECT_EQ(backend.startTimerCalls, 1u);
  EXPECT_EQ(backend.stopTimerCalls, 1u);
  EXPECT_EQ(context.finishes, 0u);
  EXPECT_EQ(context.destructors, 1u);
}

TEST(core_global_queue, user_event_branch_deactivates_and_returns_when_empty)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.opCode = actUserEvent;
  event.root.finishMethod = eventFinish;
  event.root.arg = &context;
  event.tag = 1;
  event.isSemaphore = 0;
  ASSERT_TRUE(eventReferenceTryActivate(&event));
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
  drainQueuedUserEvents(gBase);
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
  eventIncrementReference(event, 1);
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

  // delete is logically synchronous but physical timer/waiter reconciliation
  // is a queued loop task; keep draining until that internal reference drops.
  drainQueuedUserEvents(gBase);

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

TEST(user_event_coroutine, delete_resumes_waiter_before_final_destruction)
{
  CoroutineEventProbe probe;
  PublicEventProbe lifetime;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);
  eventSetDestructorCb(probe.event, publicEventDestructor, &lifetime);

  coroutineTy *coroutine = coroutineNew(waitOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  deleteUserEvent(probe.event);
  EXPECT_EQ(publicEventDestructorCount(lifetime), 0u)
    << "active waiter reference must keep terminal event storage alive";
  drainQueuedUserEvents(gBase);

  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  EXPECT_EQ(publicEventDestructorCount(lifetime), 1u)
    << "destructor must follow waiter resume and waiter-reference release";
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

  ASSERT_TRUE(eventReferenceTryActivate(&event));  // real protocol: tag = TAG_EVENT_OP + 2
  concurrentQueuePush(&backend.base.globalQueue, &event.root);
  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(event.tag, 1u) << "delivery reference was not released after the callback";

  ASSERT_TRUE(eventReferenceMarkDeleting(&event));
  eventDecrementReference(&event, 1);
  EXPECT_EQ(context.destructors, 1u) << "leaked delivery reference blocks the destructor forever";
  void *recycled = nullptr;
  EXPECT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled))
    << "event storage never returns to the pool";
  if (recycled)
    EXPECT_EQ(recycled, static_cast<void*>(&event));
}

// Activation must be rejected once deletion has begun while the caller's
// strong reference keeps the storage alive. Calling after the final reference
// was released is outside the API contract and must not be used as a hot-path
// hardening requirement.
TEST(event, semaphore_activation_after_delete_with_owned_reference_is_rejected)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.root.opCode = actUserEvent;
  event.root.finishMethod = eventFinish;
  event.root.arg = &context;
  event.tag = 2; // deleting holder + independently activating holder
  event.isSemaphore = 1;
  eventSetDestructorCb(&event, eventDestructor, &context);

  ASSERT_TRUE(eventReferenceMarkDeleting(&event));
  eventDecrementReference(&event, 1); // delete consumes its holder's reference
  ASSERT_EQ(context.destructors, 0u);

  uintptr_t deleting = event.tag;
  EXPECT_FALSE(eventReferenceTryActivate(&event));
  EXPECT_EQ(event.tag, deleting);

  // The activating thread now releases the reference that made its failed
  // call valid; this is the one and only terminal release.
  eventDecrementReference(&event, 1);
  ASSERT_EQ(context.destructors, 1u);
  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, static_cast<void*>(&event));
}

// Green companion: the non-semaphore branch already rejects activation on a
// deleting event and rolls the tag back intact - the semaphore fix above
// must keep this.
TEST(event, nonsemaphore_activation_after_delete_leaves_tag_intact)
{
  aioUserEvent event{};
  event.isSemaphore = 0;
  event.tag = TAG_EVENT_DELETE + 1; // caller still owns this reference
  EXPECT_FALSE(eventReferenceTryActivate(&event));
  EXPECT_EQ(event.tag, TAG_EVENT_DELETE + 1);
}

// The semaphore fast path optimistically increments and rolls back when it
// observes DELETE. A valid activating thread still owns its original strong
// reference throughout that transient, so a concurrent delete release cannot
// be final until the activation thread finishes and releases its ownership.
TEST(event, semaphore_activation_racing_delete_preserves_final_release)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.root.opCode = actUserEvent;
  event.isSemaphore = 1;
  eventSetDestructorCb(&event, eventDestructor, &context);

  event.tag = 2;
  ASSERT_TRUE(eventReferenceMarkDeleting(&event));

  constexpr int kAttempts = 200000;
  std::atomic<bool> activationAccepted{false};
  std::thread releaseThread([&]() {
    eventDecrementReference(&event, 1); // delete holder
  });
  std::thread activateThread([&]() {
    for (int i = 0; i < kAttempts; i++) {
      if (eventReferenceTryActivate(&event))
        activationAccepted.store(true, std::memory_order_relaxed);
    }
    eventDecrementReference(&event, 1); // activation holder
  });
  releaseThread.join();
  activateThread.join();

  EXPECT_FALSE(activationAccepted.load(std::memory_order_relaxed))
    << "activation accepted on a deleting event";
  EXPECT_EQ(context.destructors, 1u);
  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, static_cast<void*>(&event));
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
  deleteUserEvent(event);
  backend.drainCompletions();

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
  backend.drainCompletions();
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

  ASSERT_TRUE(eventReferenceTryActivate(&event));
  concurrentQueuePush(&backend.base.globalQueue, &event.root);
  currentFinishedSync = 17;
  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(currentFinishedSync, 0u) << "sync budget leaks through the event callback boundary";
  currentFinishedSync = 0;  // keep the sabotage out of the following tests
}

} // namespace
