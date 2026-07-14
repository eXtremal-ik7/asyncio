// aioUserEvent contract tests. The core_* suites exercise the protocol's
// decision machinery on TestBackend; user_event* suites exercise the public
// API on the real backend. The `event` family keeps focused regressions for
// defects found while implementing the contract.

#include "coretest.h"
#include "reactor.h"
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

bool waitForEventCallbacks(PublicEventProbe &probe, unsigned expected, std::chrono::milliseconds timeout = 1500ms)
{
  std::unique_lock<std::mutex> lock(probe.mutex);
  return probe.changed.wait_for(lock, timeout, [&]() {
    return probe.callbacks >= expected;
  });
}

class EventLoopThread {
public:
  explicit EventLoopThread(asyncBase *baseArg) :
    base(baseArg) {
    loop = std::thread([this]() {
      loopThreadId = std::this_thread::get_id();
      started.store(true, std::memory_order_release);
      asyncLoop(base);
    });
    while (!started.load(std::memory_order_acquire))
      std::this_thread::yield();
  }

  ~EventLoopThread() { stop(); }

  EventLoopThread(const EventLoopThread&) = delete;
  EventLoopThread &operator=(const EventLoopThread&) = delete;

  void stop() {
    if (loop.joinable()) {
      postQuitOperation(base);
      loop.join();
    }
  }

  std::thread::id id() const { return loopThreadId; }

private:
  asyncBase *base;
  std::thread loop;
  std::atomic<bool> started{false};
  std::thread::id loopThreadId;
};

void drainQueuedUserEvents(asyncBase *base)
{
  struct DrainBarrier {
    std::mutex mutex;
    std::condition_variable changed;
    bool reached = false;
  } barrier;
  auto callback = [](aioUserEvent*, void *arg) {
    DrainBarrier *state = static_cast<DrainBarrier*>(arg);
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->reached = true;
    }
    state->changed.notify_one();
  };

  // A personal kernel event is no longer a globalQueue node, so appending a
  // quit marker would overtake it. Trigger a second personal event instead:
  // once its callback runs, the loop still finishes the whole harvested
  // kernel batch before observing the quit posted below.
  aioUserEvent *fence = newUserEvent(base, 0, callback, &barrier);
  ASSERT_NE(fence, nullptr);
  userEventActivate(fence);
  EventLoopThread loop(base);
  {
    std::unique_lock<std::mutex> lock(barrier.mutex);
    ASSERT_TRUE(barrier.changed.wait_for(lock, 1500ms, [&]() {
      return barrier.reached;
    }));
  }
  loop.stop();
  deleteUserEvent(fence);
}

TEST(core_user_event, nonsemaphore_activation_coalesces_until_deactivated)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  userEventActivate(event);
  userEventActivate(event);
  EXPECT_EQ(backend.completions.size(), 1u);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);

  userEventActivate(event);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 2u);

  deleteUserEvent(event);
  EXPECT_EQ(context.destructors, 1u);

  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.base.eventPool, &recycled));
  EXPECT_EQ(recycled, event);
  concurrentQueuePush(&backend.base.eventPool, recycled);
}

TEST(core_user_event, semaphore_counts_each_activation)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 1, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);

  userEventActivate(event);
  userEventActivate(event);
  EXPECT_EQ(backend.completions.size(), 1u) << "the personal kernel event is only a doorbell";
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 2u);
  deleteUserEvent(event);
}

TEST(core_user_event, claimed_manual_batch_survives_delete)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 1, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  userEventActivate(event);
  userEventActivate(event);
  ASSERT_EQ(backend.completions.size(), 1u);

  // TestBackend claims the kernel-delivery reference in activateEvent().
  // Delete closes admission, but that accepted semaphore batch must drain.
  deleteUserEvent(event);
  EXPECT_EQ(context.destructors, 0u);
  backend.drainCompletions();

  EXPECT_EQ(context.finishes, 2u);
  EXPECT_EQ(context.destructors, 1u);
}

TEST(core_user_event, queued_timer_delivery_survives_delete)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 1, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  userEventStartTimer(event, 100, 1);
  eventTimerSignalTick(event, backend.lastEventTimerGeneration, eventHandleGeneration(event));
  ASSERT_EQ(backend.completions.size(), 1u);
  ASSERT_TRUE(eventIsQueueTask(backend.completions.front()));

  deleteUserEvent(event);
  EXPECT_EQ(context.destructors, 0u);
  backend.drainCompletions();

  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(context.destructors, 1u);
}

TEST(core_user_event, timer_state_is_lazy_for_a_manual_only_pool_cell)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);

  EXPECT_EQ(sizeof(aioUserEvent), 112u);
  EXPECT_EQ(offsetof(aioUserEvent, timerControl), (size_t)CACHE_LINE_SIZE);
  EXPECT_EQ(sizeof(aioEventTimerState), 24u);
  EXPECT_EQ(eventTimerDataLoad(event, amoAcquire), nullptr);

  // Neither manual delivery nor a redundant Stop turns a manual-only cell
  // into a timer-bearing one.
  userEventActivate(event);
  backend.drainCompletions();
  userEventStopTimer(event);
  EXPECT_EQ(eventTimerDataLoad(event, amoAcquire), nullptr);

  userEventStartTimer(event, 100, 1);
  EXPECT_NE(eventTimerDataLoad(event, amoAcquire), nullptr);
  EXPECT_EQ(backend.startTimerCalls, 1u);
  userEventStopTimer(event);
  deleteUserEvent(event);
}

TEST(core_user_event, strong_references_delay_final_release_after_delete)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, nullptr, nullptr);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  eventIncrementReference(event, 2);
  EXPECT_EQ(__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 3u);
  deleteUserEvent(event);
  EXPECT_EQ(__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 2u);
  EXPECT_EQ(context.destructors, 0u);
  eventDecrementReference(event, 1);
  EXPECT_EQ(__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 1u);
  EXPECT_EQ(context.destructors, 0u);
  eventDecrementReference(event, 1);
  EXPECT_EQ(context.destructors, 1u);

  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.base.eventPool, &recycled));
  EXPECT_EQ(recycled, event);
  concurrentQueuePush(&backend.base.eventPool, recycled);
}

TEST(core_user_event, stale_timer_generation_cannot_touch_restarted_schedule)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 1, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  userEventStartTimer(event, 100, 2);
  uint32_t firstGeneration = backend.lastEventTimerGeneration;
  uint64_t incarnation = eventHandleGeneration(event);
  ASSERT_NE(firstGeneration, 0u);
  EXPECT_EQ(backend.startTimerCalls, 1u);

  eventTimerSignalTick(event, firstGeneration, incarnation);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(backend.rearmTimerCalls, 1u);

  userEventStartTimer(event, 200, 1);
  uint32_t secondGeneration = backend.lastEventTimerGeneration;
  ASSERT_NE(secondGeneration, firstGeneration);
  EXPECT_EQ(backend.startTimerCalls, 2u);
  EXPECT_EQ(backend.stopTimerCalls, 1u);

  // The bucket generation changed before backend restart. Even a kernel
  // delivery which had already copied the old event pointer cannot enqueue a
  // control pass or spend the new counter.
  eventTimerSignalTick(event, firstGeneration, incarnation);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);

  eventTimerSignalTick(event, secondGeneration, incarnation);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 2u);
  EXPECT_EQ(backend.stopTimerCalls, 2u); // finite counter exhausted

  deleteUserEvent(event);
  EXPECT_EQ(context.destructors, 1u);
}

TEST(core_user_event, epoll_probe_requires_confirmation_and_rearms_oneshot)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 1, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);

  unsigned consumeCalls = 0;
  backend.eventTimerHook = [&](aioUserEvent*, EventTimerUpdate update, uint32_t, uint64_t) {
    if (update != etuConsume)
      return 1;
    // First probe models EAGAIN from an old envelope; the second models the
    // current timerfd being readable. Only the latter is a timer tick.
    return ++consumeCalls == 1 ? 1 : 2;
  };

  userEventStartTimer(event, 100, 2);
  uint32_t generation = backend.lastEventTimerGeneration;
  uint64_t incarnation = eventHandleGeneration(event);

  eventTimerSignalProbe(event, generation, incarnation);
  backend.drainCompletions();
  EXPECT_EQ(consumeCalls, 1u);
  EXPECT_EQ(context.finishes, 0u);
  EXPECT_EQ(backend.rearmTimerCalls, 1u) << "even a stale probe consumed the EPOLLONESHOT registration";

  eventTimerSignalProbe(event, generation, incarnation);
  backend.drainCompletions();
  EXPECT_EQ(consumeCalls, 2u);
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(backend.rearmTimerCalls, 2u);

  deleteUserEvent(event);
}

// A POSIX harvested timer envelope may still hold its permanently paired
// timer/event storage after stop released the last logical reference. Signal
// publication must fail instead of retaining 0 -> 1 and queuing a second
// terminal control pass.
TEST(core_user_event, retired_timer_signal_cannot_resurrect_zero_ref_event)
{
  TestBackend backend;
  aioUserEvent event{};
  event.header.base = &backend.base;
  objectHeaderSetType(&event.header, ohtUserEvent);
  __uint64_atomic_store(&event.header.tag.low, TAG_EVENT_DELETE, amoRelaxed);
  __uint64_atomic_store(&event.header.tag.high, 1, amoRelaxed);
  eventTimerSignalTick(&event, 42, 1);

  EXPECT_EQ(__uint64_atomic_load(&event.header.tag.low, amoRelaxed), TAG_EVENT_DELETE);
  EXPECT_EQ(eventTimerDataLoad(&event, amoRelaxed), nullptr);
  EXPECT_TRUE(backend.completions.empty());
}

TEST(core_user_event, compact_kernel_claim_accepts_matching_generation_wrap)
{
  alignas(64) aioUserEvent event{};
  uint64_t fullGeneration = REACTOR_HANDLE_GENERATION_MASK + 42;
  __uint64_atomic_store(&event.header.tag.low, 1, amoRelaxed);
  __uint64_atomic_store(&event.header.tag.high, fullGeneration, amoRelaxed);
  objectHeaderSetType(&event.header, ohtUserEvent);

  uint64_t encoded = reactorHandleEncode(&event.header);
  uint64_t decodedGeneration = 0;
  objectHeader *decodedHeader = reactorHandleDecode(encoded, &decodedGeneration);
  aioUserEvent *decoded = (aioUserEvent*)decodedHeader;
  EXPECT_EQ(decodedGeneration, fullGeneration);
  EXPECT_TRUE(eventTimerTryClaimReference(decoded, decodedGeneration));
  EXPECT_EQ(__uint64_atomic_load(&event.header.tag.low, amoRelaxed), 2u);
  eventReferenceDropNonFinal(&event, 1);
}

TEST(core_user_event, stale_timer_incarnation_cannot_claim_recycled_event)
{
  TestBackend backend;
  EventContext firstContext;
  aioUserEvent *first = newUserEvent(&backend.base, 1, coreEventCallback, &firstContext);
  ASSERT_NE(first, nullptr);

  userEventStartTimer(first, 100, 1);
  uint64_t firstIncarnation = eventHandleGeneration(first);
  uint32_t firstGeneration = backend.lastEventTimerGeneration;
  aioEventTimerState *firstTimerData = eventTimerDataLoad(first, amoAcquire);
  ASSERT_NE(firstTimerData, nullptr);
  void *pairedTimer = firstTimerData->timerId;
  deleteUserEvent(first);

  EventContext secondContext;
  aioUserEvent *second = newUserEvent(&backend.base, 1, coreEventCallback, &secondContext);
  ASSERT_EQ(second, first);
  EXPECT_EQ(eventTimerDataLoad(second, amoAcquire)->timerId, pairedTimer);
  uint64_t secondIncarnation = eventHandleGeneration(second);
  EXPECT_EQ(secondIncarnation & REACTOR_HANDLE_GENERATION_MASK, (firstIncarnation & REACTOR_HANDLE_GENERATION_MASK) + 1);

  userEventStartTimer(second, 100, 1);
  uint32_t secondGeneration = backend.lastEventTimerGeneration;
  EXPECT_EQ(secondGeneration, firstGeneration) << "schedule generations may restart because event incarnation is the ABA guard";

  // Model a stale kernel envelope whose generation happens to match the live
  // bucket but whose arm belonged to the previous logical event.
  eventTimerSignalTick(second, secondGeneration, firstIncarnation);
  backend.drainCompletions();
  EXPECT_EQ(secondContext.finishes, 0u);

  eventTimerSignalTick(second, secondGeneration, secondIncarnation);
  backend.drainCompletions();
  EXPECT_EQ(secondContext.finishes, 1u);
  deleteUserEvent(second);
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
  backend.eventTimerHook = [&](aioUserEvent*, EventTimerUpdate update, uint32_t, uint64_t) {
    if (update == etuStart) {
      std::unique_lock<std::mutex> lock(mutex);
      insideStart = true;
      changed.notify_all();
      changed.wait(lock, [&]() {
        return allowStartReturn;
      });
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
    ASSERT_TRUE(changed.wait_for(lock, 1500ms, [&]() {
      return insideStart;
    }));
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

TEST(core_user_event, timer_publisher_does_not_wait_for_a_busy_kernel_owner)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);

  userEventStartTimer(event, 100, 2);
  uint32_t firstGeneration = backend.lastEventTimerGeneration;
  uint64_t incarnation = eventHandleGeneration(event);

  std::mutex mutex;
  std::condition_variable changed;
  bool insideRearm = false;
  bool allowRearm = false;
  backend.eventTimerHook = [&](aioUserEvent*, EventTimerUpdate update, uint32_t generation, uint64_t) {
    if (update == etuRearm && generation == firstGeneration) {
      std::unique_lock<std::mutex> lock(mutex);
      insideRearm = true;
      changed.notify_all();
      changed.wait(lock, [&]() {
        return allowRearm;
      });
    }
    return 1;
  };

  std::thread tick([&]() {
    eventTimerSignalTick(event, firstGeneration, incarnation);
  });
  bool ownerReachedRearm;
  {
    std::unique_lock<std::mutex> lock(mutex);
    ownerReachedRearm = changed.wait_for(lock, 1500ms, [&]() {
      return insideRearm;
    });
  }
  if (!ownerReachedRearm) {
    tick.join();
    backend.drainCompletions();
    deleteUserEvent(event);
    FAIL() << "kernel tick did not enter the owner rearm path";
    return;
  }

  bool publisherReturned = false;
  eventIncrementReference(event, 1);
  std::thread publisher([&]() {
    userEventStartTimer(event, 200, 1);
    {
      std::lock_guard<std::mutex> lock(mutex);
      publisherReturned = true;
    }
    changed.notify_all();
    eventDecrementReference(event, 1);
  });
  {
    std::unique_lock<std::mutex> lock(mutex);
    bool returnedWithoutOwner = changed.wait_for(lock, 1500ms, [&]() {
      return publisherReturned;
    });
    allowRearm = true;
    EXPECT_TRUE(returnedWithoutOwner);
  }
  changed.notify_all();
  publisher.join();
  tick.join();

  uint32_t secondGeneration = backend.lastEventTimerGeneration;
  EXPECT_NE(secondGeneration, firstGeneration);
  EXPECT_EQ(backend.startTimerCalls, 2u);
  EXPECT_EQ(backend.stopTimerCalls, 1u);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);

  eventTimerSignalTick(event, secondGeneration, incarnation);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 2u);
  deleteUserEvent(event);
}

TEST(core_user_event, competing_timer_tick_notifies_the_existing_owner)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);

  userEventStartTimer(event, 100, 3);
  uint32_t generation = backend.lastEventTimerGeneration;
  uint64_t incarnation = eventHandleGeneration(event);

  std::mutex mutex;
  std::condition_variable changed;
  bool insideRearm = false;
  bool allowRearm = false;
  backend.eventTimerHook = [&](aioUserEvent*, EventTimerUpdate update, uint32_t, uint64_t) {
    if (update == etuRearm) {
      std::unique_lock<std::mutex> lock(mutex);
      if (!insideRearm) {
        insideRearm = true;
        changed.notify_all();
        changed.wait(lock, [&]() {
          return allowRearm;
        });
      }
    }
    return 1;
  };

  std::thread firstTick([&]() {
    eventTimerSignalTick(event, generation, incarnation);
  });
  bool ownerReachedRearm;
  {
    std::unique_lock<std::mutex> lock(mutex);
    ownerReachedRearm = changed.wait_for(lock, 1500ms, [&]() {
      return insideRearm;
    });
  }
  if (!ownerReachedRearm) {
    firstTick.join();
    backend.drainCompletions();
    deleteUserEvent(event);
    FAIL() << "first tick did not enter the owner rearm path";
    return;
  }
  std::thread secondTick([&]() {
    eventTimerSignalTick(event, generation, incarnation);
  });
  secondTick.join();
  {
    std::lock_guard<std::mutex> lock(mutex);
    allowRearm = true;
  }
  changed.notify_all();
  firstTick.join();

  ASSERT_EQ(backend.completions.size(), 1u);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 2u);
  EXPECT_EQ(backend.rearmTimerCalls, 2u);

  eventTimerSignalTick(event, generation, incarnation);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 3u);
  deleteUserEvent(event);
}

TEST(core_global_queue, timer_delivery_branch_deactivates_and_returns_when_empty)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  userEventStartTimer(event, 100, 1);
  aioEventTimerState *timer = eventTimerDataLoad(event, amoAcquire);
  ASSERT_NE(timer, nullptr);
  __uintptr_atomic_store(&timer->deliveryState, 1, amoRelease);
  eventIncrementReference(event, 1); // tagged delivery reference
  concurrentQueuePush(&backend.base.globalQueue, eventDeliveryNode(event));

  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_EQ(context.finishes, 1u);
  userEventStopTimer(event);
  EXPECT_EQ(__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 1u);
  deleteUserEvent(event);
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
  for (std::thread &producer: producers) {
    eventIncrementReference(event, 1);
    producer = std::thread([event]() {
      for (unsigned i = 0; i < kActivationsPerThread; ++i)
        userEventActivate(event);
      eventDecrementReference(event, 1);
    });
  }
  for (std::thread &producer: producers)
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
  for (std::thread &producer: producers) {
    eventIncrementReference(event, 1);
    producer = std::thread([event]() {
      for (unsigned i = 0; i < kActivationsPerThread; ++i)
        userEventActivate(event);
      eventDecrementReference(event, 1);
    });
  }
  for (std::thread &producer: producers)
    producer.join();

  drainQueuedUserEvents(gBase);
  EXPECT_EQ(publicEventCallbackCount(probe), kThreads * kActivationsPerThread);
  deleteUserEvent(event);
}

struct SelfReactivationProbe: PublicEventProbe {
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
    probe->destructorSawAllCallbacksComplete = probe->completedCallbacks == probe->callbacks;
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
  probe->changed.wait(lock, [&]() {
    return probe->allowCallbackReturn;
  });
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

  for (std::thread &producer: producers) {
    // Retain before publishing the pointer to the new thread.
    eventIncrementReference(event, 1);
    producer = std::thread([&]() {
      {
        std::unique_lock<std::mutex> lock(barrierMutex);
        ++ready;
        barrierChanged.notify_all();
        barrierChanged.wait(lock, [&]() {
          return mayActivate;
        });
      }

      // The owner has closed the event, but this thread's strong reference
      // keeps the pointer valid and makes the operation a safe terminal no-op.
      userEventActivate(event);

      {
        std::unique_lock<std::mutex> lock(barrierMutex);
        ++observedDelete;
        barrierChanged.notify_all();
        barrierChanged.wait(lock, [&]() {
          return mayRelease;
        });
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

  for (std::thread &producer: producers)
    producer.join();

  // delete is logically synchronous but physical timer/waiter reconciliation
  // is a queued loop task; keep draining until that internal reference drops.
  drainQueuedUserEvents(gBase);

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 0u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, 1u);
}

TEST(user_event_lifetime, readiness_not_claimed_before_delete_may_be_dropped)
{
  EventLifetimeProbe probe;
  aioUserEvent *event = newUserEvent(gBase, 0, lifetimeContractEventCb, &probe);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeContractDestructor, &probe);

  userEventActivate(event);
  deleteUserEvent(event);
  drainQueuedUserEvents(gBase);

  std::lock_guard<std::mutex> lock(probe.mutex);
  EXPECT_EQ(probe.callbacks, 0u);
  EXPECT_EQ(probe.completedCallbacks, 0u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.lastCallbackSequence, 0u);
  EXPECT_EQ(probe.destructorSequence, 1u);
  EXPECT_TRUE(probe.destructorSawAllCallbacksComplete);
}

TEST(user_event_lifetime, unclaimed_semaphore_readiness_may_be_dropped_on_delete)
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
  EXPECT_EQ(probe.callbacks, 0u);
  EXPECT_EQ(probe.completedCallbacks, 0u);
  EXPECT_EQ(probe.destructors, 1u);
  EXPECT_EQ(probe.destructorSequence, 1u);
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
    EXPECT_EQ(probe.destructors, 0u) << "destructor ran while its accepted callback was still executing";
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
      probe->changed.wait(lock, [&]() {
        return probe->allowCallbackReturn;
      });
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
    probe->destructorSawAllCallbacksComplete = probe->completedCallbacks == probe->callbacks;
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
  for (int counter: {0, -1}) {
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
  for (std::thread &producer: producers) {
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
  for (std::thread &producer: producers)
    producer.join();

  drainQueuedUserEvents(gBase);
  EXPECT_EQ(publicEventCallbackCount(probe), kProducerThreads * kActivationsPerThread);
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

void retainCoroutineEvent(CoroutineEventProbe *probe)
{
  // The coroutine owns this reference for the complete helper call, including
  // suspension. The test/main coroutine keeps the initial reference for its
  // concurrent Activate/Delete calls.
  eventIncrementReference(probe->event, 1);
}

void releaseCoroutineEvent(CoroutineEventProbe *probe)
{
  eventDecrementReference(probe->event, 1);
}

bool waitForCoroutinePhase(CoroutineEventProbe &probe, unsigned expected, std::chrono::milliseconds timeout = 1500ms)
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
  releaseCoroutineEvent(probe);
  probe->phase.store(1, std::memory_order_release);
}

void waitManyCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  for (unsigned i = 0; i < probe->waits; ++i) {
    ioWaitUserEvent(probe->event);
    if (i + 1 != probe->waits)
      probe->phase.store(i + 1, std::memory_order_release);
  }
  releaseCoroutineEvent(probe);
  probe->phase.store(probe->waits, std::memory_order_release);
}

void sleepOnceCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  ioSleep(probe->event, 20000);
  releaseCoroutineEvent(probe);
  probe->phase.store(1, std::memory_order_release);
}

void externallyWokenSleepCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  ioSleep(probe->event, 100000);
  probe->phase.store(1, std::memory_order_release);
  ioWaitUserEvent(probe->event);
  releaseCoroutineEvent(probe);
  probe->phase.store(2, std::memory_order_release);
}

void longSleepCoroutine(void *arg)
{
  CoroutineEventProbe *probe = static_cast<CoroutineEventProbe*>(arg);
  ioSleep(probe->event, 60000000);
  releaseCoroutineEvent(probe);
  probe->phase.store(1, std::memory_order_release);
}

TEST(core_user_event, delete_resumes_waiter_without_timer_state)
{
  TestBackend backend;
  CoroutineEventProbe probe;
  EventContext lifetime;
  probe.event = newUserEvent(&backend.base, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);
  eventSetDestructorCb(probe.event, eventDestructor, &lifetime);

  coroutineTy *coroutine = coroutineNew(waitOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  retainCoroutineEvent(&probe);
  ASSERT_EQ(coroutineCall(coroutine), 0);
  ASSERT_EQ(eventTimerDataLoad(probe.event, amoAcquire), nullptr);
  EXPECT_EQ(__uint64_atomic_load(&probe.event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 2u)
      << "waiting must borrow the coroutine's reference, not retain another";
  EXPECT_NE(__uint64_atomic_load(&probe.event->header.tag.low, amoRelaxed) & TAG_EVENT_WAITER_COMMITTED, 0u);

  deleteUserEvent(probe.event);
  EXPECT_EQ(lifetime.destructors, 0u);
  backend.drainCompletions();

  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  EXPECT_EQ(lifetime.destructors, 1u);
}

TEST(core_user_event, delete_cancellation_is_sticky_against_late_timer_delivery)
{
  TestBackend backend;
  CoroutineEventProbe probe;
  EventContext lifetime;
  probe.event = newUserEvent(&backend.base, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);
  eventSetDestructorCb(probe.event, eventDestructor, &lifetime);
  userEventStartTimer(probe.event, 100, 1);

  coroutineTy *coroutine = coroutineNew(waitOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  retainCoroutineEvent(&probe);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  aioEventTimerState *timer = eventTimerDataLoad(probe.event, amoAcquire);
  ASSERT_NE(timer, nullptr);
  eventIncrementReference(probe.event, 1); // keep the terminal state inspectable
  deleteUserEvent(probe.event);

  // Model a tick which won admission before Delete but published its queued
  // delivery after the cancellation task. It may be drained, but it must not
  // recreate a credit after the waiter has been resumed.
  __uintptr_atomic_store(&timer->deliveryState, 1, amoRelease);
  eventIncrementReference(probe.event, 1);
  backend.completions.push_back(eventDeliveryNode(probe.event));
  backend.drainCompletions();

  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  EXPECT_EQ(__uintptr_atomic_load(&probe.event->waiter, amoRelaxed), (uintptr_t)ewtDeleted);
  EXPECT_EQ(__uintptr_atomic_load(&timer->deliveryState, amoRelaxed), 0u);
  EXPECT_EQ(lifetime.destructors, 0u);
  eventDecrementReference(probe.event, 1);
  EXPECT_EQ(lifetime.destructors, 1u);
}

TEST(core_user_event, manual_wake_discards_an_already_queued_sleep_tick)
{
  TestBackend backend;
  CoroutineEventProbe probe;
  probe.event = newUserEvent(&backend.base, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(externallyWokenSleepCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  retainCoroutineEvent(&probe);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  aioEventTimerState *timer = eventTimerDataLoad(probe.event, amoAcquire);
  ASSERT_NE(timer, nullptr);
  uint32_t generation = eventTimerGeneration(probe.event);
  eventTimerSignalTick(probe.event, generation, eventHandleGeneration(probe.event));
  userEventActivate(probe.event);
  backend.drainCompletions();

  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u) << "the queued tick became a credit for the wait after ioSleep";
  if (probe.phase.load(std::memory_order_acquire) < 2) {
    userEventActivate(probe.event);
    backend.drainCompletions();
  }
  userEventStopTimer(probe.event);
  deleteUserEvent(probe.event);
  backend.drainCompletions();
}

TEST(core_user_event, semaphore_manual_wake_discards_an_already_queued_sleep_tick)
{
  TestBackend backend;
  CoroutineEventProbe probe;
  probe.event = newUserEvent(&backend.base, 1, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(externallyWokenSleepCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  retainCoroutineEvent(&probe);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  uint32_t generation = eventTimerGeneration(probe.event);
  eventTimerSignalTick(probe.event, generation, eventHandleGeneration(probe.event));
  userEventActivate(probe.event);
  backend.drainCompletions();

  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u) << "the simultaneous tick became a semaphore credit after cancelling ioSleep";
  userEventActivate(probe.event);
  backend.drainCompletions();
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 2u);

  userEventStopTimer(probe.event);
  deleteUserEvent(probe.event);
  backend.drainCompletions();
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
  retainCoroutineEvent(&probe);
  int finished = coroutineCall(coroutine);

  EXPECT_EQ(finished, 1);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  deleteUserEvent(probe.event);
  if (!finished)
    drainQueuedUserEvents(gBase);
}

TEST(user_event_coroutine, activation_resumes_an_installed_waiter)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(waitOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  retainCoroutineEvent(&probe);
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
  EXPECT_EQ(__uint64_atomic_load(&probe.event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 1u);
  EXPECT_EQ(__uint64_atomic_load(&probe.event->header.tag.low, amoRelaxed) & TAG_EVENT_WAITER_COMMITTED, 0u);
  deleteUserEvent(probe.event);
  if (!finished)
    drainQueuedUserEvents(gBase);
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
  retainCoroutineEvent(&probe);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  deleteUserEvent(probe.event);
  EXPECT_EQ(publicEventDestructorCount(lifetime), 0u) << "the coroutine's caller-owned reference must keep storage alive";
  drainQueuedUserEvents(gBase);

  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  EXPECT_EQ(publicEventDestructorCount(lifetime), 1u) << "destructor must follow waiter resume and external-reference release";
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
  retainCoroutineEvent(&probe);
  int finished = coroutineCall(coroutine);

  EXPECT_EQ(finished, 1);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), probe.waits);
  deleteUserEvent(probe.event);
  if (!finished)
    drainQueuedUserEvents(gBase);
}

TEST(user_event_coroutine, sleep_returns_on_its_timer)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(sleepOnceCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  retainCoroutineEvent(&probe);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  EventLoopThread loop(gBase);
  bool woke = waitForCoroutinePhase(probe, 1);
  userEventStopTimer(probe.event);
  loop.stop();

  EXPECT_TRUE(woke);
  deleteUserEvent(probe.event);
  drainQueuedUserEvents(gBase);
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
  retainCoroutineEvent(&probe);
  int finished = coroutineCall(coroutine);

  EXPECT_EQ(finished, 1);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u);
  if (!finished)
    userEventStopTimer(probe.event);
  deleteUserEvent(probe.event);
  if (!finished)
    drainQueuedUserEvents(gBase);
}

TEST(user_event_coroutine, external_wake_cancels_sleep_without_crediting_next_wait)
{
  CoroutineEventProbe probe;
  probe.event = newUserEvent(gBase, 0, nullptr, nullptr);
  ASSERT_NE(probe.event, nullptr);

  coroutineTy *coroutine = coroutineNew(externallyWokenSleepCoroutine, &probe, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  retainCoroutineEvent(&probe);
  ASSERT_EQ(coroutineCall(coroutine), 0);

  EventLoopThread loop(gBase);
  std::this_thread::sleep_for(10ms);
  userEventActivate(probe.event);
  bool externallyWoken = waitForCoroutinePhase(probe, 1);

  // Wait well past the original sleep deadline. Its canceled generation must
  // not resume the second wait or leave it a credit.
  std::this_thread::sleep_for(200ms);
  EXPECT_TRUE(externallyWoken);
  EXPECT_EQ(probe.phase.load(std::memory_order_acquire), 1u) << "late tick from the externally-woken sleep resumed the next wait";

  if (probe.phase.load(std::memory_order_acquire) < 2)
    userEventActivate(probe.event);
  bool secondWake = waitForCoroutinePhase(probe, 2);
  userEventStopTimer(probe.event);
  loop.stop();

  EXPECT_TRUE(secondWake);
  deleteUserEvent(probe.event);
  drainQueuedUserEvents(gBase);
}

// ---- compact-event regressions ---------------------------------------------

TEST(event, manual_kernel_reference_is_released_after_delivery)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);

  userEventActivate(event);
  EXPECT_EQ(__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 2u);
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_REF_MASK, 1u);

  deleteUserEvent(event);
  EXPECT_EQ(context.destructors, 1u);
}

void verifyActivationAfterDeleteIsIgnored(int isSemaphore)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, isSemaphore, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);
  eventIncrementReference(event, 1);

  deleteUserEvent(event);
  ASSERT_EQ(context.destructors, 0u);
  uintptr_t signal = __uintptr_atomic_load(&event->signalState, amoRelaxed);
  userEventActivate(event);
  EXPECT_EQ(__uintptr_atomic_load(&event->signalState, amoRelaxed), signal);
  EXPECT_TRUE(backend.completions.empty());

  eventDecrementReference(event, 1);
  EXPECT_EQ(context.destructors, 1u);
}

TEST(event, semaphore_activation_after_delete_is_ignored)
{
  verifyActivationAfterDeleteIsIgnored(1);
}

TEST(event, nonsemaphore_activation_after_delete_is_ignored)
{
  verifyActivationAfterDeleteIsIgnored(0);
}

TEST(event, activation_racing_delete_preserves_exactly_once_destruction)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 1, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, eventDestructor, &context);
  eventIncrementReference(event, 1); // producer ownership

  std::atomic<bool> start{false};
  std::thread producer([&]() {
    while (!start.load(std::memory_order_acquire))
      std::this_thread::yield();
    for (int i = 0; i < 200000; ++i)
      userEventActivate(event);
    eventDecrementReference(event, 1);
  });
  start.store(true, std::memory_order_release);
  deleteUserEvent(event);
  producer.join();
  backend.drainCompletions();

  EXPECT_EQ(context.destructors, 1u);
}

TEST(event, recycled_compact_event_reinitializes_protocol_fields)
{
  TestBackend backend;
  EventContext firstContext;
  aioUserEvent *first = newUserEvent(&backend.base, 1, coreEventCallback, &firstContext);
  ASSERT_NE(first, nullptr);
  uint64_t firstIncarnation = eventHandleGeneration(first);
  userEventActivate(first);
  backend.drainCompletions();
  deleteUserEvent(first);

  EventContext secondContext;
  aioUserEvent *second = newUserEvent(&backend.base, 0, coreEventCallback, &secondContext);
  ASSERT_EQ(second, first);
  EXPECT_EQ(eventHandleGeneration(second) & REACTOR_HANDLE_GENERATION_MASK, (firstIncarnation & REACTOR_HANDLE_GENERATION_MASK) + 1);
  EXPECT_EQ(__uintptr_atomic_load(&second->signalState, amoRelaxed), 0u);
  EXPECT_EQ(__uintptr_atomic_load(&second->waiter, amoRelaxed), 0u);
  EXPECT_EQ(eventTimerDataLoad(second, amoRelaxed), nullptr);
  EXPECT_EQ(second->callback, reinterpret_cast<void*>(coreEventCallback));
  EXPECT_EQ(second->arg, &secondContext);
  deleteUserEvent(second);
}

TEST(event, user_event_delivery_resets_sync_budget)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent *event = newUserEvent(&backend.base, 0, coreEventCallback, &context);
  ASSERT_NE(event, nullptr);

  userEventActivate(event);
  currentFinishedSync = 17;
  backend.drainCompletions();
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(currentFinishedSync, 0u);
  deleteUserEvent(event);
}

} // namespace
