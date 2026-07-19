// Message-loop lifecycle contract: postQuitOperation is a sticky
// level-triggered stop (every current AND future asyncLoop invocation
// returns), resetQuitOperation is the quiescent-only rearm that makes the
// base reusable for a next round of loop threads. The suite runs against the
// real OS backend: the defect class here is a late asyncLoop entrant missing
// a quit that was posted before it registered.

#include "unittest.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

using namespace std::chrono;
using namespace std::chrono_literals;

namespace {

// A loop hung by the defect under test sits in an eternal kernel wait, which
// would hang the whole gtest binary. The pump converts that hang into a
// measurable delay: after a grace period it re-posts quit until the caller
// reports completion, freeing one stuck entrant per post under the broken
// one-shot-token scheme. On the fixed code the pump never fires.
class QuitRescuePump {
public:
  QuitRescuePump(asyncBase *base, milliseconds grace) {
    pump = std::thread([this, base, grace]() {
      auto deadline = steady_clock::now() + grace;
      while (!done.load(std::memory_order_acquire)) {
        if (steady_clock::now() >= deadline) {
          rescued.fetch_add(1, std::memory_order_relaxed);
          postQuitOperation(base);
        }
        std::this_thread::sleep_for(50ms);
      }
    });
  }

  ~QuitRescuePump() {
    done.store(true, std::memory_order_release);
    pump.join();
  }

  unsigned rescues() const { return rescued.load(std::memory_order_relaxed); }

private:
  std::thread pump;
  std::atomic<bool> done{false};
  std::atomic<unsigned> rescued{0};
};

struct FireProbe {
  std::mutex mutex;
  std::condition_variable changed;
  unsigned fired = 0;
  asyncBase *base = nullptr;
  bool quitOnFire = true;
};

void fireProbeCallback(aioUserEvent*, void *arg)
{
  FireProbe *probe = static_cast<FireProbe*>(arg);
  {
    std::lock_guard<std::mutex> lock(probe->mutex);
    probe->fired++;
  }
  probe->changed.notify_all();
  // Runs on the loop thread; postQuitOperation is callable from any context
  if (probe->quitOnFire)
    postQuitOperation(probe->base);
}

bool waitForFire(FireProbe &probe, unsigned expected, milliseconds timeout)
{
  std::unique_lock<std::mutex> lock(probe.mutex);
  return probe.changed.wait_for(lock, timeout, [&]() {
    return probe.fired >= expected;
  });
}

} // namespace

// Quit posted before any thread entered the loop: every later entrant must
// observe it and return. The one-shot token consumed by the first entrant
// leaves the second one asleep forever.
TEST(loop_lifecycle, quit_before_entry_stops_every_later_asyncLoop)
{
  asyncBase *base = createAsyncBase(amOSDefault, 2);
  ASSERT_NE(base, nullptr);

  postQuitOperation(base);

  QuitRescuePump rescue(base, 3000ms);
  auto begin = steady_clock::now();
  asyncLoop(base);
  asyncLoop(base);
  auto elapsedMs = duration_cast<milliseconds>(steady_clock::now() - begin).count();

  EXPECT_LT(elapsedMs, 1500) << "a late asyncLoop entrant slept through an already-posted quit";
  EXPECT_EQ(rescue.rescues(), 0u);
}

// Entrants staggered in time against a single quit: whichever moment a thread
// registers, one posted quit must stop it. Under the token scheme the first
// entrant eats the token with nobody else registered (remaining == 0, no
// re-post), and every following entrant hangs.
TEST(loop_lifecycle, single_quit_covers_staggered_entrants)
{
  constexpr unsigned kThreads = 4;
  asyncBase *base = createAsyncBase(amOSDefault, kThreads);
  ASSERT_NE(base, nullptr);

  postQuitOperation(base);

  QuitRescuePump rescue(base, 3000ms);
  auto begin = steady_clock::now();
  std::vector<std::thread> entrants;
  for (unsigned i = 0; i < kThreads; i++) {
    entrants.emplace_back([base, i]() {
      std::this_thread::sleep_for(milliseconds(150 * i));
      asyncLoop(base);
    });
  }
  for (auto &thread : entrants)
    thread.join();
  auto elapsedMs = duration_cast<milliseconds>(steady_clock::now() - begin).count();

  EXPECT_LT(elapsedMs, 1500) << "a staggered asyncLoop entrant slept through the single posted quit";
  EXPECT_EQ(rescue.rescues(), 0u);
}

// resetQuitOperation must erase a quit that nobody ever consumed: the next
// round of loop threads runs normally instead of swallowing a stale stop
// (with the queue-token scheme the leftover token kills the first entrant of
// the new round).
TEST(loop_lifecycle, reset_clears_pending_quit_before_any_entry)
{
  asyncBase *base = createAsyncBase(amOSDefault, 1);
  ASSERT_NE(base, nullptr);

  postQuitOperation(base);
  resetQuitOperation(base);

  FireProbe probe;
  probe.base = base;
  aioUserEvent *event = newUserEvent(base, 0, fireProbeCallback, &probe);
  ASSERT_NE(event, nullptr);
  userEventStartTimer(event, 50000, 1);

  std::thread loop([base]() { asyncLoop(base); });
  bool delivered = waitForFire(probe, 1, 2000ms);
  if (!delivered)
    postQuitOperation(base);
  loop.join();

  EXPECT_TRUE(delivered) << "the round after reset must process work, not replay the erased quit";
  deleteUserEvent(event);
}

// Full round-trip: quit stops round one, reset rearms the base, round two is
// fully operational (timers fire, quit stops it again).
TEST(loop_lifecycle, reset_restores_base_after_quit)
{
  asyncBase *base = createAsyncBase(amOSDefault, 1);
  ASSERT_NE(base, nullptr);

  postQuitOperation(base);
  asyncLoop(base);
  resetQuitOperation(base);

  FireProbe probe;
  probe.base = base;
  aioUserEvent *event = newUserEvent(base, 0, fireProbeCallback, &probe);
  ASSERT_NE(event, nullptr);
  userEventStartTimer(event, 50000, 1);

  std::thread loop([base]() { asyncLoop(base); });
  bool delivered = waitForFire(probe, 1, 2000ms);
  if (!delivered)
    postQuitOperation(base);
  loop.join();

  EXPECT_TRUE(delivered) << "round two after quit+reset must be fully operational";
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.fired, 1u);
  }
  deleteUserEvent(event);
}

// Quit stops processing but does not cancel work: a timer armed in round one
// survives the quit/reset pause and fires in round two, exactly once.
TEST(loop_lifecycle, timer_survives_quit_reset_cycle)
{
  asyncBase *base = createAsyncBase(amOSDefault, 1);
  ASSERT_NE(base, nullptr);

  FireProbe probe;
  probe.base = base;
  aioUserEvent *event = newUserEvent(base, 0, fireProbeCallback, &probe);
  ASSERT_NE(event, nullptr);
  userEventStartTimer(event, 400000, 1);

  std::thread roundOne([base]() { asyncLoop(base); });
  std::this_thread::sleep_for(50ms);
  postQuitOperation(base);
  roundOne.join();

  resetQuitOperation(base);

  std::thread roundTwo([base]() { asyncLoop(base); });
  bool delivered = waitForFire(probe, 1, 3000ms);
  if (!delivered)
    postQuitOperation(base);
  roundTwo.join();

  EXPECT_TRUE(delivered) << "a timer armed before the pause must fire after the restart";
  {
    std::lock_guard<std::mutex> lock(probe.mutex);
    EXPECT_EQ(probe.fired, 1u);
  }
  deleteUserEvent(event);
}
