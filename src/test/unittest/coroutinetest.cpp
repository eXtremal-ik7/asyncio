#include "unittest.h"

#include "asyncio/coroutine.h"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <thread>

void coroutine_create_proc(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
}

TEST(coroutine, create)
{
  int x = 0;
  coroutineTy *coro = coroutineNew(coroutine_create_proc, &x, 0x10000);
  while (!coroutineCall(coro))
    continue;
  ASSERT_EQ(x, 1);
}

void coroutine_yield_proc(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
  coroutineYield();
  (*x)++;
}

TEST(coroutine, yield)
{
  int x = 0;
  coroutineTy *coro = coroutineNew(coroutine_yield_proc, &x, 0x10000);
  while (!coroutineCall(coro))
    continue;
  ASSERT_EQ(x, 2);
}

void coroutine_nested_proc2(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
}

void coroutine_nested_proc1(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
  coroutineYield();
  coroutineTy *coro = coroutineNew(coroutine_nested_proc2, x, 0x10000);
  coroutineCall(coro);
  coroutineYield();
  (*x)++;
}

TEST(coroutine, nested)
{
  int x = 0;
  coroutineTy *coro = coroutineNew(coroutine_nested_proc1, &x, 0x10000);
  while (!coroutineCall(coro))
    continue;
  ASSERT_EQ(x, 3);
}

// coroutineCall on a running coroutine does not switch: it records the wakeup
// in the counter and returns, and the owner consumes the record at the next
// coroutineYield (which then resumes immediately instead of parking). When the
// wakeup lands after the final yield, the coroutine finishes with the record
// still pending, and the caller's re-entry loop (coroutinePosix.c do/while in
// coroutineCall, same shape in coroutineWin32.c) trusts the counter alone: it
// switches back into the finished context, where execution falls off the end
// of fiberEntryPoint - a garbage return address on POSIX, an implicit thread
// exit with Windows fibers. In production the window opens with several loop
// threads: one resumes an operation-owning coroutine that is about to return
// while another delivers a second wakeup for it (a user event, another
// operation on the same second). The handshake below forces that exact
// interleaving on every run, no timing involved. The scenario runs in a death
// test child: today it must die there; with the re-entry loop respecting
// `finished` it exits through the normal path - coroutineCall reports
// completion and the finish callback fires exactly once.
static coroutineTy *coroWakeupRaceCoroutine;
static std::atomic<int> coroWakeupRacePhase(0);
static int coroWakeupRaceFinishCalls = 0;

static void coroWakeupRaceFinishCb(void*)
{
  coroWakeupRaceFinishCalls++;
}

static void coroWakeupRaceProc(void*)
{
  coroWakeupRacePhase.store(1);
  while (coroWakeupRacePhase.load() != 2)
    std::this_thread::yield();
  // return without yielding: the recorded wakeup is still in the counter
}

static void coroWakeupRaceChild()
{
  // On Windows the broken path silently exits the calling thread instead of
  // crashing, which would leave the child hanging; turn a hang into a verdict
  std::thread watchdog([]() {
    std::this_thread::sleep_for(std::chrono::seconds(10));
    std::_Exit(3);
  });
  watchdog.detach();

  coroWakeupRaceCoroutine = coroutineNewWithCb(coroWakeupRaceProc, nullptr, 0x10000, coroWakeupRaceFinishCb, nullptr);
  std::thread wakeup([]() {
    while (coroWakeupRacePhase.load() != 1)
      std::this_thread::yield();
    coroutineCall(coroWakeupRaceCoroutine); // the coroutine is running: recorded, not switched
    coroWakeupRacePhase.store(2);
  });

  int finished = coroutineCall(coroWakeupRaceCoroutine);
  wakeup.join();
  // _Exit keeps the verdict to the scenario itself (no atexit, no leak check)
  std::_Exit(finished == 1 && coroWakeupRaceFinishCalls == 1 ? 42 : 1);
}

TEST(coroutine, wakeup_racing_finish)
{
  testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(coroWakeupRaceChild(), testing::ExitedWithCode(42), "");
}
