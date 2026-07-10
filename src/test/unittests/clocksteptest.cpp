// Clock-step regression test: timeouts and realtime timers must key off a
// monotonic clock, not wall-clock time(0). A backward wall-clock step (e.g. an
// NTP correction on a server whose clock ran fast) must not freeze the timeout
// grid; a forward step must not fire timeouts prematurely.
//
// This test MUTATES THE SYSTEM CLOCK, so it is gated twice:
//   1. the whole suite is named DISABLED_clock_step and requires the explicit
//      gtest flag --gtest_also_run_disabled_tests,
//   2. it is skipped unless the process can actually set the clock (root / admin).
// The clock is restored on every exit path: normal return, watchdog, fatal
// signal and atexit. Elapsed time is measured with a monotonic steady_clock,
// which our stepping does not perturb.
//
// Coverage: the grid path (afNone I/O timeout, endTime = time(0) + timeout in
// asyncioImpl.c) and the realtime-timer path (userEventStartTimer -> startTimer,
// timerfd_create(CLOCK_REALTIME) on epoll).
//
// Expected verdicts:
//   grid_* (all backends) and realtime_* on epoll (Linux): RED before the
//   CLOCK_MONOTONIC fix, GREEN after. realtime_* on kqueue (macOS/BSD) is GREEN
//   even before the fix, because EVFILT_TIMER already runs off a monotonic base.

#include "unittest.h"

#include <asyncio/socket.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>

#if defined(OS_WINDOWS)
// windows.h is already pulled in by asyncioTypes.h
#else
#include <sys/time.h>
#include <unistd.h>
#endif

// ---- platform wall-clock get/set (microseconds since the Unix epoch, UTC) ----

static int64_t wallClockUs()
{
#if defined(OS_WINDOWS)
  FILETIME ft;
  GetSystemTimeAsFileTime(&ft);
  ULARGE_INTEGER u;
  u.LowPart = ft.dwLowDateTime;
  u.HighPart = ft.dwHighDateTime;
  const int64_t epochDiff100ns = 116444736000000000LL; // 1601-01-01 -> 1970-01-01
  return (int64_t)((u.QuadPart - epochDiff100ns) / 10);
#else
  struct timeval tv;
  gettimeofday(&tv, nullptr);
  return (int64_t)tv.tv_sec * 1000000 + tv.tv_usec;
#endif
}

static int wallClockSetUs(int64_t us)
{
#if defined(OS_WINDOWS)
  ULARGE_INTEGER u;
  u.QuadPart = (uint64_t)(us * 10 + 116444736000000000LL);
  FILETIME ft;
  ft.dwLowDateTime = u.LowPart;
  ft.dwHighDateTime = u.HighPart;
  SYSTEMTIME st;
  if (!FileTimeToSystemTime(&ft, &st))
    return -1;
  return SetSystemTime(&st) ? 0 : -1; // SetSystemTime takes UTC
#else
  struct timeval tv;
  tv.tv_sec = (time_t)(us / 1000000);
  tv.tv_usec = (int)(us % 1000000);
  return settimeofday(&tv, nullptr);
#endif
}

// ---- privilege check (also enables SE_SYSTEMTIME on Windows) ----

#if defined(OS_WINDOWS)
static bool haveClockPrivilege()
{
  HANDLE token;
  if (!OpenProcessToken(GetCurrentProcess(), TOKEN_ADJUST_PRIVILEGES | TOKEN_QUERY, &token))
    return false;
  LUID luid;
  bool ok = false;
  if (LookupPrivilegeValue(nullptr, SE_SYSTEMTIME_NAME, &luid)) {
    TOKEN_PRIVILEGES tp;
    tp.PrivilegeCount = 1;
    tp.Privileges[0].Luid = luid;
    tp.Privileges[0].Attributes = SE_PRIVILEGE_ENABLED;
    ok = AdjustTokenPrivileges(token, FALSE, &tp, sizeof(tp), nullptr, nullptr) &&
         GetLastError() == ERROR_SUCCESS;
  }
  CloseHandle(token);
  return ok;
}
#else
static bool haveClockPrivilege() { return geteuid() == 0; }
#endif

// ---- crash-safe clock guard ------------------------------------------------
// Restore reconstructs correct wall-clock time from a monotonic baseline, so any
// restore path (watchdog / signal / atexit) lands on real time regardless of how
// the clock was stepped in between.

static std::atomic<bool> gClockStepped{false};
static int64_t gRealBaselineUs = 0;
static std::chrono::steady_clock::time_point gBaselineMono;

static void clockArmGuard()
{
  gRealBaselineUs = wallClockUs();
  gBaselineMono = std::chrono::steady_clock::now();
  gClockStepped.store(true);
}

static void clockRestore()
{
  if (gClockStepped.exchange(false)) {
    int64_t elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - gBaselineMono).count();
    wallClockSetUs(gRealBaselineUs + elapsed);
  }
}

static void clockStepSignalHandler(int sig)
{
  clockRestore();
  std::signal(sig, SIG_DFL);
  std::raise(sig);
}

// Force-restores the clock and aborts if a run leaves it stepped past the cap.
class ClockWatchdog {
public:
  explicit ClockWatchdog(int capSeconds)
  {
    thread_ = std::thread([this, capSeconds]() {
      for (int i = 0; i < capSeconds * 10 && !done_.load(); i++)
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      if (!done_.load() && gClockStepped.load()) {
        clockRestore();
        std::fprintf(stderr, "clockstep watchdog: forced clock restore + abort\n");
        std::abort();
      }
    });
  }
  ~ClockWatchdog()
  {
    done_.store(true);
    if (thread_.joinable())
      thread_.join();
  }
private:
  std::atomic<bool> done_{false};
  std::thread thread_;
};

// Stops the message loop after a cap so a misbehaving timer can never hang the
// process while the clock is stepped. If the timer never fires the loop still
// returns and the test reports a clean failure instead of tripping the watchdog.
class LoopGuard {
public:
  LoopGuard(asyncBase *base, int capMs) : base_(base)
  {
    thread_ = std::thread([this, capMs]() {
      for (int i = 0; i < capMs / 50 && !done_.load(); i++)
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
      if (!done_.load())
        postQuitOperation(base_);
    });
  }
  ~LoopGuard()
  {
    done_.store(true);
    if (thread_.joinable())
      thread_.join();
  }
private:
  asyncBase *base_;
  std::atomic<bool> done_{false};
  std::thread thread_;
};

// ---- timer probes ----------------------------------------------------------

struct TimerProbe {
  asyncBase *base;
  std::chrono::steady_clock::time_point armMono;
  std::atomic<int64_t> elapsedUs{-1};
};

static int64_t monoElapsedUs(const TimerProbe *p)
{
  return std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::steady_clock::now() - p->armMono).count();
}

static void gridTimeoutCb(AsyncOpStatus status, aioObject*, HostAddress, size_t, void *arg)
{
  TimerProbe *p = static_cast<TimerProbe*>(arg);
  p->elapsedUs.store(monoElapsedUs(p));
  (void)status; // expected aosTimeout; the firing time is what we assert
  postQuitOperation(p->base);
}

static void realtimeTimerCb(aioUserEvent*, void *arg)
{
  TimerProbe *p = static_cast<TimerProbe*>(arg);
  p->elapsedUs.store(monoElapsedUs(p));
  postQuitOperation(p->base);
}

static aioObject *makeSilentUDP(asyncBase *base)
{
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy s = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  if (socketBind(s, &address) != 0) {
    socketClose(s);
    return nullptr;
  }
  return newSocketIo(base, s);
}

// ---- measurement -----------------------------------------------------------

enum class TimerKind { Grid, Realtime };

struct StepResult {
  int64_t elapsedUs;
  bool stepApplied;
};

// The grid keys off time(0), i.e. whole-second resolution, so the timer and the
// step are sized to dwarf that 1 s quantization and keep the pass band robust.
static constexpr int64_t kTimeoutUs   = 2000000; // 2 s timer under test
static constexpr int64_t kStepUs      = 4000000; // 4 s wall-clock step
static constexpr int     kLoopGuardMs = 10000;   // hard cap on a single loop run

// Correct behavior: the timer fires ~2 s after arming regardless of the step. A
// forward step that fires it early lands below the low bound; a backward step
// that freezes the grid (or a timer that never fires) lands above the high one.
// Bounds leave room for the grid's 1 s quantization on the correct-fire side.
static constexpr int64_t kBandLowMs  = 1500; // catches premature fire (< 2 s timeout)
static constexpr int64_t kBandHighMs = 4500; // catches a freeze (> 2 s + quantization)

// One shared base for every test, created by unittest's main(). asyncio has no
// deleteAsyncBase and its op pools (opPool/opTimerPool/eventPool) are process
// globals, so spinning up a fresh base per test leaks cross-test state that can
// stall a later run — unittest shares a single base for exactly this reason.

// Arms a timer for kTimeoutUs, steps the wall clock by stepUs (signed) while the
// timer is pending, runs the loop until it fires, restores the clock, and
// returns the monotonically-measured firing delay.
static StepResult measureUnderStep(TimerKind kind, int64_t stepUs)
{
  StepResult result{-1, false};
  asyncBase *base = gBase;
  TimerProbe probe;
  probe.base = base;

  aioObject *udp = nullptr;
  aioUserEvent *ev = nullptr;
  uint8_t buffer[64];

  probe.armMono = std::chrono::steady_clock::now();
  if (kind == TimerKind::Grid) {
    udp = makeSilentUDP(base);
    if (!udp)
      return result;
    // afNone -> the op lands in the wall-clock timeout grid, not on a timerfd.
    aioReadMsg(udp, buffer, sizeof(buffer), afNone, (uint64_t)kTimeoutUs, gridTimeoutCb, &probe);
  } else {
    // userEventStartTimer arms the realtime timer synchronously (timerfd on epoll).
    ev = newUserEvent(base, 0, realtimeTimerCb, &probe);
    userEventStartTimer(ev, (uint64_t)kTimeoutUs, 1);
  }

  {
    ClockWatchdog watchdog(18);
    clockArmGuard();
    if (wallClockSetUs(gRealBaselineUs + stepUs) == 0) {
      // Confirm the step actually landed (a sandbox may refuse it even as root).
      int64_t target = gRealBaselineUs + stepUs;
      int64_t d = wallClockUs() - target;
      if (d < 0)
        d = -d;
      result.stepApplied = d < 2000000;
    }
    if (result.stepApplied) {
      LoopGuard guard(base, kLoopGuardMs);
      asyncLoop(base);
    }
    clockRestore();
  }

  if (result.stepApplied) {
    int64_t e = probe.elapsedUs.load();
    // -1 means the guard had to stop the loop: the timer never fired within the
    // cap. Report it as a large elapsed so the high-bound assertion fails cleanly
    // instead of the run hanging.
    result.elapsedUs = (e < 0) ? (int64_t)kLoopGuardMs * 1000 : e;
  }

  if (udp)
    deleteAioObject(udp);
  if (ev)
    deleteUserEvent(ev);
  return result;
}

// ---- gate + tests ----------------------------------------------------------

static void installClockRestoreGuards()
{
  static const bool installed = []() {
    std::atexit(clockRestore);
    std::signal(SIGINT,  clockStepSignalHandler);
    std::signal(SIGTERM, clockStepSignalHandler);
    std::signal(SIGSEGV, clockStepSignalHandler);
    std::signal(SIGABRT, clockStepSignalHandler);
    std::signal(SIGFPE,  clockStepSignalHandler);
    std::signal(SIGILL,  clockStepSignalHandler);
#if defined(SIGBUS)
    std::signal(SIGBUS, clockStepSignalHandler);
#endif
    return true;
  }();
  (void)installed;
}

static void runClockStepTest(TimerKind kind, int64_t stepUs)
{
  if (!haveClockPrivilege())
    GTEST_SKIP() << "insufficient privileges: run as root / elevated administrator to set the system clock";

  installClockRestoreGuards();

  StepResult r = measureUnderStep(kind, stepUs);
  if (!r.stepApplied)
    GTEST_SKIP() << "system clock could not be stepped (sandbox / seccomp?)";

  int64_t elapsedMs = r.elapsedUs / 1000;
  EXPECT_GE(elapsedMs, kBandLowMs)
      << "timer fired too early under a wall-clock step (premature timeout); elapsed=" << elapsedMs << "ms";
  EXPECT_LE(elapsedMs, kBandHighMs)
      << "timer froze or never fired under a wall-clock step (queue stalled); elapsed=" << elapsedMs << "ms";
}

TEST(DISABLED_clock_step, grid_backward)     { runClockStepTest(TimerKind::Grid,     -kStepUs); }
TEST(DISABLED_clock_step, grid_forward)      { runClockStepTest(TimerKind::Grid,      kStepUs); }
TEST(DISABLED_clock_step, realtime_backward) { runClockStepTest(TimerKind::Realtime, -kStepUs); }
TEST(DISABLED_clock_step, realtime_forward)  { runClockStepTest(TimerKind::Realtime,  kStepUs); }
