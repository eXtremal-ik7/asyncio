// User-event timer backend stress. This is intentionally standalone (no
// gtest), so the same finite-count/rearm protocol runs on Windows builds too.
// Each round forces thousands of one-shot backend rearms and verifies exact
// semaphore delivery plus exactly-once destruction before the slot is reused.
//
// Usage: eventstress [rounds] [ticksPerRound] [periodUs] [loopThreads]

#include "asyncio/asyncio.h"

#include <atomic>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using Clock = std::chrono::steady_clock;

struct EventCtx {
  std::atomic<unsigned> callbacks{0};
  std::atomic<unsigned> destructors{0};
  std::atomic<unsigned> afterDestructor{0};
};

static void eventCb(aioUserEvent*, void *arg)
{
  EventCtx *ctx = static_cast<EventCtx*>(arg);
  if (ctx->destructors.load(std::memory_order_acquire))
    ctx->afterDestructor.fetch_add(1, std::memory_order_relaxed);
  ctx->callbacks.fetch_add(1, std::memory_order_release);
}

static void eventDestructor(aioUserEvent*, void *arg)
{
  static_cast<EventCtx*>(arg)->destructors.fetch_add(1, std::memory_order_release);
}

static bool waitFor(const std::atomic<unsigned> &value,
                    unsigned expected,
                    Clock::duration timeout)
{
  Clock::time_point deadline = Clock::now() + timeout;
  while (value.load(std::memory_order_acquire) < expected) {
    if (Clock::now() >= deadline)
      return false;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return true;
}

int main(int argc, char **argv)
{
  unsigned rounds = argc > 1 ? static_cast<unsigned>(atoi(argv[1])) : 8;
  unsigned ticks = argc > 2 ? static_cast<unsigned>(atoi(argv[2])) : 1000;
  uint64_t periodUs = argc > 3 ? static_cast<uint64_t>(strtoull(argv[3], nullptr, 10)) : 100;
  unsigned loopThreads = argc > 4 ? static_cast<unsigned>(atoi(argv[4])) : 2;
  if (!rounds || !ticks || ticks > static_cast<unsigned>(INT_MAX) ||
      !periodUs || periodUs > static_cast<uint64_t>(INT64_MAX) / 10 ||
      !loopThreads) {
    fprintf(stderr, "usage: eventstress [rounds] [ticksPerRound] [periodUs] [loopThreads]\n");
    return 1;
  }

  initializeAsyncIo(aiNone);
  asyncBase *base = createAsyncBase(amOSDefault, loopThreads);
  if (!base) {
    fprintf(stderr, "createAsyncBase failed\n");
    return 1;
  }

  std::vector<std::thread> loops;
  loops.reserve(loopThreads);
  for (unsigned i = 0; i < loopThreads; i++)
    loops.emplace_back([base]() { asyncLoop(base); });

  uint64_t totalCallbacks = 0;
  Clock::time_point started = Clock::now();
  for (unsigned round = 0; round < rounds; round++) {
    EventCtx ctx;
    aioUserEvent *event = newUserEvent(base, 1, eventCb, &ctx);
    if (!event) {
      fprintf(stderr, "newUserEvent failed at round %u\n", round);
      fflush(nullptr);
      std::_Exit(2);
    }
    eventSetDestructorCb(event, eventDestructor, &ctx);
    userEventStartTimer(event, periodUs, static_cast<int>(ticks));

    if (!waitFor(ctx.callbacks, ticks, std::chrono::seconds(30))) {
      fprintf(stderr,
              "STALL round %u: %u/%u callbacks, %u destructor(s)\n",
              round,
              ctx.callbacks.load(),
              ticks,
              ctx.destructors.load());
      fflush(nullptr);
      std::_Exit(2);
    }

    deleteUserEvent(event);
    if (!waitFor(ctx.destructors, 1, std::chrono::seconds(10))) {
      fprintf(stderr, "STALL round %u: destructor did not run\n", round);
      fflush(nullptr);
      std::_Exit(2);
    }
    unsigned delivered = ctx.callbacks.load(std::memory_order_acquire);
    unsigned destructors = ctx.destructors.load(std::memory_order_acquire);
    unsigned late = ctx.afterDestructor.load(std::memory_order_acquire);
    if (delivered != ticks || destructors != 1 || late != 0) {
      fprintf(stderr,
              "VIOLATION round %u: callbacks %u/%u, destructors %u, after-destructor %u\n",
              round,
              delivered,
              ticks,
              destructors,
              late);
      fflush(nullptr);
      std::_Exit(3);
    }
    totalCallbacks += delivered;
  }

  // One quit token is handed from each exiting loop thread to the next.
  postQuitOperation(base);
  for (std::thread &loop : loops)
    loop.join();

  double seconds = std::chrono::duration<double>(Clock::now() - started).count();
  printf("eventstress: %u rounds, %u ticks/round, %llu callbacks, %.3fs\nOK\n",
         rounds,
         ticks,
         static_cast<unsigned long long>(totalCallbacks),
         seconds);
  return 0;
}
