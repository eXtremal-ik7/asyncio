#pragma once

// Shared drain / verdict tooling of the lifetime-style stress binaries. Each
// binary fills per-worker arenas of contexts carrying the exactly-once
// counters (expected/callbacks/destructors) and a raw object handle, then:
//  - drainOrDie() waits until every submission reported and every object
//    destructed; 10 seconds without progress is a verdict, not a timeout - it
//    dumps up to 16 stuck objects (a stuck object is alive by definition,
//    stranded operations pin it, so peeking is safe - everything is quiescent
//    by then) and exits hard;
//  - countExactlyOnceViolations() recounts the per-context invariant once the
//    loop threads were joined.

#include "asyncio/asyncio.h"

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <deque>
#include <thread>
#include <vector>

template<typename Ctx>
static void drainOrDie(std::vector<std::deque<Ctx>> &arenas,
                       uint64_t expectedOps, unsigned expectedObjects,
                       std::atomic<uint64_t> &callbacksDelivered,
                       std::atomic<unsigned> &destructorsFired,
                       const char *objectsNoun, const char *dumpTag,
                       void (*extraStallSummary)(const std::vector<std::deque<Ctx>>&) = nullptr)
{
  auto lastProgressAt = std::chrono::steady_clock::now();
  uint64_t lastProgress = ~static_cast<uint64_t>(0);
  for (;;) {
    uint64_t delivered = callbacksDelivered.load();
    unsigned destroyed = destructorsFired.load();
    if (destroyed == expectedObjects && delivered >= expectedOps)
      return;

    uint64_t progress = delivered + destroyed;
    auto now = std::chrono::steady_clock::now();
    if (progress != lastProgress) {
      lastProgress = progress;
      lastProgressAt = now;
    } else if (now - lastProgressAt > std::chrono::seconds(10)) {
      fprintf(stderr, "STALL: %u/%u %s destroyed, %" PRIu64 "/%" PRIu64 " callbacks delivered\n",
              destroyed, expectedObjects, objectsNoun, delivered, expectedOps);
      if (extraStallSummary)
        extraStallSummary(arenas);
      unsigned dumped = 0;
      for (auto &arena : arenas) {
        for (auto &ctx : arena) {
          if (ctx.destructors.load() || ctx.callbacks.load() == ctx.expected.load() || !ctx.handle)
            continue;
          aioObjectRoot *o = ctx.handle;
          fprintf(stderr,
                  "  %s %p: refs=%" PRIuPTR " head=%" PRIx64 " readQ=%p writeQ=%p"
                  " deletePending=%u initialization=%" PRIxPTR " missing=%u\n",
                  dumpTag,
                  (void*)o,
                  o->refs,
                  __uint64_atomic_load(&o->header.tag.low, amoRelaxed),
                  (void*)o->readQueue.head,
                  (void*)o->writeQueue.head,
                  o->DeletePending, o->initializationOp,
                  ctx.expected.load() - ctx.callbacks.load());
          if (++dumped == 16)
            break;
        }
        if (dumped == 16)
          break;
      }
      fflush(nullptr);
      std::_Exit(2); // loop threads may be jammed, a clean join is not coming
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

template<typename Ctx>
static uint64_t countExactlyOnceViolations(const std::vector<std::deque<Ctx>> &arenas)
{
  uint64_t violations = 0;
  for (auto &arena : arenas) {
    for (auto &ctx : arena) {
      if (ctx.callbacks.load() != ctx.expected.load() || ctx.destructors.load() != 1)
        violations++;
    }
  }
  return violations;
}
