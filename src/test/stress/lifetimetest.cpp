// Object lifetime stress. Worker threads create aio objects, park
// operations on them, feed them datagrams and destroy them while several
// loop threads race to deliver completions, timeouts and readiness events -
// the concurrent counterpart of the deterministic unittest lifetime suite.
// Checked invariants:
//   - every submitted operation reports exactly once (none lost, none twice);
//   - the destructor callback fires exactly once per object and no callback
//     of that object fires after it;
//   - the system drains: a stall means a jammed combiner or a lost wakeup.
// The actual memory errors (touching a destroyed object from a combiner
// chain, cross-object corruption through recycled memory) are the address
// sanitizer's job: configure with -DBUILD_SANITIZE_ADDRESS=ON - objects
// parked in the recycling pools are asan-poisoned, so stale touches report
// use-after-poison while the allocation pattern stays production-like.
//
// Usage: lifetimetest [workers] [loopThreads] [iterations] [opsPerObject] [seed]

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"

#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <random>
#include <thread>
#include <vector>

static asyncBase *gBase = nullptr;

struct ObjectCtx {
  std::atomic<unsigned> expected{0};
  std::atomic<unsigned> callbacks{0};
  std::atomic<unsigned> destructors{0};
  std::atomic<unsigned> afterDestructor{0};
  // Raw handle for the post-mortem dump: a stuck object is alive by
  // definition (references pin it), so peeking at it from the stalled
  // verdict path is safe - everything is quiescent by then
  aioObjectRoot *handle{nullptr};
  char buffer[64];
};

static std::atomic<unsigned> objectsCreated(0);
static std::atomic<unsigned> destructorsFired(0);
static std::atomic<uint64_t> opsSubmitted(0);
static std::atomic<uint64_t> callbacksDelivered(0);
static std::atomic<uint64_t> afterDestructorTotal(0);

static void lifetimeReadCb(AsyncOpStatus, aioObject*, HostAddress, size_t, void *arg)
{
  ObjectCtx *ctx = static_cast<ObjectCtx*>(arg);
  if (ctx->destructors.load(std::memory_order_relaxed)) {
    ctx->afterDestructor.fetch_add(1, std::memory_order_relaxed);
    afterDestructorTotal.fetch_add(1, std::memory_order_relaxed);
  }
  ctx->callbacks.fetch_add(1, std::memory_order_relaxed);
  callbacksDelivered.fetch_add(1, std::memory_order_relaxed);
}

static void lifetimeDestructorCb(aioObjectRoot*, void *arg)
{
  ObjectCtx *ctx = static_cast<ObjectCtx*>(arg);
  ctx->destructors.fetch_add(1, std::memory_order_relaxed);
  destructorsFired.fetch_add(1, std::memory_order_relaxed);
}

static void submitRead(aioObject *object, ObjectCtx *ctx, AsyncFlags flags, uint64_t usTimeout)
{
  ctx->expected.fetch_add(1, std::memory_order_relaxed);
  opsSubmitted.fetch_add(1, std::memory_order_relaxed);
  aioReadMsg(object, ctx->buffer, sizeof(uint32_t), flags, usTimeout, lifetimeReadCb, ctx);
}

static void worker(unsigned id, unsigned iterations, unsigned opsPerObject, unsigned seed, std::deque<ObjectCtx> *arena)
{
  std::minstd_rand rng(seed + id);
  socketTy sender = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);

  for (unsigned i = 0; i < iterations; i++) {
    arena->emplace_back();
    ObjectCtx *ctx = &arena->back();

    HostAddress address;
    address.family = AF_INET;
    address.ipv4 = inet_addr("127.0.0.1");
    address.port = 0;
    socketTy fd = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
    if (socketBind(fd, &address) != 0) {
      fprintf(stderr, "error: socketBind failed (worker %u iteration %u)\n", id, i);
      exit(1);
    }

    sockaddr_in selfAddress;
    memset(&selfAddress, 0, sizeof(selfAddress));
#ifdef OS_WINDOWS
    int selfAddressLength = static_cast<int>(sizeof(selfAddress));
#else
    socklen_t selfAddressLength = sizeof(selfAddress);
#endif
    getsockname(fd, reinterpret_cast<sockaddr*>(&selfAddress), &selfAddressLength);

    aioObject *object = newSocketIo(gBase, fd);
    objectsCreated.fetch_add(1, std::memory_order_relaxed);
    ctx->handle = aioObjectHandle(object);
    objectSetDestructorCb(ctx->handle, lifetimeDestructorCb, ctx);

    for (unsigned k = 0; k < opsPerObject; k++) {
      // A mix of arming modes so the delete races different completion
      // sources: nothing (cancel is the only way out), a precise per-op
      // timer, the second-grid timer
      switch (rng() % 4) {
        case 0:
        case 1:
          submitRead(object, ctx, afNone, 0);
          break;
        case 2:
          submitRead(object, ctx, afRealtime, 50 + rng() % 2000);
          break;
        case 3:
          submitRead(object, ctx, afNone, 300000);
          break;
      }
    }

    // A few datagrams so part of the reads complete successfully and the
    // loop threads keep entering this object's combiner
    unsigned feeds = rng() % 3;
    for (unsigned f = 0; f < feeds; f++) {
      uint32_t payload = i;
      sendto(sender, reinterpret_cast<const char*>(&payload), sizeof(payload), 0,
             reinterpret_cast<sockaddr*>(&selfAddress), sizeof(selfAddress));
    }

    // Jitter widens the interleaving between loop-thread activity and the
    // destruction below
    switch (rng() % 3) {
      case 0:
        break;
      case 1:
        std::this_thread::yield();
        break;
      case 2:
        std::this_thread::sleep_for(std::chrono::microseconds(rng() % 100));
        break;
    }

    if (rng() % 8 == 0) {
      // cancelIo flavor: everything pending dies, the object must stay
      // usable for one more submission before dying for real
      cancelIo(aioObjectHandle(object));
      submitRead(object, ctx, afNone, 0);
    }

    deleteAioObject(object);
  }

  socketClose(sender);
}

int main(int argc, char **argv)
{
  unsigned workers = argc > 1 ? static_cast<unsigned>(atoi(argv[1])) : 4;
  unsigned loopThreads = argc > 2 ? static_cast<unsigned>(atoi(argv[2])) : 3;
  unsigned iterations = argc > 3 ? static_cast<unsigned>(atoi(argv[3])) : 2000;
  unsigned opsPerObject = argc > 4 ? static_cast<unsigned>(atoi(argv[4])) : 8;
  unsigned seed = argc > 5 ? static_cast<unsigned>(atoi(argv[5])) : 20260707;
  if (!workers || !loopThreads || !iterations) {
    fprintf(stderr, "usage: lifetimetest [workers] [loopThreads] [iterations] [opsPerObject] [seed]\n");
    return 1;
  }

  initializeAsyncIo(aiNone);
  gBase = createAsyncBase(amOSDefault);

  std::vector<std::thread> loops;
  for (unsigned i = 0; i < loopThreads; i++)
    loops.emplace_back([]() { asyncLoop(gBase); });

  auto startedAt = std::chrono::steady_clock::now();
  std::vector<std::deque<ObjectCtx>> arenas(workers);
  std::vector<std::thread> workerThreads;
  for (unsigned i = 0; i < workers; i++)
    workerThreads.emplace_back(worker, i, iterations, opsPerObject, seed, &arenas[i]);
  for (auto &thread : workerThreads)
    thread.join();

  // Drain: every operation must report and every object must destruct on
  // its own; no progress for 10 seconds is a verdict, not a timeout
  uint64_t expectedOps = opsSubmitted.load();
  unsigned expectedObjects = objectsCreated.load();
  auto lastProgressAt = std::chrono::steady_clock::now();
  uint64_t lastProgress = ~static_cast<uint64_t>(0);
  for (;;) {
    uint64_t delivered = callbacksDelivered.load();
    unsigned destroyed = destructorsFired.load();
    if (destroyed == expectedObjects && delivered >= expectedOps)
      break;

    uint64_t progress = delivered + destroyed;
    auto now = std::chrono::steady_clock::now();
    if (progress != lastProgress) {
      lastProgress = progress;
      lastProgressAt = now;
    } else if (now - lastProgressAt > std::chrono::seconds(10)) {
      fprintf(stderr, "STALL: %u/%u objects destroyed, %" PRIu64 "/%" PRIu64 " callbacks delivered\n",
              destroyed, expectedObjects, delivered, expectedOps);
      unsigned stuckWithCallbacksMissing = 0, stuckCallbacksComplete = 0, destroyedCallbacksMissing = 0;
      for (auto &arena : arenas) {
        for (auto &ctx : arena) {
          bool complete = ctx.callbacks.load() == ctx.expected.load();
          if (!ctx.destructors.load())
            complete ? stuckCallbacksComplete++ : stuckWithCallbacksMissing++;
          else if (!complete)
            destroyedCallbacksMissing++;
        }
      }
      fprintf(stderr, "  stuck objects: %u with missing callbacks (stranded operations hold references), "
                      "%u with all callbacks (lost delete), %u destroyed but callbacks missing\n",
              stuckWithCallbacksMissing, stuckCallbacksComplete, destroyedCallbacksMissing);
      unsigned dumped = 0;
      for (auto &arena : arenas) {
        for (auto &ctx : arena) {
          if (ctx.destructors.load() || ctx.callbacks.load() == ctx.expected.load() || !ctx.handle)
            continue;
          aioObjectRoot *o = ctx.handle;
          fprintf(stderr,
                  "  obj %p: refs=%" PRIuPTR " head=%" PRIxPTR " readQ=%p writeQ=%p"
                  " cancelIoFlag=%u deletePending=%u initialization=%" PRIxPTR " missing=%u\n",
                  (void*)o, o->refs, o->Head.data, (void*)o->readQueue.head, (void*)o->writeQueue.head,
                  o->CancelIoFlag, o->DeletePending, o->initializationOp,
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

  postQuitOperation(gBase);
  for (auto &thread : loops)
    thread.join();

  uint64_t exactlyOnceViolations = 0;
  for (auto &arena : arenas) {
    for (auto &ctx : arena) {
      if (ctx.callbacks.load() != ctx.expected.load() || ctx.destructors.load() != 1)
        exactlyOnceViolations++;
    }
  }

  double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - startedAt).count();
  printf("objects %u, ops %" PRIu64 ", callbacks %" PRIu64 ", after-destructor %" PRIu64
         ", exactly-once violations %" PRIu64 ", %.1fs\n",
         expectedObjects, expectedOps, callbacksDelivered.load(),
         afterDestructorTotal.load(), exactlyOnceViolations, elapsed);
  if (afterDestructorTotal.load() || exactlyOnceViolations) {
    fprintf(stderr, "FAILED\n");
    return 1;
  }

  printf("OK\n");
  return 0;
}
