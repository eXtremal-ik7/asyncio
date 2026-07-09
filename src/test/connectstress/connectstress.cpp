// Composite-connect lifetime stress. Worker threads spin up zmtp sockets over
// loopback TCP and drive aioZmtpConnect with short, randomized timeouts while
// several loop threads deliver the handshake child completions and the
// timeouts. This targets the resumeParent() branch of the *shared* combiner:
// a parent connect operation (the zmtp handshake state machine) carries a
// timeout and stays pending across several child round-trips; every child
// completion pushes combinerPushOperation(aaContinue) on the parent from one
// thread, while the parent's own timeout pushes aaCancel from another. Both
// target the same operation node (op->next) - the Treiber-stack double-push.
//
// zmtp is only the vehicle: no libzmq, no TLS certificates. The raced code is
// backend-agnostic and shared by ssl/http/smtp/btc/rlpx connect as well, so a
// report here is a report for all of them, on reactors and the proactor alike.
//
// Checked invariants (mirroring the udp lifetime suite):
//   - every aioZmtpConnect reports its callback exactly once (none lost - a
//     self-looped stack swallows the op; none twice - a doubly-processed op);
//   - the destructor callback fires exactly once per socket and no connect
//     callback of that socket fires after it;
//   - the system drains: no progress for 10 seconds is a jammed combiner.
// The memory errors themselves (touching a recycled op/object through a
// corrupted combiner chain) are the address sanitizer's job: build with
// -DBUILD_SANITIZE_ADDRESS=ON, the recycling pools are asan-poisoned.
//
// Usage: connectstress [workers] [loopThreads] [iterations] [seed]

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"
#include "asyncioextras/zmtp.h"

#ifdef OS_WINDOWS
#include <winsock2.h>
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <csignal>
#endif

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

#ifdef OS_WINDOWS
static const socketTy kBadSocket = INVALID_SOCKET;
#else
static const socketTy kBadSocket = static_cast<socketTy>(-1);
#endif

static asyncBase *gBase = nullptr;
static socketTy gListener = kBadSocket;
static std::atomic<bool> gStop(false);
static std::atomic<uint64_t> gAcceptSeq(0);

struct ConnCtx {
  std::atomic<unsigned> expected{0};
  std::atomic<unsigned> callbacks{0};
  std::atomic<unsigned> destructors{0};
  std::atomic<unsigned> afterDestructor{0};
  // Raw handle for the post-mortem dump: a stuck socket is alive by
  // definition (a stranded operation pins it), so peeking from the stalled
  // verdict path is safe - everything is quiescent by then.
  aioObjectRoot *handle{nullptr};
};

static std::atomic<unsigned> objectsCreated(0);
static std::atomic<unsigned> destructorsFired(0);
static std::atomic<uint64_t> opsSubmitted(0);
static std::atomic<uint64_t> callbacksDelivered(0);
static std::atomic<uint64_t> afterDestructorTotal(0);
static std::atomic<uint64_t> stSuccess(0), stTimeout(0), stCanceled(0), stDisconnected(0), stOther(0);

static void connectCb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  ConnCtx *ctx = static_cast<ConnCtx*>(arg);
  if (ctx->destructors.load(std::memory_order_relaxed)) {
    ctx->afterDestructor.fetch_add(1, std::memory_order_relaxed);
    afterDestructorTotal.fetch_add(1, std::memory_order_relaxed);
  }
  ctx->callbacks.fetch_add(1, std::memory_order_relaxed);
  callbacksDelivered.fetch_add(1, std::memory_order_relaxed);

  switch (status) {
    case aosSuccess:      stSuccess.fetch_add(1, std::memory_order_relaxed); break;
    case aosTimeout:      stTimeout.fetch_add(1, std::memory_order_relaxed); break;
    case aosCanceled:     stCanceled.fetch_add(1, std::memory_order_relaxed); break;
    case aosDisconnected: stDisconnected.fetch_add(1, std::memory_order_relaxed); break;
    default:              stOther.fetch_add(1, std::memory_order_relaxed); break;
  }
}

static void destructorCb(aioObjectRoot*, void *arg)
{
  ConnCtx *ctx = static_cast<ConnCtx*>(arg);
  ctx->destructors.fetch_add(1, std::memory_order_relaxed);
  destructorsFired.fetch_add(1, std::memory_order_relaxed);
}

// Loopback zmtp peer. The zmtp greeting is symmetric, so a paced echo drives
// the real handshake to completion: read the client's stage, wait, echo it
// back and its matching read child completes. The per-round delay spreads those
// child completions - each an aaContinue push on the parent - across the
// parent's timeout window, so the timer's aaCancel and a child's aaContinue
// race on the same parent op. The state machine does not validate content, so
// echoing the client's own bytes satisfies each read. A quarter of the
// connections die after two rounds so disconnect children (aaFinish) race too.
static void responder()
{
  while (!gStop.load(std::memory_order_relaxed)) {
    struct sockaddr_in cli;
#ifdef OS_WINDOWS
    int len = static_cast<int>(sizeof(cli));
#else
    socklen_t len = sizeof(cli);
#endif
    socketTy c = accept(gListener, reinterpret_cast<struct sockaddr*>(&cli), &len);
    if (c == kBadSocket) {
      if (gStop.load(std::memory_order_relaxed))
        break;
      continue;
    }

    uint64_t seq = gAcceptSeq.fetch_add(1, std::memory_order_relaxed);
    unsigned delayUs = 40 + static_cast<unsigned>(seq % 8) * 90; // 40..670us per round
    unsigned maxRounds = (seq & 3) == 3 ? 2 : 12;                // 1/4 die early
    char buf[512];
    for (unsigned r = 0; r < maxRounds && !gStop.load(std::memory_order_relaxed); r++) {
      int n = static_cast<int>(recv(c, buf, sizeof(buf), 0));
      if (n <= 0)
        break;
      std::this_thread::sleep_for(std::chrono::microseconds(delayUs));
      if (send(c, buf, n, 0) <= 0)
        break;
    }

    socketClose(c);
  }
}

static void worker(unsigned id, unsigned iterations, unsigned seed, std::deque<ConnCtx> *arena, const HostAddress *target)
{
  std::minstd_rand rng(seed + id);

  for (unsigned i = 0; i < iterations; i++) {
    arena->emplace_back();
    ConnCtx *ctx = &arena->back();

    socketTy fd = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
    if (fd == kBadSocket) {
      fprintf(stderr, "error: socketCreate failed (worker %u iteration %u)\n", id, i);
      exit(1);
    }

    aioObject *transport = newSocketIo(gBase, fd);
    zmtpSocket *zs = zmtpSocketNew(gBase, transport, zmtpSocketDEALER);
    objectsCreated.fetch_add(1, std::memory_order_relaxed);
    // zmtpSocket embeds aioObjectRoot as its first member; the user destructor
    // hook is separate from zmtp's own object destructor
    ctx->handle = reinterpret_cast<aioObjectRoot*>(zs);
    objectSetDestructorCb(ctx->handle, destructorCb, ctx);

    ctx->expected.fetch_add(1, std::memory_order_relaxed);
    opsSubmitted.fetch_add(1, std::memory_order_relaxed);

    // Arming mix: the short realtime timer is the one that expires mid-handshake
    // and races the child completions' aaContinue push. Timeout 0 is the control
    // path - no parent timer, only the delete race.
    switch (rng() % 4) {
      case 0:
      case 1:
        aioZmtpConnect(zs, target, afRealtime, 20 + rng() % 3000, connectCb, ctx);
        break;
      case 2:
        aioZmtpConnect(zs, target, afRealtime, 20 + rng() % 300, connectCb, ctx);
        break;
      case 3:
        aioZmtpConnect(zs, target, afNone, 0, connectCb, ctx);
        break;
    }

    // Let the handshake actually run so its child completions (each an
    // aaContinue push on the parent) race the parent timeout, then delete. A
    // quarter delete immediately for the cancelIo-vs-resume-vs-timeout race;
    // the rest sleep a spread straddling the handshake, so some deletes land
    // mid-handshake and some after it resolves. The sleep also throttles the
    // connect rate so the paced peer is not overrun into RST.
    if (rng() % 4 != 0)
      std::this_thread::sleep_for(std::chrono::microseconds(rng() % 2500));
    zmtpSocketDelete(zs);
  }
}

int main(int argc, char **argv)
{
  unsigned workers = argc > 1 ? static_cast<unsigned>(atoi(argv[1])) : 4;
  unsigned loopThreads = argc > 2 ? static_cast<unsigned>(atoi(argv[2])) : 4;
  unsigned iterations = argc > 3 ? static_cast<unsigned>(atoi(argv[3])) : 5000;
  unsigned seed = argc > 4 ? static_cast<unsigned>(atoi(argv[4])) : 20260708;
  if (!workers || !loopThreads || !iterations) {
    fprintf(stderr, "usage: connectstress [workers] [loopThreads] [iterations] [seed]\n");
    return 1;
  }

#ifndef OS_WINDOWS
  // The responder writes to sockets the client may have already closed
  signal(SIGPIPE, SIG_IGN);
#endif

  initializeAsyncIo(aiNone);
  gBase = createAsyncBase(amOSDefault);

  // Blocking loopback listener on an ephemeral port
  gListener = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  if (gListener == kBadSocket) {
    fprintf(stderr, "error: listener socketCreate failed\n");
    return 1;
  }
  socketReuseAddr(gListener);

  HostAddress bindAddr;
  bindAddr.family = AF_INET;
  bindAddr.ipv4 = inet_addr("127.0.0.1");
  bindAddr.port = 0;
  if (socketBind(gListener, &bindAddr) != 0) {
    fprintf(stderr, "error: listener bind failed\n");
    return 1;
  }
  socketListen(gListener);

  struct sockaddr_in self;
  memset(&self, 0, sizeof(self));
#ifdef OS_WINDOWS
  int selfLen = static_cast<int>(sizeof(self));
#else
  socklen_t selfLen = sizeof(self);
#endif
  getsockname(gListener, reinterpret_cast<struct sockaddr*>(&self), &selfLen);

  HostAddress target;
  target.family = AF_INET;
  target.ipv4 = inet_addr("127.0.0.1");
  target.port = ntohs(self.sin_port); // HostAddress.port is host byte order

  std::vector<std::thread> responders;
  for (unsigned i = 0; i < 32; i++)
    responders.emplace_back(responder);

  std::vector<std::thread> loops;
  for (unsigned i = 0; i < loopThreads; i++)
    loops.emplace_back([]() { asyncLoop(gBase); });

  auto startedAt = std::chrono::steady_clock::now();
  std::vector<std::deque<ConnCtx>> arenas(workers);
  std::vector<std::thread> workerThreads;
  for (unsigned i = 0; i < workers; i++)
    workerThreads.emplace_back(worker, i, iterations, seed, &arenas[i], &target);
  for (auto &thread : workerThreads)
    thread.join();

  // Drain: every connect must report and every socket must destruct; no
  // progress for 10 seconds is a verdict, not a timeout
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
      fprintf(stderr, "STALL: %u/%u sockets destroyed, %" PRIu64 "/%" PRIu64 " callbacks delivered\n",
              destroyed, expectedObjects, delivered, expectedOps);
      unsigned dumped = 0;
      for (auto &arena : arenas) {
        for (auto &ctx : arena) {
          if (ctx.destructors.load() || ctx.callbacks.load() == ctx.expected.load() || !ctx.handle)
            continue;
          aioObjectRoot *o = ctx.handle;
          fprintf(stderr,
                  "  sock %p: refs=%" PRIuPTR " head=%" PRIxPTR " readQ=%p writeQ=%p"
                  " cancelIoFlag=%u deletePending=%u exclusive=%" PRIxPTR " missing=%u\n",
                  (void*)o, o->refs, o->Head.data, (void*)o->readQueue.head, (void*)o->writeQueue.head,
                  o->CancelIoFlag, o->DeletePending, o->exclusiveOp,
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

  gStop.store(true, std::memory_order_relaxed);
  // Closing the listener wakes a thread blocked in accept() on macOS/BSD but not
  // on Linux, so nudge each responder with a throwaway loopback connection: it
  // returns from accept(), observes gStop and exits. Then close and join.
  for (unsigned i = 0; i < 32; i++) {
    socketTy w = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
    if (w != kBadSocket) {
      connect(w, reinterpret_cast<struct sockaddr*>(&self), selfLen);
      socketClose(w);
    }
  }
  socketClose(gListener);
  for (auto &thread : responders)
    thread.join();

  uint64_t exactlyOnceViolations = 0;
  for (auto &arena : arenas) {
    for (auto &ctx : arena) {
      if (ctx.callbacks.load() != ctx.expected.load() || ctx.destructors.load() != 1)
        exactlyOnceViolations++;
    }
  }

  double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - startedAt).count();
  printf("status: success %" PRIu64 " timeout %" PRIu64 " canceled %" PRIu64
         " disconnected %" PRIu64 " other %" PRIu64 "\n",
         stSuccess.load(), stTimeout.load(), stCanceled.load(), stDisconnected.load(), stOther.load());
  printf("sockets %u, connects %" PRIu64 ", callbacks %" PRIu64 ", after-destructor %" PRIu64
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
