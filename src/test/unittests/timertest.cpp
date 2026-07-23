// White-box tests of the timeout machinery: the grid contract (timer_grid),
// the lock-free cascading wheel protocol (timer_wheel), the sleep handshake
// of the message loops with the grid (timer_wakeup), the kernel-armed
// realtime timers (timer_realtime) and the reactor doorbell protocol of
// epoll/kqueue (timer_reactor). Mocks come from coretest.h.

#include "coretest.h"

#include "reactor.h"
#include "atomic.h"

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"

#include <chrono>
#include <cstring>
#include <thread>

#ifdef __linux__
#include <sys/timerfd.h>
#include <poll.h>
#include <unistd.h>
#include <cerrno>
#endif

namespace {

static_assert(sizeof(aioTimer) <= 2 * CACHE_LINE_SIZE, "POSIX reactor timer must not regain aioObjectRoot-sized storage");
static_assert(offsetof(aioTimer, header) == 0, "reactor timer must start with the common header");

void initializeTimer(aioTimer &timer, asyncBase *base = nullptr)
{
  timerInitialize(&timer);
  timer.header.base = base;
}

inline uint128 peekWheelSlot(TestBackend &backend, unsigned level, unsigned index)
{
  return __uint128_atomic_load(&backend.base.timerWheel.slots[level][index]);
}

// Detach a slot's list without delivering it: diagnostic recovery for tests
// that deliberately leave a link behind the checkpoint
inline asyncOpListLink *drainWheelSlot(TestBackend &backend, unsigned level, unsigned index)
{
  volatile uint128 *slot = &backend.base.timerWheel.slots[level][index];
  uint128 observed = __uint128_atomic_load(slot);
  uint128 desired;
  do {
    desired.low = 0;
    desired.high = observed.high;
  } while (!__uint128_atomic_compare_and_swap(slot, &observed, desired));
  return reinterpret_cast<asyncOpListLink*>(static_cast<uintptr_t>(observed.low));
}

inline bool wheelSlotOccupied(TestBackend &backend, unsigned level, unsigned index)
{
  return (backend.base.timerWheel.occupancy[level][index >> 6] >> (index & 63)) & 1;
}

// Heap link for direct wheel insertion: the sweep recycles links into the
// global link pool, so a stack instance would leave a dangling pointer for a
// later addToTimeoutQueue
asyncOpListLink *makeHeapLink(TestOp &op, TestObject &object, uintptr_t generation, uint64_t deadlineTick)
{
  auto *link = static_cast<asyncOpListLink*>(malloc(sizeof(asyncOpListLink)));
  link->op = &op.root;
  link->generation = generation;
  link->object = &object.root;
  link->objectGeneration = __uint64_atomic_load(&object.root.header.tag.high, amoRelaxed);
  link->next = nullptr;
  link->deadlineTick = deadlineTick;
  return link;
}

// Mounts a stack cell as the operation's realtime timer the way the backends
// create one. Stack instances must mirror the alignment of production cells
// (alignedMalloc) at their declaration: alignas(TAGGED_POINTER_ALIGNMENT).
void makeOperationTimer(aioTimer &timer, TestBackend &backend, TestOp &op)
{
  initializeTimer(timer, &backend.base);
  timer.operation.op = &op.root;
  op.root.timerId = &timer;
  timer.header.timer.kind = tkOperation;
}

// Wires a stack event/timer pair the way newUserEvent and the first arm do:
// the pairing is permanent, tag.low holds one reference and tag.high the
// event incarnation.
void makeEventTimerFixture(aioUserEvent &event, aioTimer &timer, uint64_t incarnation)
{
  __uint64_atomic_store(&event.header.tag.low, 1, amoRelaxed);
  __uint64_atomic_store(&event.header.tag.high, incarnation, amoRelaxed);
  event.timer = &timer;
  initializeTimer(timer);
  timer.event.userEvent = &event;
  timer.header.timer.kind = tkUserEvent;
}

// Publication helpers, one per arm flavor. Each reproduces the exact store
// sequence and memory order of the corresponding backend arm; the ident
// flavors also write the composite kernel id into fd.
void publishOperationIdent(aioTimer &timer, aioObjectRoot &object, uint64_t generation, uint64_t ident)
{
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.header), amoRelaxed);
  timerPublishEnd(&timer, generation, ident);
}

void publishOperationDeadline(aioTimer &timer, aioObjectRoot &object, uint64_t deadline, uint64_t generation)
{
  timerPublishBegin(&timer);
    __uint64_atomic_store(&timer.operation.deadline, deadline, amoRelaxed);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.header), amoRelaxed);
  timerPublishEnd(&timer, generation, 1);
}

void publishEventIdent(aioTimer &timer, aioUserEvent &event, uint64_t generation, uint64_t ident)
{
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __uint64_atomic_store(&timer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&timer, generation, ident);
}

void publishEventSchedule(aioTimer &timer, aioUserEvent &event, uint64_t generation)
{
  timerPublishBegin(&timer);
    __uint64_atomic_store(&timer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&timer, generation, 1);
}

TEST(timer_grid, rounds_deadlines_up_and_delivers_each_window_once)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object), third(object);
  // 125 ms ticks: 1000001 us rounds up to tick 9, both others to tick 16
  first.root.deadlineTick = timerDeadlineTick(1000001);
  second.root.deadlineTick = timerDeadlineTick(1999999);
  third.root.deadlineTick = timerDeadlineTick(2000000);

  addToTimeoutQueue(&backend.base, &first.root);
  addToTimeoutQueue(&backend.base, &second.root);
  addToTimeoutQueue(&backend.base, &third.root);

  auto *firstLink = static_cast<asyncOpListLink*>(first.root.timerId);
  auto *secondLink = static_cast<asyncOpListLink*>(second.root.timerId);
  auto *thirdLink = static_cast<asyncOpListLink*>(third.root.timerId);
  ASSERT_NE(firstLink, nullptr);
  EXPECT_EQ(firstLink->deadlineTick, 9u);
  EXPECT_EQ(secondLink->deadlineTick, 16u);
  EXPECT_EQ(thirdLink->deadlineTick, 16u);

  // Level-0 routing, same-window links stack LIFO
  EXPECT_EQ(peekWheelSlot(backend, 0, 9).low, reinterpret_cast<uint64_t>(firstLink));
  ASSERT_EQ(peekWheelSlot(backend, 0, 16).low, reinterpret_cast<uint64_t>(thirdLink));
  EXPECT_EQ(thirdLink->next, secondLink);

  // The sweep to tick 10 delivers only the tick-9 window; a repeated sweep is
  // a checkpoint no-op and cannot deliver the same window twice
  processTimeoutQueue(&backend.base, nullptr, 10);
  EXPECT_EQ(opGetStatus(&first.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&second.root), aosPending);
  processTimeoutQueue(&backend.base, nullptr, 10);
  EXPECT_EQ(opGetStatus(&second.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 0, 9).low, 0u);

  processTimeoutQueue(&backend.base, nullptr, 17);
  EXPECT_EQ(opGetStatus(&second.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&third.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 0, 16).low, 0u);

  objectDecrementReference(&object.root, 3);
  objectDelete(&object.root);
}

TEST(timer_grid, expiration_cancels_and_releases_waiting_operation)
{
  TestBackend backend;
  backend.base.timerCloseCursor = 100;
  TestObject object(backend);
  TestOp op(object);
  op.root.deadlineTick = 100;
  eqPushBack(&object.root.readQueue, &op.root);
  addToTimeoutQueue(&backend.base, &op.root);

  processTimeoutQueue(&backend.base, nullptr, 101);

  EXPECT_EQ(backend.base.timerCloseCursor, 101u);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(op.releaseCalls, 1u);
  EXPECT_EQ(object.root.readQueue.head, nullptr);
  backend.drainCompletions();
  EXPECT_EQ(op.callbackStatus, aosTimeout);
  objectDelete(&object.root);
}

// Terminal-window regression: the sweep reopens window 100 and publishes
// checkpoint 101, then the producer resumes and arms a deadline-100 timer.
// The publication must be refused (the producer observes the swept window)
// and the operation expired immediately instead of being delivered one
// rotation late.
TEST(timer_grid, late_arm_after_swept_checkpoint_expires_instead_of_being_stranded)
{
  TestBackend backend;
  backend.base.timerCloseCursor = 100;
  TestObject object(backend);
  TestOp op(object);
  op.root.deadlineTick = 100;
  eqPushBack(&object.root.readQueue, &op.root);

  processTimeoutQueue(&backend.base, nullptr, 101);
  ASSERT_EQ(backend.base.timerCloseCursor, 101u);
  addToTimeoutQueue(&backend.base, &op.root);

  AsyncOpStatus statusAfterArm = opGetStatus(&op.root);
  asyncOpListLink *stranded = drainWheelSlot(backend, 0, 100);
  EXPECT_EQ(statusAfterArm, aosTimeout) << "a timer armed after its window was swept was not expired immediately";
  EXPECT_EQ(stranded, nullptr) << "the late timer was published into the reopened slot and will fire a rotation late";

  // Keep the known-red test hygienic on the current implementation. The
  // diagnostic drain above recovered ownership of the physical link; it must
  // not remain in the global link pool with a pointer to this stack operation.
  if (stranded) {
    op.root.timerId = nullptr;
    free(stranded);
    cancelAndDrain(backend, object);
  } else {
    backend.drainCompletions();
  }
  objectDelete(&object.root);
}

TEST(timer_grid, current_or_stale_clock_reading_is_a_noop)
{
  TestBackend backend;
  backend.base.timerCloseCursor = 10;

  processTimeoutQueue(&backend.base, nullptr, 10);
  EXPECT_EQ(backend.base.timerCloseCursor, 10u);

  // A caller whose clock reading is behind the confirmed cursor (it raced a
  // faster sweeper) must not sweep anything or move the cursor backwards
  processTimeoutQueue(&backend.base, nullptr, 8);
  EXPECT_EQ(backend.base.timerCloseCursor, 10u);
}

TEST(timer_grid, stale_generation_link_does_not_cancel_reused_operation)
{
  TestBackend backend;
  backend.base.timerCloseCursor = 7;
  TestObject object(backend);
  TestOp op(object);
  uintptr_t staleGeneration = opGetGeneration(&op.root);
  op.root.tag = ((staleGeneration + 1) << TAG_STATUS_SIZE) | aosPending;
  asyncOpListLink *link = makeHeapLink(op, object, staleGeneration, 7);
  timerWheelInsert(&backend.base, link, 7);

  processTimeoutQueue(&backend.base, nullptr, 8);

  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_EQ(__uint64_atomic_load(&object.root.header.tag.low, amoRelaxed), 0u);
  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_grid, ordinary_operation_arms_monotonic_grid_timer)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afNone, 1);
  op.setResults({aosPending});

  combinerPushOperation(&op.root);

  EXPECT_NE(op.root.timerId, nullptr);
  EXPECT_GT(op.root.deadlineTick, 0u);
  EXPECT_EQ(backend.startTimerCalls, 0u);

  cancelIo(&object.root);
  backend.drainCompletions();
  // The armed link stays parked in the wheel with a now-stale generation; the
  // TestBackend teardown recycles it without touching this stack operation
  objectDelete(&object.root);
}

TEST(timer_grid, huge_timeout_saturates_instead_of_wrapping)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afNone, UINT64_MAX);
  op.setResults({aosPending});

  // UINT64_MAX must mean "practically never": unsaturated now + timeout
  // wraps to nowUs - 1 and fires at once
  combinerPushOperation(&op.root);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  processTimeoutQueue(&backend.base, nullptr, getMonotonicTicks() + 2);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  cancelIo(&object.root);
  backend.drainCompletions();
  objectDelete(&object.root);
}

TEST(timer_grid, timeout_saturates_at_documented_maximum)
{
  TestBackend backend;
  TestObject object(backend);
  // The public contract: usTimeout saturates at MAX_TIMEOUT_US right at op
  // initialization, so every backend arm (wheel, timerfd, kevent, waitable
  // timer) stays inside its kernel range instead of wrapping there. The
  // assert must precede the push: the grid arm repurposes the union slot
  // as the absolute deadline tick.
  TestOp op(object, OPCODE_READ, afNone, UINT64_MAX);
  op.setResults({aosPending});
  EXPECT_EQ(op.root.timeout, MAX_TIMEOUT_US);

  combinerPushOperation(&op.root);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  cancelIo(&object.root);
  backend.drainCompletions();

  // The realtime arm hands op->timeout to the backend as-is: the clamped
  // period is what reaches the kernel
  TestOp realtimeOp(object, OPCODE_READ, afRealtime, UINT64_MAX);
  realtimeOp.setResults({aosPending});
  combinerPushOperation(&realtimeOp.root);
  EXPECT_EQ(realtimeOp.root.timeout, MAX_TIMEOUT_US);
  EXPECT_EQ(backend.startTimerCalls, 1u);

  cancelIo(&object.root);
  backend.drainCompletions();
  objectDelete(&object.root);
}

TEST(timer_wheel, detach_reopens_and_is_idempotent_per_incarnation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &op.root);
  auto *link = static_cast<asyncOpListLink*>(op.root.timerId);
  ASSERT_NE(link, nullptr);
  ASSERT_EQ(link->deadlineTick, 5u);

  // The winner takes the chain, and the same CAS reopens the slot for the
  // next rotation
  asyncOpListLink *chain = timerWheelDetach(&backend.base, nullptr, 0, 5);
  ASSERT_EQ(chain, link);
  uint128 slotPair = peekWheelSlot(backend, 0, 5);
  EXPECT_EQ(slotPair.low, 0u);
  EXPECT_EQ(slotPair.high, 5u + TIMER_WHEEL_SLOTS);

  // Any other visitor of the same window observes the advanced baseTick:
  // no chain, no second reopen
  EXPECT_EQ(timerWheelDetach(&backend.base, nullptr, 0, 5), nullptr);
  EXPECT_EQ(peekWheelSlot(backend, 0, 5).high, 5u + TIMER_WHEEL_SLOTS);

  // Publication into the drained window is refused: its deadlines are
  // terminal, and the wheel must not accept a link that could only fire a
  // rotation late
  link->next = nullptr;
  EXPECT_EQ(timerWheelInsert(&backend.base, link, 5), 0);
  EXPECT_EQ(peekWheelSlot(backend, 0, 5).low, 0u);

  // The refused link stays with its owner and is processed exactly as during
  // a sweep: the timeout is delivered and the link recycled
  timerWheelProcessDetached(&backend.base, chain, 5);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, bitless_detach_uses_fast_path_without_preclear_bracket)
{
  TestBackend backend;

  // An empty slot still has to advance its incarnation, but there is no
  // destructive occupancy clear to protect.
  EXPECT_EQ(timerWheelDetach(&backend.base, &backend.loopStates[3], 0, 4),
            nullptr);
  EXPECT_EQ(peekWheelSlot(backend, 0, 4).high,
            4u + TIMER_WHEEL_SLOTS);
  EXPECT_EQ(backend.loopStates[3].preclearSequence, 0u);
  EXPECT_EQ(backend.base.timerPreclearOverflow, 0u);

  // A live bitless chain models a publisher stalled between its pair CAS and
  // occupancy set. The same fast DWCAS must acquire the chain safely without
  // opening a bracket.
  TestObject object(backend);
  TestOp op(object);
  asyncOpListLink *link =
    makeHeapLink(op, object, opGetGeneration(&op.root), 5);
  volatile uint128 *slot = &backend.base.timerWheel.slots[0][5];
  uint128 observed = __uint128_atomic_load(slot);
  uint128 desired{reinterpret_cast<uint64_t>(link), observed.high};
  ASSERT_TRUE(__uint128_atomic_compare_and_swap(slot, &observed, desired));
  ASSERT_FALSE(wheelSlotOccupied(backend, 0, 5));

  asyncOpListLink *chain =
    timerWheelDetach(&backend.base, &backend.loopStates[3], 0, 5);
  ASSERT_EQ(chain, link);
  EXPECT_EQ(backend.loopStates[3].preclearSequence, 0u);
  EXPECT_EQ(backend.base.timerPreclearOverflow, 0u);
  timerWheelProcessDetached(&backend.base, chain, 5);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, arm_behind_multiple_rotations_expires_without_publication)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // The producer was preempted between reading the cursor and publishing for
  // two whole rotations of the deadline's slot: the slot alone proves the
  // window is long gone, whatever the stale cursor hint says
  EXPECT_EQ(timerWheelDetach(&backend.base, nullptr, 0, 5), nullptr);
  EXPECT_EQ(timerWheelDetach(&backend.base, nullptr, 0, 5 + TIMER_WHEEL_SLOTS), nullptr);
  op.root.deadlineTick = 5;
  eqPushBack(&object.root.readQueue, &op.root);

  addToTimeoutQueue(&backend.base, &op.root);

  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(op.root.timerId, nullptr);
  EXPECT_EQ(peekWheelSlot(backend, 0, 5).low, 0u);
  backend.drainCompletions();
  EXPECT_EQ(op.callbackStatus, aosTimeout);
  objectDelete(&object.root);
}

TEST(timer_wheel, publication_descends_into_open_lower_window_of_drained_upper)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // The sweep legally passed the level-1 boundary: the [1024, 2048) window is
  // drained, but tick 1030 inside it is still ahead of the cursor
  processTimeoutQueue(&backend.base, nullptr, 1025);
  ASSERT_EQ(backend.base.timerCloseCursor, 1025u);

  // A producer with a stale cursor hint routes to the drained level-1 window
  // first; the publication must descend into the still-open level-0 slot -
  // not be refused and not park a rotation late in level 1
  asyncOpListLink *link = makeHeapLink(op, object, opGetGeneration(&op.root), 1030);
  ASSERT_EQ(timerWheelInsert(&backend.base, link, 0), 1);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).low, reinterpret_cast<uint64_t>(link));
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);

  // Delivered by the normal sweep in its exact tick
  processTimeoutQueue(&backend.base, nullptr, 1030);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  processTimeoutQueue(&backend.base, nullptr, 1031);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, expired_arm_never_starts_initialization_io)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_WRITE, afNone, 1);
  op.setResults({aosPending});
  // A connect whose timeout elapsed before the submission reached the
  // combiner: the deadline computed from the monotonic clock lies behind the
  // confirmed sweep position
  backend.base.timerCloseCursor = static_cast<uintptr_t>(getMonotonicTicks() + 1024);
  ASSERT_TRUE(__uintptr_atomic_compare_and_swap(&object.root.initializationOp, 0, reinterpret_cast<uintptr_t>(&op.root), amoSeqCst));

  combinerPushOperation(&op.root);

  EXPECT_EQ(op.executeCalls, 0u) << "initialization I/O was issued after its timeout had already won";
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(__uintptr_atomic_load(&object.root.initializationOp, amoRelaxed), 0u);
  backend.drainCompletions();
  EXPECT_EQ(op.callbackStatus, aosTimeout);
  objectDelete(&object.root);
}

TEST(timer_wheel, helper_confirms_cursor_and_stalled_owner_keeps_its_chain)
{
  TestBackend backend;
  backend.base.timerCloseCursor = 100;
  TestObject object(backend);
  TestOp stalledOp(object), liveOp(object);
  stalledOp.root.deadlineTick = 100;
  liveOp.root.deadlineTick = 101;
  addToTimeoutQueue(&backend.base, &stalledOp.root);
  addToTimeoutQueue(&backend.base, &liveOp.root);

  // Sweeper A wins the tick-100 visit and stalls before both the confirm CAS
  // and the processing of its detached chain
  asyncOpListLink *ownedChain = timerWheelDetach(&backend.base, nullptr, 0, 100);
  ASSERT_EQ(ownedChain, static_cast<asyncOpListLink*>(stalledOp.root.timerId));
  ASSERT_EQ(backend.base.timerCloseCursor, 100u);

  // Sweeper B's tick-100 visits are idempotent no-ops, yet B confirms the
  // cursor on A's behalf and the ticks behind the stalled one stay alive
  processTimeoutQueue(&backend.base, nullptr, 102);
  EXPECT_EQ(backend.base.timerCloseCursor, 102u);
  EXPECT_EQ(opGetStatus(&liveOp.root), aosTimeout);
  // Helping never delivers or steals the private chain
  EXPECT_EQ(opGetStatus(&stalledOp.root), aosPending);
  EXPECT_EQ(ownedChain->next, nullptr);

  // A resumes: its chain is processed as if the stall never happened
  timerWheelProcessDetached(&backend.base, ownedChain, 100);
  EXPECT_EQ(opGetStatus(&stalledOp.root), aosTimeout);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(timer_wheel, resurfaced_sweeper_with_stale_tick_cannot_rewind_cursor)
{
  TestBackend backend;
  backend.base.timerCloseCursor = 100;

  processTimeoutQueue(&backend.base, nullptr, 110);
  ASSERT_EQ(backend.base.timerCloseCursor, 110u);

  // A sweeper that stalled before its confirm CAS resurfaces long after its
  // tick was confirmed by helpers: the visits are idempotent no-ops and the
  // tick-exact confirm CAS misses, a plain checkpoint store would rewind here
  timerWheelSweepTick(&backend.base, nullptr, 100);
  EXPECT_EQ(backend.base.timerCloseCursor, 110u);

  processTimeoutQueue(&backend.base, nullptr, 105);
  EXPECT_EQ(backend.base.timerCloseCursor, 110u);
}

TEST(timer_wheel, boundary_deadline_migrates_and_fires_in_its_exact_tick)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp boundary(object), inWindow(object);
  // Against cursor 0 both deadlines differ in the bits 10..19 group, so both
  // are routed to level 1 (the [1024, 2048) window slot)
  boundary.root.deadlineTick = 1024;
  inWindow.root.deadlineTick = 1025;
  addToTimeoutQueue(&backend.base, &boundary.root);
  addToTimeoutQueue(&backend.base, &inWindow.root);
  ASSERT_EQ(peekWheelSlot(backend, 0, 0).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 0, 1).low, 0u);
  ASSERT_NE(peekWheelSlot(backend, 1, 1).low, 0u);

  // The whole level-0 rotation before the boundary delivers nothing
  processTimeoutQueue(&backend.base, nullptr, 1024);
  EXPECT_EQ(opGetStatus(&boundary.root), aosPending);
  EXPECT_EQ(opGetStatus(&inWindow.root), aosPending);

  // Tick 1024 visits level 1 before level 0: the boundary deadline expires in
  // this very tick (not one rotation later), the in-window one migrates down
  // into its level-0 slot
  processTimeoutQueue(&backend.base, nullptr, 1025);
  EXPECT_EQ(opGetStatus(&boundary.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&inWindow.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).low, reinterpret_cast<uint64_t>(inWindow.root.timerId));

  processTimeoutQueue(&backend.base, nullptr, 1026);
  EXPECT_EQ(opGetStatus(&inWindow.root), aosTimeout);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(timer_wheel, numeric_distance_keeps_adjacent_boundary_deadline_in_level_zero)
{
  TestBackend backend;
  timerWheelInit(&backend.base, 1023);
  TestObject object(backend);
  TestOp op(object);

  // XOR sees a differing bit in the next radix group here. Numeric distance
  // is one tick, so no level-1 publication/cascade is needed.
  op.root.deadlineTick = 1024;
  addToTimeoutQueue(&backend.base, &op.root);
  auto *link = static_cast<asyncOpListLink*>(op.root.timerId);
  ASSERT_EQ(peekWheelSlot(backend, 0, 0).low,
            reinterpret_cast<uint64_t>(link));
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);

  processTimeoutQueue(&backend.base, nullptr, 1025);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, cascade_chain_migrates_l2_to_l1_to_l0_and_fires_in_its_exact_tick)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp boundary(object), chained(object);
  // Both deadlines differ from cursor 0 in the bits 20..29 group: level-2
  // routing. The boundary one sits exactly on the window start; the chained
  // one needs the full migration ladder level 2 -> level 1 -> level 0
  const uint64_t l2Window = uint64_t{1} << (2 * TIMER_WHEEL_LEVEL_BITS);
  const uint64_t chainedDeadline = l2Window + TIMER_WHEEL_SLOTS + 1;
  boundary.root.deadlineTick = l2Window;
  chained.root.deadlineTick = chainedDeadline;
  addToTimeoutQueue(&backend.base, &boundary.root);
  addToTimeoutQueue(&backend.base, &chained.root);
  ASSERT_EQ(peekWheelSlot(backend, 2, 1).low, reinterpret_cast<uint64_t>(chained.root.timerId));
  ASSERT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);

  // Catching up a whole level-1 rotation in one call (a base resuming from
  // suspend) delivers nothing ahead of time
  processTimeoutQueue(&backend.base, nullptr, l2Window);
  EXPECT_EQ(opGetStatus(&boundary.root), aosPending);
  EXPECT_EQ(opGetStatus(&chained.root), aosPending);

  // Tick 2^20 visits level 2: the boundary deadline expires in this very tick
  // (not one rotation later), the chained one migrates into the level-1
  // window reopened by the sweep in exact lockstep
  processTimeoutQueue(&backend.base, nullptr, l2Window + 1);
  EXPECT_EQ(opGetStatus(&boundary.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&chained.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 2, 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 1, 1).low, reinterpret_cast<uint64_t>(chained.root.timerId));
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).high, l2Window + TIMER_WHEEL_SLOTS);

  // The level-1 boundary tick migrates it again, into the level-0 slot whose
  // current incarnation covers exactly the deadline tick
  processTimeoutQueue(&backend.base, nullptr, chainedDeadline);
  EXPECT_EQ(opGetStatus(&chained.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 0, 1).low, reinterpret_cast<uint64_t>(chained.root.timerId));
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).high, chainedDeadline);

  // The last hop is the delivery itself, in the exact deadline tick
  processTimeoutQueue(&backend.base, nullptr, chainedDeadline + 1);
  EXPECT_EQ(opGetStatus(&chained.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).low, 0u);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(timer_wheel, boundary_deadline_at_top_level_window_start_fires_at_its_visit)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // Differs from cursor 0 in the bits 30..39 group: top-level routing, into
  // the [2^30, 2^31) window whose slot still holds its initial incarnation
  const uint64_t l3Window = uint64_t{1} << (3 * TIMER_WHEEL_LEVEL_BITS);
  op.root.deadlineTick = l3Window;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_EQ(peekWheelSlot(backend, 3, 1).low, reinterpret_cast<uint64_t>(op.root.timerId));

  // Sweeping 2^30 ticks one by one is out of unit-test reach; the visit units
  // run directly at the exact window-start tick, as the contiguous sweep
  // would. The window-start deadline is due at this very visit and must be
  // delivered here, not re-parked for another rotation
  timerWheelProcessDetached(&backend.base, timerWheelDetach(&backend.base, nullptr, 3, l3Window), l3Window);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 3, 1).low, 0u);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, beyond_range_deadline_clamps_to_last_representable_point)
{
  TestBackend backend;
  timerWheelInit(&backend.base, 5);
  TestObject object(backend);
  TestOp op(object);
  // The deadline is exactly one full range ahead. Routing clamps the target
  // point to cursor + range - 1, the last point represented by the wheel.
  const uint64_t wheelRange = uint64_t{1} << (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS);
  op.root.deadlineTick = (wheelRange + 5);
  addToTimeoutQueue(&backend.base, &op.root);
  auto *link = static_cast<asyncOpListLink*>(op.root.timerId);
  EXPECT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 3, 0).low, reinterpret_cast<uint64_t>(link));
  EXPECT_EQ(peekWheelSlot(backend, 3, 0).high, wheelRange);
  EXPECT_TRUE(wheelSlotOccupied(backend, 3, 0));
  EXPECT_EQ(link->deadlineTick, wheelRange + 5);

  // Still pending and parked: the wheel teardown recycles the link without
  // delivering it
  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, near_deadline_across_range_boundary_is_delivered_not_clamped)
{
  const uint64_t range = uint64_t{1} << (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS);
  TestBackend backend;
  timerWheelInit(&backend.base, range - 1);
  TestObject object(backend);
  TestOp op(object);

  // One tick ahead, but across the 2^40 binary boundary - only the XOR
  // image looks out-of-range
  op.root.deadlineTick = range;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_EQ(opGetStatus(&op.root), aosPending);

  // Two sweep ticks cover the deadline: the timeout is due here
  processTimeoutQueue(&backend.base, nullptr, range + 1);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 2).low, 0u);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, far_insert_with_stale_cursor_refuses_already_swept_deadline)
{
  const uint64_t range = uint64_t{1} << (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS);
  TestBackend backend;
  timerWheelInit(&backend.base, range + 10);
  TestObject object(backend);
  TestOp op(object);

  // Stale hint (allowed by contract) from before the boundary, deadline
  // already behind the confirmed sweep: the insert must refuse, not park
  // the link into a future incarnation
  asyncOpListLink *link = makeHeapLink(op, object, opGetGeneration(&op.root), range + 2);
  int inserted = timerWheelInsert(&backend.base, link, range - 1);
  EXPECT_EQ(inserted, 0);
  EXPECT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 2).low, 0u);
  if (!inserted)
    free(link);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, stalled_cascade_into_swept_window_delivers_exactly_once)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // Level-1 routing against cursor 0: window [1024, 2048), deadline inside it
  op.root.deadlineTick = 1030;
  addToTimeoutQueue(&backend.base, &op.root);
  auto *link = static_cast<asyncOpListLink*>(op.root.timerId);
  ASSERT_EQ(peekWheelSlot(backend, 1, 1).low, reinterpret_cast<uint64_t>(link));

  processTimeoutQueue(&backend.base, nullptr, 1024);

  // The owner wins the level-1 boundary visit and stalls before processing
  // its chain; helpers sweep past the deadline meanwhile, so the level-0 slot
  // the cascade is aiming at is already reopened for its next rotation
  asyncOpListLink *ownedChain = timerWheelDetach(&backend.base, nullptr, 1, 1024);
  ASSERT_EQ(ownedChain, link);
  processTimeoutQueue(&backend.base, nullptr, 1031);
  ASSERT_EQ(backend.base.timerCloseCursor, 1031u);
  ASSERT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).high, 1030u + TIMER_WHEEL_SLOTS);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  // The resumed cascade observes the terminal window and must deliver right
  // now (late, never early - the deadline tick is already swept), not park
  // the link into the reopened incarnation to fire a rotation later
  timerWheelProcessDetached(&backend.base, ownedChain, 1024);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).low, 0u);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).high, 1030u + TIMER_WHEEL_SLOTS);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, terminal_same_generation_link_is_dropped_before_recascade)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  uintptr_t generation = opGetGeneration(&op.root);
  asyncOpListLink *link = makeHeapLink(op, object, generation, 5000);
  ASSERT_TRUE(timerWheelInsert(&backend.base, link, 0));
  ASSERT_EQ(peekWheelSlot(backend, 1, 4).low,
            reinterpret_cast<uint64_t>(link));

  // Completion changed only the status bits. A generation-only check would
  // re-publish this dead link into level 0 at the boundary.
  __uintptr_atomic_store(&op.root.tag,
                         (generation << TAG_STATUS_SIZE) | aosSuccess,
                         amoRelaxed);
  timerWheelProcessDetached(
    &backend.base,
    timerWheelDetach(&backend.base, nullptr, 1, 4096), 4096);
  EXPECT_EQ(opGetStatus(&op.root), aosSuccess);
  EXPECT_EQ(peekWheelSlot(backend, 0, 5000 % TIMER_WHEEL_SLOTS).low, 0u);
  EXPECT_FALSE(wheelSlotOccupied(backend, 0,
                                 5000 % TIMER_WHEEL_SLOTS));

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

// The visitor pre-clears the occupancy bit before its drain CAS, so losing
// the drain race can leave the clear ordered after a fresh publication into
// the reopened incarnation. The idempotent exit must repair that: a live
// chain observed behind an advanced baseTick gets its bit re-set before the
// visitor backs off empty-handed.
TEST(timer_wheel, stale_detach_restores_live_occupancy_bit)
{
  TestBackend backend;
  timerWheelInit(&backend.base, 2048);
  TestObject object(backend);
  TestOp op(object);

  // Park a link in level-0 slot 5 (tick 2053 of the current rotation)
  op.root.deadlineTick = 2053;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 0, 5));

  // Simulate the lost race: the stale visitor's pre-clear landing after the
  // fresh publication
  __uintptr_atomic_fetch_and(&backend.base.timerWheel.occupancy[0][0], ~(static_cast<uintptr_t>(1) << 5), amoSeqCst);

  // The stale visitor re-reads the pair, sees the advanced incarnation with
  // a live chain and restores the bit while claiming nothing
  EXPECT_EQ(timerWheelDetach(&backend.base, nullptr, 0, 2053 - TIMER_WHEEL_SLOTS), nullptr);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5));
  EXPECT_NE(peekWheelSlot(backend, 0, 5).low, 0u);

  processTimeoutQueue(&backend.base, nullptr, 2054);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wakeup, occupancy_bit_follows_link_lifecycle)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp delivered(object), stacked(object), late(object);
  ASSERT_FALSE(wheelSlotOccupied(backend, 0, 5));

  // Activating an empty slot sets its bit; stacking onto a live chain
  // re-asserts it (every publisher performs the idempotent set itself).
  delivered.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &delivered.root);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5));
  stacked.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &stacked.root);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5));

  // The visit clears the bit before draining the chain
  processTimeoutQueue(&backend.base, nullptr, 6);
  EXPECT_EQ(opGetStatus(&delivered.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&stacked.root), aosTimeout);
  EXPECT_FALSE(wheelSlotOccupied(backend, 0, 5));

  // A refused late arm never becomes visible: no bit and no kick, the
  // timeout is delivered by the arm itself
  late.root.deadlineTick = 3;
  addToTimeoutQueue(&backend.base, &late.root);
  EXPECT_FALSE(wheelSlotOccupied(backend, 0, 3));
  EXPECT_EQ(opGetStatus(&late.root), aosTimeout);
  EXPECT_EQ(backend.wakeupCalls, 0u);

  objectDecrementReference(&object.root, 3);
  objectDelete(&object.root);
}

TEST(timer_wakeup, stacked_arm_is_visible_to_sleep_despite_stalled_first_publisher)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp stalledOp(object), stackedOp(object);

  // Producer P1 activates the empty level-0 slot 5 and stalls between the
  // publish CAS and the occupancy set - the exact first half of
  // timerWheelTryPublish. The chain is live, the bitmap still shows nothing
  asyncOpListLink *stalledLink = makeHeapLink(stalledOp, object, opGetGeneration(&stalledOp.root), 5);
  volatile uint128 *slot = &backend.base.timerWheel.slots[0][5];
  uint128 observed = __uint128_atomic_load(slot);
  ASSERT_EQ(observed.low, 0u);
  uint128 desired;
  desired.low = reinterpret_cast<uint64_t>(stalledLink);
  desired.high = observed.high;
  ASSERT_TRUE(__uint128_atomic_compare_and_swap(slot, &observed, desired));
  ASSERT_FALSE(wheelSlotOccupied(backend, 0, 5));

  // Producer P2 stacks onto the live chain through the production path and
  // returns: from here on the armed timer must not depend on P1 resuming
  stackedOp.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &stackedOp.root);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5));

  // Sleep planning reads only the bitmap: a bitless chain would park the
  // loop in an eternal wait with both timers lost until P1 resumes
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0), 6u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 6u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  processTimeoutQueue(&backend.base, nullptr, 6);
  EXPECT_EQ(opGetStatus(&stalledOp.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&stackedOp.root), aosTimeout);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(timer_wakeup, stalled_visitor_between_clear_and_drain_does_not_hide_due_chain)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);

  // A timer parked at tick 5 through the production path; the sweep catches
  // up to its tick without visiting it yet
  op.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &op.root);
  processTimeoutQueue(&backend.base, nullptr, 5);
  ASSERT_EQ(backend.base.timerCloseCursor, 5u);
  ASSERT_TRUE(wheelSlotOccupied(backend, 0, 5));

  // The tick-5 visitor opens its bracket, performs the occupancy clear and
  // stalls before the drain CAS: the slot still holds the live due chain,
  // the bitmap no longer shows it
  __uintptr_atomic_fetch_and_add(&backend.base.timerPreclearOverflow,
                                 timerPreclearOverflowEntry, amoSeqCst);
  __uintptr_atomic_fetch_and(&backend.base.timerWheel.occupancy[0][0],
                             ~(static_cast<uintptr_t>(1) << 5), amoSeqCst);
  ASSERT_NE(peekWheelSlot(backend, 0, 5).low, 0u);

  // Another loop planning its sleep sees the open bracket and must not trust
  // its empty scan: an eternal wait here would park the loop until the
  // stalled visitor resumes
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 5), 6u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 6u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // The woken loop drains the tick as a helper; the stalled visitor is not
  // needed for delivery
  processTimeoutQueue(&backend.base, nullptr, 6);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 0, 5).low, 0u);
  __uintptr_atomic_fetch_and_add(&backend.base.timerPreclearOverflow,
                                 ~static_cast<uintptr_t>(0), amoRelease);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wakeup, stalled_boundary_visitor_does_not_hide_cascading_window)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);

  // Tick 5000 parks in level-1 slot 4 (window [4096, 5120)); the sweep stops
  // right at the window's visit boundary
  op.root.deadlineTick = 5000;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 1, 4));
  processTimeoutQueue(&backend.base, nullptr, 4096);
  ASSERT_EQ(backend.base.timerCloseCursor, 4096u);

  // The boundary visitor opens its bracket, clears the level-1 bit and
  // stalls before its drain CAS; the cascade the sleeper counts on has not
  // happened
  __uintptr_atomic_fetch_and_add(&backend.base.timerPreclearOverflow,
                                 timerPreclearOverflowEntry, amoSeqCst);
  __uintptr_atomic_fetch_and(&backend.base.timerWheel.occupancy[1][0],
                             ~(static_cast<uintptr_t>(1) << 4), amoSeqCst);

  // The boundary window still holds the chain behind the open bracket: the
  // sleeper must wake right behind the cursor instead of waiting eternally
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 4096), 4097u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 4097u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // The woken helper performs the visit and the stalled visitor resumes
  // (losing CAS, closed bracket): the chain cascades into level 0 with its
  // own bit and the following scan stretches to the exact deadline
  processTimeoutQueue(&backend.base, nullptr, 4097);
  __uintptr_atomic_fetch_and_add(&backend.base.timerPreclearOverflow,
                                 ~static_cast<uintptr_t>(0), amoRelease);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5000 % TIMER_WHEEL_SLOTS));
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 4097), 5001u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  processTimeoutQueue(&backend.base, nullptr, 5001);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wakeup, stale_clear_of_reopened_slot_caps_sleep_while_visitor_is_parked)
{
  TestBackend backend;
  timerWheelInit(&backend.base, 1023);
  TestObject object(backend);
  TestOp op(object);

  // The tick-1023 visitor loads the pair {*, baseTick 1023} and stalls
  // before its occupancy pre-clear (executed below); a helper sweeps the
  // tick, reopens the physical slot for baseTick 2047 and confirms the cursor
  processTimeoutQueue(&backend.base, nullptr, 1024);
  ASSERT_EQ(backend.base.timerCloseCursor, 1024u);
  ASSERT_EQ(peekWheelSlot(backend, 0, 1023).high, 2047u);

  // A fresh arm parks tick 2047 into the reopened incarnation
  op.root.deadlineTick = 2047;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 0, 1023));
  ASSERT_NE(peekWheelSlot(backend, 0, 1023).low, 0u);

  // The stale visitor resumes inside its still-open bracket and its
  // pre-clear lands on the NEW incarnation's bit (the bit carries no
  // incarnation number); it stalls again before its losing CAS
  __uintptr_atomic_fetch_and_add(&backend.base.timerPreclearOverflow,
                                 timerPreclearOverflowEntry, amoSeqCst);
  __uintptr_atomic_fetch_and(&backend.base.timerWheel.occupancy[0][1023 >> 6],
                             ~(static_cast<uintptr_t>(1) << (1023 & 63)), amoSeqCst);

  // CONTRACT: the open bracket forbids committing to an eternal wait - the
  // sleep is capped at one grid tick however long the visitor stays parked,
  // so delivery does not hinge on the guilty thread's schedule
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 1024), 1025u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 1025u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // The visitor's resume: the losing CAS refreshes the pair, the repair
  // re-sets the bit, the bracket closes
  timerWheelProcessDetached(&backend.base, timerWheelDetach(&backend.base, nullptr, 0, 1023), 1023);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 1023));
  __uintptr_atomic_fetch_and_add(&backend.base.timerPreclearOverflow,
                                 ~static_cast<uintptr_t>(0), amoRelease);

  // The next sleep decision takes the clean path to the exact horizon
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 1024), 2048u);
  timerLoopCancelSleep(&backend.loopStates[0]);
  processTimeoutQueue(&backend.base, nullptr, 2048);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wakeup, sleep_snapshot_excludes_only_the_callers_own_state)
{
  TestBackend backend;

  // The owning loop cannot be inside detach and sleep planning at once, so
  // its own sequence is intentionally absent from both snapshots.
  backend.loopStates[0].preclearSequence = 1;
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0),
            UINT64_MAX);
  timerLoopCancelSleep(&backend.loopStates[0]);
  backend.loopStates[0].preclearSequence = 0;

  // Every other loop remains visible. An open bracket makes the one-tick
  // result final without spending a full bitmap scan.
  backend.loopStates[1].preclearSequence = 1;
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0),
            1u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 1u);
  timerLoopCancelSleep(&backend.loopStates[0]);
  backend.loopStates[1].preclearSequence = 0;
}

TEST(timer_wheel, detach_bracket_routes_by_visitor_identity)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object);

  first.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &first.root);
  second.root.deadlineTick = 6;
  addToTimeoutQueue(&backend.base, &second.root);

  // A foreign thread (no loop identity) brackets through the overflow word:
  // one completed entry stays recorded in the high half
  timerWheelProcessDetached(&backend.base, timerWheelDetach(&backend.base, nullptr, 0, 5), 5);
  EXPECT_EQ(opGetStatus(&first.root), aosTimeout);
  EXPECT_EQ(backend.base.timerPreclearOverflow, timerPreclearOverflowEntry - 1);
  EXPECT_EQ(backend.loopStates[3].preclearSequence, 0u);

  // A registered loop passes its state explicitly; no TLS lookup is needed.
  timerWheelProcessDetached(
    &backend.base,
    timerWheelDetach(&backend.base, &backend.loopStates[3], 0, 6), 6);
  EXPECT_EQ(opGetStatus(&second.root), aosTimeout);
  EXPECT_EQ(backend.loopStates[3].preclearSequence, 2u);
  EXPECT_EQ(backend.base.timerPreclearOverflow, timerPreclearOverflowEntry - 1);

  // A visit of an already-reopened window opens no bracket
  EXPECT_EQ(timerWheelDetach(&backend.base, nullptr, 0, 5), nullptr);
  EXPECT_EQ(backend.base.timerPreclearOverflow, timerPreclearOverflowEntry - 1);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(timer_wakeup, cascade_reinsert_kicks_sleepers_that_would_miss_the_deadline)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp covered(object), uncovered(object);
  covered.root.deadlineTick = 5000;
  addToTimeoutQueue(&backend.base, &covered.root);
  uncovered.root.deadlineTick = 9200;
  addToTimeoutQueue(&backend.base, &uncovered.root);
  ASSERT_EQ(backend.wakeupCalls, 0u);

  // A sleeper due at tick 5001 meets the tick-5000 deadline: the cascade at
  // boundary 4096 re-parks the link without a kick
  backend.loopStates[2].wakeTick = 5001;
  processTimeoutQueue(&backend.base, nullptr, 4097);
  EXPECT_EQ(opGetStatus(&covered.root), aosPending);
  EXPECT_EQ(backend.wakeupCalls, 0u);
  processTimeoutQueue(&backend.base, nullptr, 5001);
  EXPECT_EQ(opGetStatus(&covered.root), aosTimeout);

  // A sleeper due at tick 20000 would deliver the tick-9200 deadline over
  // ten thousand ticks late: the re-inserting sweeper is this link's
  // publisher and must kick, exactly like a fresh arm - a sleeper that
  // scanned before the level-0 bit appeared has no other way to learn of it
  backend.loopStates[2].wakeTick = 20000;
  processTimeoutQueue(&backend.base, nullptr, 8193);
  EXPECT_EQ(opGetStatus(&uncovered.root), aosPending);
  EXPECT_EQ(backend.wakeupCalls, 1u);
  processTimeoutQueue(&backend.base, nullptr, 9201);
  EXPECT_EQ(opGetStatus(&uncovered.root), aosTimeout);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(timer_wakeup, teardown_clears_occupancy)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.root.deadlineTick = 5000;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 1, 5000 >> TIMER_WHEEL_LEVEL_BITS));

  timerWheelTeardown(&backend.base);
  for (unsigned level = 0; level < TIMER_WHEEL_LEVELS; level++) {
    for (unsigned word = 0; word < TIMER_WHEEL_SLOTS / 64; word++)
      EXPECT_EQ(backend.base.timerWheel.occupancy[level][word], 0u);
  }

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wakeup, producer_kicks_only_sleepers_that_would_miss_the_deadline)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object), third(object);

  // A sleeper due to wake at tick 10 would deliver a tick-5 deadline five
  // ticks late: the arm must kick it
  backend.loopStates[2].wakeTick = 10;
  first.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &first.root);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  // A sleeper waking at tick 6 sweeps every tick below it and delivers the
  // tick-5 deadline itself with at most a tick of lag: no kick
  backend.loopStates[2].wakeTick = 6;
  second.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &second.root);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  // Awake threads park their slots and never attract kicks: they re-scan the
  // occupancy bitmap before their next sleep
  backend.loopStates[2].wakeTick = UINTPTR_MAX;
  third.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &third.root);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  objectDecrementReference(&object.root, 3);
  objectDelete(&object.root);
}

TEST(timer_wakeup, prepare_sleep_is_tickless_and_wakes_right_after_nearest_deadline)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp nearOp(object), farOp(object);

  // Empty wheel: the wait is unbounded and the slot
  // carries the eternal sentinel - farther than any tick, so any arm kicks
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0), UINT64_MAX);
  EXPECT_EQ(backend.loopStates[0].wakeTick, timerSleepEternal);
  timerLoopCancelSleep(&backend.loopStates[0]);
  EXPECT_EQ(backend.loopStates[0].wakeTick, UINTPTR_MAX);

  // A faraway deadline sleeps up to the visit of its slot, without polling:
  // tick 5000 parks in level-1 slot 4, whose window [4096, 5120) is visited
  // at tick 4096 (the cascade boundary must not be overslept)
  farOp.root.deadlineTick = 5000;
  addToTimeoutQueue(&backend.base, &farOp.root);
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0), 4097u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 4097u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // A deadline inside the level-0 rotation shrinks the wait to its exact
  // wake tick (deadline + 1: the sweep of a tick runs on the first call
  // past it)
  nearOp.root.deadlineTick = 5;
  addToTimeoutQueue(&backend.base, &nearOp.root);
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0), 6u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 6u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // A wake tick not ahead of the clock is due backlog: no sleep at all
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 6), 6u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // Delivery clears the near slot and the sleep stretches back out to the
  // far link's cascade boundary
  processTimeoutQueue(&backend.base, nullptr, 6);
  EXPECT_EQ(opGetStatus(&nearOp.root), aosTimeout);
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 6), 4097u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 4097u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(timer_wakeup, prepare_sleep_scans_the_full_rotation_in_wrap_around_order)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);

  // A deadline beyond the wheel range clamps into the farthest future
  // top-level slot: index 1023, one position BEHIND the scan's start - only
  // the circular wrap-around pass sees its bit (a horizon-capped scan never
  // could). Its visit is millennia out, so the wait meets the kernel-range
  // clamp; the published horizon matches the actual wake, waking early just
  // re-scans
  op.root.deadlineTick = (((uint64_t)1 << 40) + 5);
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 3, TIMER_WHEEL_SLOTS - 1));
  const uint64_t limitTicks = 0x7FFFFFFFu / 125u;
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0), limitTicks);
  EXPECT_EQ(backend.loopStates[0].wakeTick, limitTicks);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // The link belongs to the wheel: recover it without a 2^40-tick sweep
  asyncOpListLink *link = drainWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 1);
  ASSERT_NE(link, nullptr);
  EXPECT_EQ(link->next, nullptr);
  op.root.timerId = nullptr;
  free(link);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wakeup, prepare_sleep_clamps_far_wakes_to_the_kernel_timeout_range)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);

  // Tick 2^25 parks on level 2 (slot 32); its visit is ~48 days away, past
  // what epoll accepts as an int millisecond timeout. The wait is clamped
  // and the published horizon matches the actual wake
  op.root.deadlineTick = ((uint64_t)1 << 25);
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 2, 32));
  const uint64_t limitTicks = 0x7FFFFFFFu / 125u;
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 0), limitTicks);
  EXPECT_EQ(backend.loopStates[0].wakeTick, limitTicks);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // The link belongs to the wheel: recover it without a 2^25-tick sweep
  asyncOpListLink *link = drainWheelSlot(backend, 2, 32);
  ASSERT_NE(link, nullptr);
  op.root.timerId = nullptr;
  free(link);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wakeup, prepare_sleep_wakes_at_cascade_boundary_of_occupied_upper_slot)
{
  TestBackend backend;
  timerWheelInit(&backend.base, 1022);
  TestObject object(backend);
  TestOp op(object);

  // Distance 1027 selects level 1. Deadline 2049 lives in slot 2 until the
  // tick-2048 visit
  // re-cascades it: a sleeper spanning that boundary must wake right after
  // the boundary tick or the cascade (and the deadline) oversleeps
  op.root.deadlineTick = 2049;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 1, 2));

  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 1022), 2049u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 2049u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  // Past the boundary the link sits re-cascaded in its exact level-0 slot
  // and the next sleep runs up to the deadline itself
  processTimeoutQueue(&backend.base, nullptr, 2049);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_FALSE(wheelSlotOccupied(backend, 1, 2));
  ASSERT_TRUE(wheelSlotOccupied(backend, 0, 2049 % TIMER_WHEEL_SLOTS));
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, &backend.loopStates[0], 2049), 2050u);
  EXPECT_EQ(backend.loopStates[0].wakeTick, 2050u);
  timerLoopCancelSleep(&backend.loopStates[0]);

  processTimeoutQueue(&backend.base, nullptr, 2050);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_realtime, terminal_completion_starts_and_stops_backend_timer)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  op.root.timerId = reinterpret_cast<void*>(1);
  op.setResults({aosSuccess});

  combinerPushOperation(&op.root);

  EXPECT_EQ(backend.startTimerCalls, 1u);
  EXPECT_EQ(backend.stopTimerCalls, 1u);
  backend.drainCompletions();
  objectDelete(&object.root);
}

TEST(timer_realtime, timeout_completion_does_not_stop_fired_timer)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  op.root.timerId = reinterpret_cast<void*>(1);
  op.setResults({aosPending});
  combinerPushOperation(&op.root);

  opCancel(&op.root, opGetGeneration(&op.root), aosTimeout, &object.root, __uint64_atomic_load(&object.root.header.tag.high, amoRelaxed));

  EXPECT_EQ(backend.startTimerCalls, 1u);
  EXPECT_EQ(backend.stopTimerCalls, 0u);
  backend.drainCompletions();
  EXPECT_EQ(op.callbackStatus, aosTimeout);
  objectDelete(&object.root);
}

// Regression for the opEncodeTag removal (kqueue/ident protocol): a stale
// doorbell - harvested into another loop thread's batch, then processed after
// the operation completed, its storage got recycled and the SAME timer got
// re-armed - must not adopt the new arming's generation and win the status
// CAS. The arming identity travels full-width through the kernel as the
// knote ident; a mismatch with the published tag rejects the doorbell.
TEST(timer_reactor, stale_doorbell_must_not_expire_a_rearmed_operation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  makeOperationTimer(timer, backend, op);
  timer.fd = static_cast<intptr_t>(uint64_t{7} << 32); // creation seeds the base id

  // The first incarnation arms its realtime timer; the kernel keeps both the
  // envelope and the ident of this arming inside the harvested event
  uint64_t ident = static_cast<uint64_t>(timer.fd) + 1;
  publishOperationIdent(timer, object.root, opGetGeneration(&op.root), ident);
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t staleIdent = static_cast<uint64_t>(timer.fd);
  uint64_t staleGeneration = 0;
  objectHeader *decoded = kernelHandleDecode(envelope, &staleGeneration);
  ASSERT_EQ(decoded, &timer.header);

  // The operation completes; its storage is recycled and re-armed for a new
  // submission with a fresh deadline before the doorbell gets processed
  uintptr_t firstGeneration = opGetGeneration(&op.root);
  op.root.tag = ((firstGeneration + 1) << TAG_STATUS_SIZE) | aosPending;
  ident++;
  publishOperationIdent(timer, object.root, opGetGeneration(&op.root), ident);
  uint64_t currentEnvelope = kernelHandleEncode(&timer.header);
  uint64_t currentGeneration = 0;
  EXPECT_EQ(kernelHandleDecode(currentEnvelope, &currentGeneration), &timer.header);

  // Another loop thread finally processes the stale doorbell.
  EXPECT_NE(__uint64_atomic_load(&timer.header.tag.low, amoAcquire), staleIdent)
      << "a doorbell carrying the previous arming's ident matched the re-armed timer";
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  // The current arm publishes a coherent cancellation snapshot.
  EXPECT_EQ(__uint64_atomic_load(&timer.header.tag.low, amoAcquire), static_cast<uint64_t>(timer.fd));
  EXPECT_EQ(objectHeaderGeneration(&timer.header), opGetGeneration(&op.root));
  EXPECT_EQ(__pointer_atomic_load((void *volatile*)&timer.operation.object, amoRelaxed), &object.root);
  EXPECT_EQ(__uint64_atomic_load(&timer.operation.objectGeneration, amoRelaxed), objectHeaderGeneration(&object.root.header));

  timerUnpublish(&timer);
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

// Same scenario through the epoll/deadline protocol. A stale envelope cannot
// consume shared fd state: it snapshots the current absolute deadline. Before
// that deadline it is ignored; afterwards expiring the current generation is
// correct regardless of which envelope caused the wakeup.
TEST(timer_reactor, stale_epoll_doorbell_uses_the_current_absolute_deadline)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  makeOperationTimer(timer, backend, op);

  publishOperationDeadline(timer, object.root, 100, opGetGeneration(&op.root));
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t staleGeneration = 0;
  ASSERT_EQ(kernelHandleDecode(envelope, &staleGeneration), &timer.header);

  uintptr_t firstGeneration = opGetGeneration(&op.root);
  op.root.tag = ((firstGeneration + 1) << TAG_STATUS_SIZE) | aosPending;
  publishOperationDeadline(timer, object.root, 200, opGetGeneration(&op.root));
  uint64_t currentEnvelope = kernelHandleEncode(&timer.header);
  uint64_t currentGeneration = 0;
  ASSERT_EQ(kernelHandleDecode(currentEnvelope, &currentGeneration), &timer.header);

  int isStale = !__uint64_atomic_load(&timer.header.tag.low, amoAcquire);
  uint64_t timerGeneration = __uint64_atomic_load(&timer.header.tag.high, amoAcquire);
  isStale |= timerGeneration != staleGeneration;
  uint64_t deadline = __uint64_atomic_load(&timer.operation.deadline, amoRelaxed);
  isStale |= 199 < deadline;
  EXPECT_TRUE(isStale);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  isStale = !__uint64_atomic_load(&timer.header.tag.low, amoAcquire);
  timerGeneration = __uint64_atomic_load(&timer.header.tag.high, amoAcquire);
  isStale |= timerGeneration != currentGeneration;
  deadline = __uint64_atomic_load(&timer.operation.deadline, amoRelaxed);
  isStale |= 200 < deadline;
  EXPECT_FALSE(isStale);
  EXPECT_EQ(timerGeneration, opGetGeneration(&op.root));

  timerUnpublish(&timer);
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

TEST(timer_reactor, side_snapshot_rejects_fields_from_a_newer_arm)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{};
  initializeTimer(timer, &backend.base);
  timer.header.timer.kind = tkOperation;
  uint64_t ident = (uint64_t{9} << 32) + 1;
  publishOperationIdent(timer, object.root, 7, ident);

  // Pause after the first ident check, then publish a complete new arm.
  uint64_t eventIdent = static_cast<uint64_t>(timer.fd);
  int isStale = __uint64_atomic_load(&timer.header.tag.low, amoAcquire) != eventIdent;
  ASSERT_FALSE(isStale);
  ident++;
  publishOperationIdent(timer, object.root, 8, ident);

  uint64_t generation = __uint64_atomic_load(&timer.header.tag.high, amoAcquire);
  isStale |= __uint64_atomic_load(&timer.header.tag.low, amoRelaxed) != eventIdent;
  EXPECT_EQ(generation, 8u);
  EXPECT_TRUE(isStale);
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

// User events restart with an UNCHANGED operation generation, so the arming
// identity - not the generation - must distinguish restarts (the old
// "compare timer and event tag" TODO). kqueue/ident protocol: every arming
// takes a fresh ident, a doorbell of the previous one no longer matches.
TEST(timer_reactor, stale_doorbell_must_not_activate_a_restarted_user_event)
{
  aioUserEvent event{};
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  makeEventTimerFixture(event, timer, (UINT64_C(1) << 40) | 5);

  uint64_t ident = (uint64_t{2} << 32) + 1;
  publishEventIdent(timer, event, 1, ident);
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t staleIdent = static_cast<uint64_t>(timer.fd);
  uint64_t staleGeneration = 0;
  ASSERT_EQ(kernelHandleDecode(envelope, &staleGeneration), &timer.header);

  timerUnpublish(&timer); // userEventStopTimer
  ident++;
  publishEventIdent(timer, event, 2, ident);
  uint64_t currentEnvelope = kernelHandleEncode(&timer.header);
  uint64_t currentGeneration = 0;
  ASSERT_EQ(kernelHandleDecode(currentEnvelope, &currentGeneration), &timer.header);

  EXPECT_NE(__uint64_atomic_load(&timer.header.tag.low, amoAcquire), staleIdent)
      << "a doorbell of the previous arming would activate the restarted user event and consume its tick";

  EXPECT_EQ(__uint64_atomic_load(&timer.header.tag.low, amoAcquire), static_cast<uint64_t>(timer.fd));
  EXPECT_EQ(objectHeaderGeneration(&timer.header), 2u);
  EXPECT_EQ(__uint64_atomic_load(&timer.event.generation, amoAcquire), (UINT64_C(1) << 40) | 5u);
}

// Same restart through the epoll/timerfd protocol: a loop delivery snapshots
// only the current schedule. The owner later reads timerfd under its serial
// ownership; eventtest covers the EAGAIN/confirmed-expiration decisions.
TEST(timer_reactor, count_handle_snapshots_the_restarted_user_event)
{
  aioUserEvent event{};
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  makeEventTimerFixture(event, timer, (UINT64_C(1) << 40) | 9);

  publishEventSchedule(timer, event, 1);
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t staleGeneration = 0;
  EXPECT_EQ(kernelHandleDecode(envelope, &staleGeneration), &timer.header);

  timerUnpublish(&timer); // userEventStopTimer
  publishEventSchedule(timer, event, 2);
  uint64_t currentEnvelope = kernelHandleEncode(&timer.header);
  uint64_t currentGeneration = 0;
  EXPECT_EQ(kernelHandleDecode(currentEnvelope, &currentGeneration), &timer.header);

  uint64_t timerGeneration = __uint64_atomic_load(&timer.header.tag.high, amoAcquire);
  int isStale = timerGeneration != staleGeneration;
  uint64_t eventGeneration = __uint64_atomic_load(&timer.event.generation, amoAcquire);
  isStale |= !__uint64_atomic_load(&timer.header.tag.low, amoAcquire);
  isStale |= objectHeaderGeneration(&timer.header) != timerGeneration;
  EXPECT_TRUE(isStale);

  timerGeneration = __uint64_atomic_load(&timer.header.tag.high, amoAcquire);
  isStale = timerGeneration != currentGeneration;
  eventGeneration = __uint64_atomic_load(&timer.event.generation, amoAcquire);
  isStale |= !__uint64_atomic_load(&timer.header.tag.low, amoAcquire);
  isStale |= objectHeaderGeneration(&timer.header) != timerGeneration;
  ASSERT_FALSE(isStale);
  EXPECT_EQ(eventGeneration, (UINT64_C(1) << 40) | 9u);
  EXPECT_EQ(timerGeneration, 2u);
}

// The "tag == 0 means unpublished" sentinel must not collide with a legally
// wrapped generation. On a 32-bit target the generation field is 24 bits
// wide: after 2^24 recycles of one operation slot initAsyncOpRoot wraps it
// to zero - the armed encoding must stay disjoint from the sentinel anyway,
// or every delivery reads "stale doorbell" and the timeout is dropped
// forever (the operation hangs). kqueue: the ident's seq part skips 0;
// epoll: the armed bit in (generation << 1) | 1.
TEST(timer_reactor, arming_a_wrapped_generation_must_not_publish_the_unpublished_sentinel)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  makeOperationTimer(timer, backend, op);
  timer.fd = 0; // even a zero base id must not let the composite ident hit 0

  // The slot's generation legally wraps around to zero (2^24 recycles on a
  // 32-bit target)
  op.root.tag = (uintptr_t{0} << TAG_STATUS_SIZE) | aosPending;
  uint64_t ident = 1;
  publishOperationIdent(timer, object.root, opGetGeneration(&op.root), ident);
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t envelopeGeneration = ~uint64_t{0};
  EXPECT_EQ(kernelHandleDecode(envelope, &envelopeGeneration), &timer.header);

  EXPECT_NE(static_cast<uint64_t>(timer.fd), 0u);
  EXPECT_EQ(__uint64_atomic_load(&timer.header.tag.low, amoAcquire), static_cast<uint64_t>(timer.fd))
      << "an armed timer with generation 0 is indistinguishable from an unpublished one: its timeout is lost";
  EXPECT_EQ(objectHeaderGeneration(&timer.header), 0u);
  timerUnpublish(&timer);

  // epoll flavor: the armed bit keeps generation 0 away from the sentinel.
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer deadlineTimer{};
  makeOperationTimer(deadlineTimer, backend, op);
  publishOperationDeadline(deadlineTimer, object.root, 10, 0);
  uint64_t deadlineEnvelope = kernelHandleEncode(&deadlineTimer.header);
  envelopeGeneration = ~uint64_t{0};
  EXPECT_EQ(kernelHandleDecode(deadlineEnvelope, &envelopeGeneration), &deadlineTimer.header);
  uint64_t publication = __uint64_atomic_load(&deadlineTimer.header.tag.low, amoAcquire);
  EXPECT_NE(publication, 0u) << "the armed deadline encoding of generation 0 collided with the unpublished sentinel";
  int isStale = !publication;
  uint64_t timerGeneration = __uint64_atomic_load(&deadlineTimer.header.tag.high, amoAcquire);
  isStale |= timerGeneration != envelopeGeneration;
  uint64_t deadline = __uint64_atomic_load(&deadlineTimer.operation.deadline, amoRelaxed);
  isStale |= 10 < deadline;
  EXPECT_FALSE(isStale);
  EXPECT_EQ(timerGeneration, 0u);
  timerUnpublish(&deadlineTimer);

  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

#ifdef __linux__
// The kernel dependency the probe/owner protocol stands on: timerfd_settime
// with a new value RESETS the fd's expiration counter, so the owner reading
// after a restart gets EAGAIN instead of inheriting the previous schedule's
// expiration. If this ever changes, a stale probe could spend a new counter.
TEST(timer_reactor, timerfd_settime_resets_the_expiration_counter)
{
  int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  ASSERT_NE(fd, -1);
  itimerspec its{};
  its.it_value.tv_nsec = 1000000; // 1ms
  ASSERT_EQ(timerfd_settime(fd, 0, &its, nullptr), 0);
  pollfd waiter{fd, POLLIN, 0};
  ASSERT_EQ(poll(&waiter, 1, 1000), 1) << "the armed timerfd never expired";

  // Re-arm with a distant deadline WITHOUT reading the expired count first -
  // exactly what a recycle+rearm does while a stale doorbell sits in a batch
  its.it_value.tv_sec = 10;
  its.it_value.tv_nsec = 0;
  ASSERT_EQ(timerfd_settime(fd, 0, &its, nullptr), 0);
  uint64_t expirations = 0;
  ssize_t result = read(fd, &expirations, sizeof(expirations));
  EXPECT_TRUE(result == -1 && errno == EAGAIN) << "settime did not reset the expiration counter: read returned " << result << " with count "
                                               << expirations << " - a stale doorbell would expire the fresh arming";

  // And a genuine expiration of the current arming still reads back
  its.it_value.tv_sec = 0;
  its.it_value.tv_nsec = 1000000;
  ASSERT_EQ(timerfd_settime(fd, 0, &its, nullptr), 0);
  ASSERT_EQ(poll(&waiter, 1, 1000), 1);
  result = read(fd, &expirations, sizeof(expirations));
  EXPECT_EQ(result, (ssize_t)sizeof(expirations));
  EXPECT_GT(expirations, 0u);
  close(fd);
}
#endif

// Every timer publication uses the same compact objectHeader envelope as
// objects and user events. The immutable header type, rather than pointer tag
// bits, selects the timer decoder after the address/generation expansion.
TEST(timer_reactor, every_timer_envelope_is_a_common_header_handle)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  initializeTimer(timer, &backend.base);
  timer.operation.op = &op.root;
  op.root.timerId = &timer;

  auto generationOf = [](uint64_t envelope, aioTimer *expected) {
    uint64_t generation = 0;
    objectHeader *header = kernelHandleDecode(envelope, &generation);
    EXPECT_EQ(header, &expected->header);
    EXPECT_EQ(objectHeaderGetType(header), ohtTimer);
    return generation;
  };

  // Kind is bound only at the first arm; timer construction itself does not
  // read the operation's not-yet-initialized opCode.
  timer.header.timer.kind = tkOperation;
  EXPECT_EQ(generationOf(kernelHandleEncode(&timer.header), &timer), 0u);

  // Both operation arm flavors preserve the operation generation.
  publishOperationIdent(timer, object.root, opGetGeneration(&op.root), 1);
  EXPECT_EQ(generationOf(kernelHandleEncode(&timer.header), &timer), opGetGeneration(&op.root));
  timerUnpublish(&timer);
  publishOperationDeadline(timer, object.root, 1, opGetGeneration(&op.root));
  EXPECT_EQ(generationOf(kernelHandleEncode(&timer.header), &timer), opGetGeneration(&op.root));
  timerUnpublish(&timer);

  // User-event timer kind lives in the common-header union and the schedule
  // generation follows the same encoding.
  aioUserEvent event{};
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer eventTimer{}; // production timers come from alignedMalloc
  makeEventTimerFixture(event, eventTimer, 1);
  publishEventIdent(eventTimer, event, 1, 1);
  EXPECT_EQ(generationOf(kernelHandleEncode(&eventTimer.header), &eventTimer), 1u);
  timerUnpublish(&eventTimer);
  publishEventSchedule(eventTimer, event, 2);
  EXPECT_EQ(generationOf(kernelHandleEncode(&eventTimer.header), &eventTimer), 2u);
  timerUnpublish(&eventTimer);

  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

} // namespace
