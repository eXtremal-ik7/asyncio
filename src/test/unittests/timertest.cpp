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

TEST(timer_grid, rounds_deadlines_up_and_delivers_each_window_once)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object), third(object);
  // 125 ms ticks: 1000001 us rounds up to tick 9, both others to tick 16
  first.root.endTime = 1000001;
  second.root.endTime = 1999999;
  third.root.endTime = 2000000;

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
  processTimeoutQueue(&backend.base, 10);
  EXPECT_EQ(opGetStatus(&first.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&second.root), aosPending);
  processTimeoutQueue(&backend.base, 10);
  EXPECT_EQ(opGetStatus(&second.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 0, 9).low, 0u);

  processTimeoutQueue(&backend.base, 17);
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
  op.root.endTime = 100 * TIMER_TICK_MICROSECONDS;
  eqPushBack(&object.root.readQueue, &op.root);
  addToTimeoutQueue(&backend.base, &op.root);

  processTimeoutQueue(&backend.base, 101);

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
  op.root.endTime = 100 * TIMER_TICK_MICROSECONDS;
  eqPushBack(&object.root.readQueue, &op.root);

  processTimeoutQueue(&backend.base, 101);
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

  processTimeoutQueue(&backend.base, 10);
  EXPECT_EQ(backend.base.timerCloseCursor, 10u);

  // A caller whose clock reading is behind the confirmed cursor (it raced a
  // faster sweeper) must not sweep anything or move the cursor backwards
  processTimeoutQueue(&backend.base, 8);
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
  // Heap link: the sweep recycles it into the global link pool, so a stack
  // instance would leave a dangling pointer for a later addToTimeoutQueue
  auto *link = static_cast<asyncOpListLink*>(malloc(sizeof(asyncOpListLink)));
  link->op = &op.root;
  link->generation = staleGeneration;
  link->object = &object.root;
  link->objectGeneration = __uint64_atomic_load(&object.root.header.tag.high, amoRelaxed);
  link->next = nullptr;
  link->deadlineTick = 7;
  timerWheelInsert(&backend.base, link, 7);

  processTimeoutQueue(&backend.base, 8);

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
  EXPECT_GT(op.root.endTime, 0u);
  EXPECT_EQ(backend.startTimerCalls, 0u);

  cancelIo(&object.root);
  backend.drainCompletions();
  // The armed link stays parked in the wheel with a now-stale generation; the
  // TestBackend teardown recycles it without touching this stack operation
  objectDelete(&object.root);
}

TEST(timer_wheel, detach_reopens_and_is_idempotent_per_incarnation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  auto *link = static_cast<asyncOpListLink*>(op.root.timerId);
  ASSERT_NE(link, nullptr);
  ASSERT_EQ(link->deadlineTick, 5u);

  // The winner takes the chain, and the same CAS reopens the slot for the
  // next rotation
  asyncOpListLink *chain = timerWheelDetach(&backend.base, 0, 5);
  ASSERT_EQ(chain, link);
  uint128 slotPair = peekWheelSlot(backend, 0, 5);
  EXPECT_EQ(slotPair.low, 0u);
  EXPECT_EQ(slotPair.high, 5u + TIMER_WHEEL_SLOTS);

  // Any other visitor of the same window observes the advanced baseTick:
  // no chain, no second reopen
  EXPECT_EQ(timerWheelDetach(&backend.base, 0, 5), nullptr);
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

TEST(timer_wheel, arm_behind_multiple_rotations_expires_without_publication)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // The producer was preempted between reading the cursor and publishing for
  // two whole rotations of the deadline's slot: the slot alone proves the
  // window is long gone, whatever the stale cursor hint says
  EXPECT_EQ(timerWheelDetach(&backend.base, 0, 5), nullptr);
  EXPECT_EQ(timerWheelDetach(&backend.base, 0, 5 + TIMER_WHEEL_SLOTS), nullptr);
  op.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
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
  processTimeoutQueue(&backend.base, 1025);
  ASSERT_EQ(backend.base.timerCloseCursor, 1025u);

  // A producer with a stale cursor hint routes to the drained level-1 window
  // first; the publication must descend into the still-open level-0 slot -
  // not be refused and not park a rotation late in level 1
  auto *link = static_cast<asyncOpListLink*>(malloc(sizeof(asyncOpListLink)));
  link->op = &op.root;
  link->generation = opGetGeneration(&op.root);
  link->object = &object.root;
  link->objectGeneration = __uint64_atomic_load(&object.root.header.tag.high, amoRelaxed);
  link->next = nullptr;
  link->deadlineTick = 1030;
  ASSERT_EQ(timerWheelInsert(&backend.base, link, 0), 1);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).low, reinterpret_cast<uint64_t>(link));
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);

  // Delivered by the normal sweep in its exact tick
  processTimeoutQueue(&backend.base, 1030);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  processTimeoutQueue(&backend.base, 1031);
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
  stalledOp.root.endTime = 100 * TIMER_TICK_MICROSECONDS;
  liveOp.root.endTime = 101 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &stalledOp.root);
  addToTimeoutQueue(&backend.base, &liveOp.root);

  // Sweeper A wins the tick-100 visit and stalls before both the confirm CAS
  // and the processing of its detached chain
  asyncOpListLink *ownedChain = timerWheelDetach(&backend.base, 0, 100);
  ASSERT_EQ(ownedChain, static_cast<asyncOpListLink*>(stalledOp.root.timerId));
  ASSERT_EQ(backend.base.timerCloseCursor, 100u);

  // Sweeper B's tick-100 visits are idempotent no-ops, yet B confirms the
  // cursor on A's behalf and the ticks behind the stalled one stay alive
  processTimeoutQueue(&backend.base, 102);
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

  processTimeoutQueue(&backend.base, 110);
  ASSERT_EQ(backend.base.timerCloseCursor, 110u);

  // A sweeper that stalled before its confirm CAS resurfaces long after its
  // tick was confirmed by helpers: the visits are idempotent no-ops and the
  // tick-exact confirm CAS misses, a plain checkpoint store would rewind here
  timerWheelSweepTick(&backend.base, 100);
  EXPECT_EQ(backend.base.timerCloseCursor, 110u);

  processTimeoutQueue(&backend.base, 105);
  EXPECT_EQ(backend.base.timerCloseCursor, 110u);
}

TEST(timer_wheel, boundary_deadline_migrates_and_fires_in_its_exact_tick)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp boundary(object), inWindow(object);
  // Against cursor 0 both deadlines differ in the bits 10..19 group, so both
  // are routed to level 1 (the [1024, 2048) window slot)
  boundary.root.endTime = 1024 * TIMER_TICK_MICROSECONDS;
  inWindow.root.endTime = 1025 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &boundary.root);
  addToTimeoutQueue(&backend.base, &inWindow.root);
  ASSERT_EQ(peekWheelSlot(backend, 0, 0).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 0, 1).low, 0u);
  ASSERT_NE(peekWheelSlot(backend, 1, 1).low, 0u);

  // The whole level-0 rotation before the boundary delivers nothing
  processTimeoutQueue(&backend.base, 1024);
  EXPECT_EQ(opGetStatus(&boundary.root), aosPending);
  EXPECT_EQ(opGetStatus(&inWindow.root), aosPending);

  // Tick 1024 visits level 1 before level 0: the boundary deadline expires in
  // this very tick (not one rotation later), the in-window one migrates down
  // into its level-0 slot
  processTimeoutQueue(&backend.base, 1025);
  EXPECT_EQ(opGetStatus(&boundary.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&inWindow.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).low, reinterpret_cast<uint64_t>(inWindow.root.timerId));

  processTimeoutQueue(&backend.base, 1026);
  EXPECT_EQ(opGetStatus(&inWindow.root), aosTimeout);

  objectDecrementReference(&object.root, 2);
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
  boundary.root.endTime = l2Window * TIMER_TICK_MICROSECONDS;
  chained.root.endTime = chainedDeadline * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &boundary.root);
  addToTimeoutQueue(&backend.base, &chained.root);
  ASSERT_EQ(peekWheelSlot(backend, 2, 1).low, reinterpret_cast<uint64_t>(chained.root.timerId));
  ASSERT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);

  // Catching up a whole level-1 rotation in one call (a base resuming from
  // suspend) delivers nothing ahead of time
  processTimeoutQueue(&backend.base, l2Window);
  EXPECT_EQ(opGetStatus(&boundary.root), aosPending);
  EXPECT_EQ(opGetStatus(&chained.root), aosPending);

  // Tick 2^20 visits level 2: the boundary deadline expires in this very tick
  // (not one rotation later), the chained one migrates into the level-1
  // window reopened by the sweep in exact lockstep
  processTimeoutQueue(&backend.base, l2Window + 1);
  EXPECT_EQ(opGetStatus(&boundary.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&chained.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 2, 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 1, 1).low, reinterpret_cast<uint64_t>(chained.root.timerId));
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).high, l2Window + TIMER_WHEEL_SLOTS);

  // The level-1 boundary tick migrates it again, into the level-0 slot whose
  // current incarnation covers exactly the deadline tick
  processTimeoutQueue(&backend.base, chainedDeadline);
  EXPECT_EQ(opGetStatus(&chained.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 0, 1).low, reinterpret_cast<uint64_t>(chained.root.timerId));
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).high, chainedDeadline);

  // The last hop is the delivery itself, in the exact deadline tick
  processTimeoutQueue(&backend.base, chainedDeadline + 1);
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
  op.root.endTime = l3Window * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_EQ(peekWheelSlot(backend, 3, 1).low, reinterpret_cast<uint64_t>(op.root.timerId));

  // Sweeping 2^30 ticks one by one is out of unit-test reach; the visit units
  // run directly at the exact window-start tick, as the contiguous sweep
  // would. The window-start deadline is due at this very visit and must be
  // delivered here, not re-parked for another rotation
  timerWheelProcessDetached(&backend.base, timerWheelDetach(&backend.base, 3, l3Window), l3Window);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 3, 1).low, 0u);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, beyond_range_deadline_clamps_and_reclamps_keeping_its_exact_tick)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // The deadline differs from cursor 0 above the wheel's addressable bits: no
  // slot covers it yet, the arm clamps into the farthest top-level slot
  const uint64_t wheelRange = uint64_t{1} << (TIMER_WHEEL_LEVEL_BITS * TIMER_WHEEL_LEVELS);
  const uint64_t topWidth = uint64_t{1} << (3 * TIMER_WHEEL_LEVEL_BITS);
  op.root.endTime = (wheelRange + 5) * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  auto *link = static_cast<asyncOpListLink*>(op.root.timerId);
  ASSERT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 1).low, reinterpret_cast<uint64_t>(link));

  // The far slot's visit re-cascades; from this window's position the
  // deadline is still beyond range, so it clamps again - parked with its
  // exact deadline tick intact, neither delivered early nor dropped
  const uint64_t farWindow = (TIMER_WHEEL_SLOTS - 1) * topWidth;
  timerWheelProcessDetached(&backend.base, timerWheelDetach(&backend.base, 3, farWindow), farWindow);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 2).low, reinterpret_cast<uint64_t>(link));
  EXPECT_EQ(link->deadlineTick, wheelRange + 5);

  // Still pending and parked: the wheel teardown recycles the link without
  // delivering it
  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(timer_wheel, stalled_cascade_into_swept_window_delivers_exactly_once)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // Level-1 routing against cursor 0: window [1024, 2048), deadline inside it
  op.root.endTime = 1030 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  auto *link = static_cast<asyncOpListLink*>(op.root.timerId);
  ASSERT_EQ(peekWheelSlot(backend, 1, 1).low, reinterpret_cast<uint64_t>(link));

  processTimeoutQueue(&backend.base, 1024);

  // The owner wins the level-1 boundary visit and stalls before processing
  // its chain; helpers sweep past the deadline meanwhile, so the level-0 slot
  // the cascade is aiming at is already reopened for its next rotation
  asyncOpListLink *ownedChain = timerWheelDetach(&backend.base, 1, 1024);
  ASSERT_EQ(ownedChain, link);
  processTimeoutQueue(&backend.base, 1031);
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
  op.root.endTime = 2053 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 0, 5));

  // Simulate the lost race: the stale visitor's pre-clear landing after the
  // fresh publication
  __uintptr_atomic_fetch_and(&backend.base.timerWheel.occupancy[0][0], ~(static_cast<uintptr_t>(1) << 5), amoSeqCst);

  // The stale visitor re-reads the pair, sees the advanced incarnation with
  // a live chain and restores the bit while claiming nothing
  EXPECT_EQ(timerWheelDetach(&backend.base, 0, 2053 - TIMER_WHEEL_SLOTS), nullptr);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5));
  EXPECT_NE(peekWheelSlot(backend, 0, 5).low, 0u);

  processTimeoutQueue(&backend.base, 2054);
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

  // Activating an empty slot sets its bit; stacking onto a live chain rides
  // on the bit already set (the same slot means the same wake tick).
  delivered.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &delivered.root);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5));
  stacked.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &stacked.root);
  EXPECT_TRUE(wheelSlotOccupied(backend, 0, 5));

  // The visit clears the bit before draining the chain
  processTimeoutQueue(&backend.base, 6);
  EXPECT_EQ(opGetStatus(&delivered.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&stacked.root), aosTimeout);
  EXPECT_FALSE(wheelSlotOccupied(backend, 0, 5));

  // A refused late arm never becomes visible: no bit and no kick, the
  // timeout is delivered by the arm itself
  late.root.endTime = 3 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &late.root);
  EXPECT_FALSE(wheelSlotOccupied(backend, 0, 3));
  EXPECT_EQ(opGetStatus(&late.root), aosTimeout);
  EXPECT_EQ(backend.wakeupCalls, 0u);

  objectDecrementReference(&object.root, 3);
  objectDelete(&object.root);
}

TEST(timer_wakeup, teardown_clears_occupancy)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.root.endTime = 5000 * TIMER_TICK_MICROSECONDS;
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
  backend.sleepSlots[2].wakeTick = 10;
  first.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &first.root);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  // A sleeper waking at tick 6 sweeps every tick below it and delivers the
  // tick-5 deadline itself with at most a tick of lag: no kick
  backend.sleepSlots[2].wakeTick = 6;
  second.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &second.root);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  // Awake threads park their slots and never attract kicks: they re-scan the
  // occupancy bitmap before their next sleep
  backend.sleepSlots[2].wakeTick = UINTPTR_MAX;
  third.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
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
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 0, 1000), UINT32_MAX);
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, timerSleepEternal);
  timerLoopCancelSleep(&backend.base, 0);
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, UINTPTR_MAX);

  // A faraway deadline sleeps up to the visit of its slot, not the fallback:
  // tick 5000 parks in level-1 slot 4, whose window [4096, 5120) is visited
  // at tick 4096 (the cascade boundary must not be overslept)
  farOp.root.endTime = 5000 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &farOp.root);
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 0, 1000), 4097u * 125u);
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, 4097u);
  timerLoopCancelSleep(&backend.base, 0);

  // A deadline inside the level-0 rotation shrinks the wait to its exact
  // wake tick (deadline + 1: the sweep of a tick runs on the first call
  // past it)
  nearOp.root.endTime = 5 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &nearOp.root);
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 0, 1000), 750u);
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, 6u);
  timerLoopCancelSleep(&backend.base, 0);

  // A wake tick not ahead of the clock is due backlog: no sleep at all
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 6, 1000), 0u);
  timerLoopCancelSleep(&backend.base, 0);

  // Delivery clears the near slot and the sleep stretches back out to the
  // far link's cascade boundary
  processTimeoutQueue(&backend.base, 6);
  EXPECT_EQ(opGetStatus(&nearOp.root), aosTimeout);
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 6, 1000), (4097u - 6u) * 125u);
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, 4097u);
  timerLoopCancelSleep(&backend.base, 0);

  // A thread beyond the horizon array has no channel for the kick: its wait
  // is capped at one grid tick instead (out-of-contract degradation)
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 100, 6, 1000), 125u);

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
  op.root.endTime = (((uint64_t)1 << 40) + 5) * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 3, TIMER_WHEEL_SLOTS - 1));
  const uint64_t limitTicks = 0x7FFFFFFFu / 125u;
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 0, 1000), (uint32_t)(limitTicks * 125u));
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, limitTicks);
  timerLoopCancelSleep(&backend.base, 0);

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
  op.root.endTime = ((uint64_t)1 << 25) * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 2, 32));
  const uint64_t limitTicks = 0x7FFFFFFFu / 125u;
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 0, 1000), (uint32_t)(limitTicks * 125u));
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, limitTicks);
  timerLoopCancelSleep(&backend.base, 0);

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

  // Deadline 1029 lives in level-1 slot 1 until the tick-1024 visit
  // re-cascades it: a sleeper spanning that boundary must wake right after
  // the boundary tick or the cascade (and the deadline) oversleeps
  op.root.endTime = 1029 * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_TRUE(wheelSlotOccupied(backend, 1, 1));

  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 1022, 1000), 375u);
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, 1025u);
  timerLoopCancelSleep(&backend.base, 0);

  // Past the boundary the link sits re-cascaded in its exact level-0 slot
  // and the next sleep runs up to the deadline itself
  processTimeoutQueue(&backend.base, 1025);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_FALSE(wheelSlotOccupied(backend, 1, 1));
  ASSERT_TRUE(wheelSlotOccupied(backend, 0, 1029 % TIMER_WHEEL_SLOTS));
  EXPECT_EQ(timerLoopPrepareSleep(&backend.base, 0, 1025, 1000), 625u);
  EXPECT_EQ(backend.sleepSlots[0].wakeTick, 1030u);
  timerLoopCancelSleep(&backend.base, 0);

  processTimeoutQueue(&backend.base, 1030);
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
  initializeTimer(timer, &backend.base);
  timer.operation.op = &op.root;
  timer.fd = static_cast<intptr_t>(uint64_t{7} << 32); // creation seeds the base id
  op.root.timerId = &timer;
  timer.header.timer.kind = tkOperation;

  // The first incarnation arms its realtime timer; the kernel keeps both the
  // envelope and the ident of this arming inside the harvested event
  uint64_t ident = static_cast<uint64_t>(timer.fd) + 1;
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, opGetGeneration(&op.root), ident);
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
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, opGetGeneration(&op.root), ident);
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
  initializeTimer(timer, &backend.base);
  timer.operation.op = &op.root;
  op.root.timerId = &timer;
  timer.header.timer.kind = tkOperation;

  timerPublishBegin(&timer);
    __uint64_atomic_store(&timer.operation.deadline, 100, amoRelaxed);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, opGetGeneration(&op.root), 1);
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t staleGeneration = 0;
  ASSERT_EQ(kernelHandleDecode(envelope, &staleGeneration), &timer.header);

  uintptr_t firstGeneration = opGetGeneration(&op.root);
  op.root.tag = ((firstGeneration + 1) << TAG_STATUS_SIZE) | aosPending;
  timerPublishBegin(&timer);
    __uint64_atomic_store(&timer.operation.deadline, 200, amoRelaxed);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, opGetGeneration(&op.root), 1);
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
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, 7, ident);

  // Pause after the first ident check, then publish a complete new arm.
  uint64_t eventIdent = static_cast<uint64_t>(timer.fd);
  int isStale = __uint64_atomic_load(&timer.header.tag.low, amoAcquire) != eventIdent;
  ASSERT_FALSE(isStale);
  ident++;
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, 8, ident);

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
  __uint64_atomic_store(&event.header.tag.low, 1, amoRelaxed);
  __uint64_atomic_store(&event.header.tag.high, (UINT64_C(1) << 40) | 5, amoRelaxed);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  event.timer = &timer;
  initializeTimer(timer);
  timer.event.userEvent = &event;
  timer.header.timer.kind = tkUserEvent;

  uint64_t ident = (uint64_t{2} << 32) + 1;
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __uint64_atomic_store(&timer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&timer, 1, ident);
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t staleIdent = static_cast<uint64_t>(timer.fd);
  uint64_t staleGeneration = 0;
  ASSERT_EQ(kernelHandleDecode(envelope, &staleGeneration), &timer.header);

  timerUnpublish(&timer); // userEventStopTimer
  ident++;
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __uint64_atomic_store(&timer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&timer, 2, ident);
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
  __uint64_atomic_store(&event.header.tag.low, 1, amoRelaxed);
  __uint64_atomic_store(&event.header.tag.high, (UINT64_C(1) << 40) | 9, amoRelaxed);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{}; // production timers come from alignedMalloc
  event.timer = &timer;
  initializeTimer(timer);
  timer.event.userEvent = &event;
  timer.header.timer.kind = tkUserEvent;

  timerPublishBegin(&timer);
    __uint64_atomic_store(&timer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&timer, 1, 1);
  uint64_t envelope = kernelHandleEncode(&timer.header);
  uint64_t staleGeneration = 0;
  EXPECT_EQ(kernelHandleDecode(envelope, &staleGeneration), &timer.header);

  timerUnpublish(&timer); // userEventStopTimer
  timerPublishBegin(&timer);
    __uint64_atomic_store(&timer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&timer, 2, 1);
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
  initializeTimer(timer, &backend.base);
  timer.operation.op = &op.root;
  timer.fd = 0; // even a zero base id must not let the composite ident hit 0
  op.root.timerId = &timer;
  timer.header.timer.kind = tkOperation;

  // The slot's generation legally wraps around to zero (2^24 recycles on a
  // 32-bit target)
  op.root.tag = (uintptr_t{0} << TAG_STATUS_SIZE) | aosPending;
  uint64_t ident = 1;
  timerPublishBegin(&timer);
    timer.fd = static_cast<intptr_t>(ident);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, opGetGeneration(&op.root), ident);
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
  initializeTimer(deadlineTimer, &backend.base);
  deadlineTimer.operation.op = &op.root;
  op.root.timerId = &deadlineTimer;
  deadlineTimer.header.timer.kind = tkOperation;
  timerPublishBegin(&deadlineTimer);
    __uint64_atomic_store(&deadlineTimer.operation.deadline, 10, amoRelaxed);
    __pointer_atomic_store((void *volatile*)&deadlineTimer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&deadlineTimer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&deadlineTimer, 0, 1);
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
  timerPublishBegin(&timer);
    timer.fd = 1;
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, opGetGeneration(&op.root), 1);
  EXPECT_EQ(generationOf(kernelHandleEncode(&timer.header), &timer), opGetGeneration(&op.root));
  timerUnpublish(&timer);
  timerPublishBegin(&timer);
    __uint64_atomic_store(&timer.operation.deadline, 1, amoRelaxed);
    __pointer_atomic_store((void *volatile*)&timer.operation.object, &object.root, amoRelaxed);
    __uint64_atomic_store(&timer.operation.objectGeneration, objectHeaderGeneration(&object.root.header), amoRelaxed);
  timerPublishEnd(&timer, opGetGeneration(&op.root), 1);
  EXPECT_EQ(generationOf(kernelHandleEncode(&timer.header), &timer), opGetGeneration(&op.root));
  timerUnpublish(&timer);

  // User-event timer kind lives in the common-header union and the schedule
  // generation follows the same encoding.
  aioUserEvent event{};
  __uint64_atomic_store(&event.header.tag.low, 1, amoRelaxed);
  __uint64_atomic_store(&event.header.tag.high, 1, amoRelaxed);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer eventTimer{}; // production timers come from alignedMalloc
  event.timer = &eventTimer;
  initializeTimer(eventTimer);
  eventTimer.event.userEvent = &event;
  eventTimer.header.timer.kind = tkUserEvent;
  timerPublishBegin(&eventTimer);
    eventTimer.fd = 1;
    __uint64_atomic_store(&eventTimer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&eventTimer, 1, 1);
  EXPECT_EQ(generationOf(kernelHandleEncode(&eventTimer.header), &eventTimer), 1u);
  timerUnpublish(&eventTimer);
  timerPublishBegin(&eventTimer);
    __uint64_atomic_store(&eventTimer.event.generation, eventHandleGeneration(&event), amoRelease);
  timerPublishEnd(&eventTimer, 2, 1);
  EXPECT_EQ(generationOf(kernelHandleEncode(&eventTimer.header), &eventTimer), 2u);
  timerUnpublish(&eventTimer);

  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

} // namespace
