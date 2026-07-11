#include "coretest.h"

#include "reactorTimer.h"

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <thread>

#ifdef __linux__
#include <sys/timerfd.h>
#include <poll.h>
#include <unistd.h>
#include <cerrno>
#endif

namespace {

static_assert(COMBINER_TAG_PROGRESS_READ == IO_EVENT_READ, "READ tag must map directly to backend events");
static_assert(COMBINER_TAG_PROGRESS_WRITE == IO_EVENT_WRITE, "WRITE tag must map directly to backend events");
static_assert(COMBINER_TAG_ERROR == IO_EVENT_ERROR, "ERROR tag must map directly to backend events");
static_assert(COMBINER_TAG_CANCEL == (1u << 3), "CANCEL must precede DELETE");
static_assert(COMBINER_TAG_DELETE == (1u << 4), "DELETE must remain the final Head tag");

void cancelAndDrain(TestBackend &backend, TestObject &object)
{
  cancelIo(&object.root);
  backend.drainCompletions();
}

void deleteOwner(TestBackend &backend, TestObject &object)
{
  objectDelete(&object.root);
  backend.drainCompletions();
}

TEST(core_decision, active_io_event_selection_covers_complete_boolean_table)
{
  for (int hasInitialization = 0; hasInitialization <= 1; ++hasInitialization) {
    for (int running = arWaiting; running <= arCancelling; ++running) {
      for (int initializationIsWrite = 0; initializationIsWrite <= 1; ++initializationIsWrite) {
        for (int hasReadQueue = 0; hasReadQueue <= 1; ++hasReadQueue) {
          for (int hasWriteQueue = 0; hasWriteQueue <= 1; ++hasWriteQueue) {
            SCOPED_TRACE(::testing::Message()
                         << "initialization=" << hasInitialization
                         << " running=" << running
                         << " initWrite=" << initializationIsWrite
                         << " readQueue=" << hasReadQueue
                         << " writeQueue=" << hasWriteQueue);
            uint32_t expected = hasInitialization
              ? (running == arRunning
                   ? (initializationIsWrite ? IO_EVENT_WRITE : IO_EVENT_READ)
                   : 0)
              : (hasReadQueue ? IO_EVENT_READ : 0) |
                (hasWriteQueue ? IO_EVENT_WRITE : 0);
            EXPECT_EQ(combinerSelectActiveIoEvents(hasInitialization,
                                                   static_cast<AsyncOpRunningTy>(running),
                                                   initializationIsWrite,
                                                   hasReadQueue,
                                                   hasWriteQueue),
                      expected);
          }
        }
      }
    }
  }
}

TEST(core_decision, operation_state_selectors_cover_all_running_states)
{
  struct Case {
    AsyncOpRunningTy running;
    AsyncOpStatus status;
    CombinerInitializationAction initialization;
    CombinerReapAction reap;
  };
  const Case cases[] = {
    {arWaiting,    aosPending, ciaNone,    craKeep},
    {arRunning,    aosPending, ciaExecute, craKeep},
    {arCancelling, aosPending, ciaRelease, craKeep},
    {arWaiting,    aosSuccess, ciaNone,    craRelease},
    {arRunning,    aosSuccess, ciaRelease, craCancel},
    {arCancelling, aosSuccess, ciaRelease, craKeep},
  };

  for (const Case &test : cases) {
    SCOPED_TRACE(::testing::Message()
                 << "running=" << test.running
                 << " status=" << test.status);
    EXPECT_EQ(combinerSelectInitializationAction(test.running, test.status),
              test.initialization);
    EXPECT_EQ(combinerSelectReapAction(test.running, test.status), test.reap);
  }
}

TEST(core_queue, push_and_remove_preserve_bidirectional_links)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object), third(object);
  List list{};

  eqPushBack(&list, &first.root);
  eqPushBack(&list, &second.root);
  eqPushBack(&list, &third.root);
  EXPECT_EQ(listContents(list), (std::vector<asyncOpRoot*>{&first.root, &second.root, &third.root}));
  EXPECT_EQ(first.root.executeQueue.prev, nullptr);
  EXPECT_EQ(second.root.executeQueue.prev, &first.root);
  EXPECT_EQ(third.root.executeQueue.prev, &second.root);

  eqRemove(&list, &second.root);
  EXPECT_EQ(listContents(list), (std::vector<asyncOpRoot*>{&first.root, &third.root}));
  EXPECT_EQ(third.root.executeQueue.prev, &first.root);
  eqRemove(&list, &first.root);
  EXPECT_EQ(list.head, &third.root);
  EXPECT_EQ(third.root.executeQueue.prev, nullptr);
  eqRemove(&list, &third.root);
  EXPECT_EQ(list.head, nullptr);
  EXPECT_EQ(list.tail, nullptr);

  // Removing an already detached node is explicitly a no-op.
  eqRemove(&list, &second.root);
  objectDecrementReference(&object.root, 3);
  deleteOwner(backend, object);
}

TEST(core_combiner, captured_submission_stack_runs_in_fifo_order)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object), third(object);
  first.setResults({aosPending});
  second.setResults({aosPending});
  third.setResults({aosPending});
  first.executeHook = [&](TestOp &op) {
    if (op.executeCalls == 1) {
      combinerPushOperation(&second.root);
      combinerPushOperation(&third.root);
    }
  };

  combinerPushOperation(&first.root);

  EXPECT_EQ(backend.started,
            (std::vector<asyncOpRoot*>{&first.root, &second.root, &third.root}));
  EXPECT_EQ(listContents(object.root.readQueue),
            (std::vector<asyncOpRoot*>{&first.root, &second.root, &third.root}));
  EXPECT_EQ(first.executeCalls, 1u);
  EXPECT_EQ(second.executeCalls, 0u);
  EXPECT_EQ(third.executeCalls, 0u);

  cancelAndDrain(backend, object);
  deleteOwner(backend, object);
}

TEST(core_combiner, repeated_signal_bits_are_coalesced)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.setResults({aosPending, aosPending});
  op.executeHook = [](TestOp &current) {
    if (current.executeCalls == 1) {
      combinerPushProgress(&current.root);
      combinerPushProgress(&current.root);
    }
  };

  combinerPushOperation(&op.root);

  EXPECT_EQ(op.executeCalls, 2u);
  EXPECT_EQ(std::count(backend.handledSignals.begin(),
                       backend.handledSignals.end(),
                       COMBINER_TAG_PROGRESS_READ), 1);
  cancelAndDrain(backend, object);
  deleteOwner(backend, object);
}

TEST(core_combiner, terminal_prefix_is_released_and_pending_head_is_preserved)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object), third(object);
  first.setResults({aosSuccess});
  second.setResults({aosUnknownError});
  third.setResults({aosPending});
  eqPushBack(&object.root.readQueue, &first.root);
  eqPushBack(&object.root.readQueue, &second.root);
  eqPushBack(&object.root.readQueue, &third.root);

  executeOperationList(&object.root.readQueue);

  EXPECT_EQ(first.releaseCalls, 1u);
  EXPECT_EQ(second.releaseCalls, 1u);
  EXPECT_EQ(third.executeCalls, 1u);
  EXPECT_EQ(object.root.readQueue.head, &third.root);
  EXPECT_EQ(object.root.readQueue.tail, &third.root);
  EXPECT_EQ(third.root.executeQueue.prev, nullptr);
  backend.drainCompletions();
  EXPECT_EQ(first.callbackStatus, aosSuccess);
  EXPECT_EQ(second.callbackStatus, aosUnknownError);

  cancelAndDrain(backend, object);
  deleteOwner(backend, object);
}

TEST(core_combiner, terminal_head_from_progress_is_not_executed_again)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  eqPushBack(&object.root.readQueue, &op.root);
  op.root.running = arRunning;
  ASSERT_TRUE(opSetStatus(&op.root, opGetGeneration(&op.root), aosDisconnected));

  combinerPushProgress(&op.root);

  EXPECT_EQ(op.executeCalls, 0u);
  EXPECT_EQ(op.releaseCalls, 1u);
  EXPECT_EQ(object.root.readQueue.head, nullptr);
  backend.drainCompletions();
  EXPECT_EQ(op.callbackStatus, aosDisconnected);
  deleteOwner(backend, object);
}

TEST(core_combiner, terminal_result_losing_timeout_race_is_released_once)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.setResults({aosSuccess});
  op.executeHook = [](TestOp &current) {
    opCancel(&current.root, opGetGeneration(&current.root), aosTimeout);
  };

  combinerPushOperation(&op.root);

  EXPECT_EQ(op.executeCalls, 1u);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(op.cancelCalls, 0u);
  EXPECT_EQ(op.releaseCalls, 1u)
    << "the terminal execute result lost its CAS and was detached without release";
  EXPECT_EQ(object.root.readQueue.head, nullptr);

  backend.drainCompletions();
  EXPECT_EQ(op.finishCalls, 1u);
  EXPECT_EQ(op.callbackStatus, aosTimeout);
  EXPECT_EQ(object.root.refs, 1u);

  // Keep the intentionally failing regression test self-contained on the
  // current implementation, which strands the operation reference.
  if (object.root.refs > 1)
    objectDecrementReference(&object.root, object.root.refs - 1);
  deleteOwner(backend, object);
}

TEST(core_cancel, waiting_and_synchronously_cancelled_running_operations_release)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp running(object), waiting(object), alreadyTerminal(object);
  running.root.running = arRunning;
  waiting.root.running = arWaiting;
  alreadyTerminal.root.running = arWaiting;
  ASSERT_TRUE(opSetStatus(&alreadyTerminal.root,
                          opGetGeneration(&alreadyTerminal.root),
                          aosTimeout));
  eqPushBack(&object.root.readQueue, &running.root);
  eqPushBack(&object.root.readQueue, &waiting.root);
  eqPushBack(&object.root.readQueue, &alreadyTerminal.root);

  cancelOperationList(&object.root.readQueue, aosCanceled);

  EXPECT_EQ(running.cancelCalls, 1u);
  EXPECT_EQ(running.releaseCalls, 1u);
  EXPECT_EQ(waiting.releaseCalls, 1u);
  EXPECT_EQ(alreadyTerminal.releaseCalls, 0u);
  EXPECT_EQ(listContents(object.root.readQueue),
            (std::vector<asyncOpRoot*>{&alreadyTerminal.root}));

  uint32_t needStart = 0;
  reapObject(&object.root, COMBINER_TAG_CANCEL, &needStart);
  EXPECT_EQ(alreadyTerminal.releaseCalls, 1u);
  EXPECT_EQ(object.root.readQueue.head, nullptr);
  backend.drainCompletions();
  EXPECT_EQ(running.callbackStatus, aosCanceled);
  EXPECT_EQ(waiting.callbackStatus, aosCanceled);
  EXPECT_EQ(alreadyTerminal.callbackStatus, aosTimeout);
  deleteOwner(backend, object);
}

TEST(core_cancel, proactor_cancel_keeps_position_until_late_progress)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp running(object), waiting(object);
  running.cancelResult = 0;
  running.root.running = arRunning;
  eqPushBack(&object.root.readQueue, &running.root);
  eqPushBack(&object.root.readQueue, &waiting.root);

  cancelOperationList(&object.root.readQueue, aosCanceled);

  EXPECT_EQ(running.root.running, arCancelling);
  EXPECT_EQ(object.root.readQueue.head, &running.root);
  EXPECT_EQ(object.root.readQueue.tail, &running.root);
  EXPECT_EQ(waiting.releaseCalls, 1u);
  backend.drainCompletions();

  combinerPushProgress(&running.root);
  EXPECT_EQ(running.releaseCalls, 1u);
  EXPECT_EQ(object.root.readQueue.head, nullptr);
  backend.drainCompletions();
  EXPECT_EQ(running.callbackStatus, aosCanceled);
  deleteOwner(backend, object);
}

TEST(core_cancel, reap_rebuilds_multiple_retained_nodes_and_late_progress_advances_queue)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp running(object), waiting(object);
  running.cancelResult = 0;
  running.root.running = arRunning;
  ASSERT_TRUE(opSetStatus(&running.root, opGetGeneration(&running.root), aosTimeout));
  eqPushBack(&object.root.readQueue, &running.root);
  eqPushBack(&object.root.readQueue, &waiting.root);
  uint32_t needStart = 0;

  reapObject(&object.root, COMBINER_TAG_CANCEL, &needStart);
  EXPECT_EQ(running.root.running, arCancelling);
  EXPECT_EQ(listContents(object.root.readQueue),
            (std::vector<asyncOpRoot*>{&running.root, &waiting.root}));

  // A second idempotent reconcile covers the already-cancelling survivor and
  // rebuilds both links again.
  reapObject(&object.root, COMBINER_TAG_CANCEL, &needStart);
  EXPECT_EQ(listContents(object.root.readQueue),
            (std::vector<asyncOpRoot*>{&running.root, &waiting.root}));
  EXPECT_EQ(waiting.root.executeQueue.prev, &running.root);

  combinerPushProgress(&running.root);
  EXPECT_EQ(running.releaseCalls, 1u);
  EXPECT_EQ(waiting.executeCalls, 1u);
  backend.drainCompletions();
  cancelAndDrain(backend, object);
  deleteOwner(backend, object);
}

TEST(core_cancel, multiple_status_race_losers_remain_linked_for_winner_reap)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp first(object), second(object);
  ASSERT_TRUE(opSetStatus(&first.root, opGetGeneration(&first.root), aosTimeout));
  ASSERT_TRUE(opSetStatus(&second.root, opGetGeneration(&second.root), aosDisconnected));
  eqPushBack(&object.root.readQueue, &first.root);
  eqPushBack(&object.root.readQueue, &second.root);

  cancelOperationList(&object.root.readQueue, aosCanceled);

  EXPECT_EQ(listContents(object.root.readQueue),
            (std::vector<asyncOpRoot*>{&first.root, &second.root}));
  EXPECT_EQ(second.root.executeQueue.prev, &first.root);
  uint32_t needStart = 0;
  reapObject(&object.root, COMBINER_TAG_CANCEL, &needStart);
  EXPECT_EQ(object.root.readQueue.head, nullptr);
  backend.drainCompletions();
  EXPECT_EQ(first.callbackStatus, aosTimeout);
  EXPECT_EQ(second.callbackStatus, aosDisconnected);
  deleteOwner(backend, object);
}

TEST(core_cancel, stale_generation_does_not_cancel_reused_operation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  uintptr_t staleGeneration = opGetGeneration(&op.root);
  op.root.tag = ((staleGeneration + 1) << TAG_STATUS_SIZE) | aosPending;

  opCancel(&op.root, staleGeneration, aosTimeout);

  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_EQ(object.root.Head.data, 0u);
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

TEST(core_cancel, covers_submitted_but_not_started_operation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp active(object), submitted(object);
  active.setResults({aosPending});
  submitted.setResults({aosPending});
  active.executeHook = [&](TestOp &op) {
    if (op.executeCalls == 1) {
      combinerPushOperation(&submitted.root);
      cancelIo(&object.root);
    }
  };

  combinerPushOperation(&active.root);
  backend.drainCompletions();

  EXPECT_EQ(opGetStatus(&active.root), aosCanceled);
  EXPECT_EQ(opGetStatus(&submitted.root), aosCanceled)
    << "cancelIo swept queues before the captured submission was started";
  EXPECT_EQ(submitted.executeCalls, 0u);
  EXPECT_EQ(submitted.finishCalls, 1u);

  if (opGetStatus(&submitted.root) == aosPending)
    cancelAndDrain(backend, object);
  deleteOwner(backend, object);
}

TEST(core_cancel, submission_after_cancel_boundary_survives)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp active(object), afterBoundary(object);
  active.setResults({aosPending});
  afterBoundary.setResults({aosPending});
  active.executeHook = [&](TestOp &op) {
    if (op.executeCalls == 1) {
      cancelIo(&object.root);
      combinerPushOperation(&afterBoundary.root);
    }
  };

  combinerPushOperation(&active.root);
  backend.drainCompletions();

  EXPECT_EQ(opGetStatus(&active.root), aosCanceled);
  EXPECT_EQ(opGetStatus(&afterBoundary.root), aosPending);
  EXPECT_EQ(afterBoundary.executeCalls, 1u);
  cancelAndDrain(backend, object);
  deleteOwner(backend, object);
}

// TDD regression: the object-global CancelIoFlag is consumed by
// the reapObject sweep at the EARLIEST captured-chain position carrying a
// CANCEL bit. When an older positional CANCEL (a grid timeout) precedes the
// cancelIo() bit, the flag-driven sweep runs before the operations submitted
// between the two positions have started, and the cancelIo() position itself
// finds the flag already zero - operations submitted BEFORE cancelIo()
// survive it, breaking the documented "everything submitted before the
// cancel gets swept" invariant. The first/second expectations are
// intentionally red until the sweep honors the cancelIo() position.
TEST(core_cancel, earlier_grid_cancel_position_must_not_exempt_operations_submitted_before_cancel_io)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp active(object), timedOut(object), first(object), second(object);
  active.setResults({aosPending});
  timedOut.setResults({aosPending});
  first.setResults({aosPending});
  second.setResults({aosPending});
  active.executeHook = [&](TestOp &op) {
    if (op.executeCalls != 1)
      return;
    // A grid timeout fires while the combiner is busy: its CANCEL bit lands
    // on the chain position of the moment...
    combinerPushOperation(&timedOut.root);
    opCancel(&timedOut.root, opGetGeneration(&timedOut.root), aosTimeout);
    // ...then the user submits two more operations and only afterwards
    // cancels everything submitted so far
    combinerPushOperation(&first.root);
    combinerPushOperation(&second.root);
    cancelIo(&object.root);
  };

  combinerPushOperation(&active.root);
  backend.drainCompletions();

  EXPECT_EQ(opGetStatus(&active.root), aosCanceled);
  EXPECT_EQ(opGetStatus(&timedOut.root), aosTimeout);
  EXPECT_EQ(opGetStatus(&first.root), aosCanceled)
    << "the early grid-timeout position consumed the flag and exempted a pre-cancel submission";
  EXPECT_EQ(opGetStatus(&second.root), aosCanceled)
    << "the cancelIo position found the flag already consumed by the earlier CANCEL position";

  if (opGetStatus(&first.root) == aosPending || opGetStatus(&second.root) == aosPending)
    cancelAndDrain(backend, object);
  deleteOwner(backend, object);
}

TEST(core_cancel, repeated_request_while_flag_is_set_does_not_push_second_signal)
{
  TestBackend backend;
  TestObject object(backend);
  object.root.CancelIoFlag = 1;

  cancelIo(&object.root);

  EXPECT_EQ(object.root.CancelIoFlag, 2u);
  EXPECT_TRUE(backend.handledSignals.empty());
  EXPECT_EQ(object.root.Head.data, 0u);
  object.root.CancelIoFlag = 0;
  deleteOwner(backend, object);
}

TEST(core_initialization, pending_operation_freezes_ordinary_queues_until_success)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp connect(object, OPCODE_WRITE), read(object);
  connect.setResults({aosPending, aosSuccess});
  read.setResults({aosSuccess});
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&connect.root);

  combinerPushOperation(&connect.root);
  combinerPushOperation(&read.root);
  EXPECT_EQ(connect.root.running, arRunning);
  EXPECT_EQ(read.executeCalls, 0u);
  EXPECT_EQ(object.root.readQueue.head, &read.root);

  combinerPushProgress(&connect.root);

  EXPECT_EQ(object.root.initializationOp, 0u);
  EXPECT_EQ(connect.executeCalls, 2u);
  EXPECT_EQ(read.executeCalls, 1u);
  EXPECT_EQ(connect.releaseCalls, 1u);
  EXPECT_EQ(read.releaseCalls, 1u);
  backend.drainCompletions();
  EXPECT_EQ(connect.callbackStatus, aosSuccess);
  EXPECT_EQ(read.callbackStatus, aosSuccess);
  deleteOwner(backend, object);
}

TEST(core_initialization, read_direction_progress_drives_handshake_slot)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp handshake(object, OPCODE_READ), write(object, OPCODE_WRITE);
  handshake.setResults({aosPending, aosSuccess});
  write.setResults({aosSuccess});
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&handshake.root);

  combinerPushOperation(&handshake.root);
  combinerPushOperation(&write.root);
  ASSERT_EQ(handshake.root.running, arRunning);
  ASSERT_EQ(write.executeCalls, 0u);

  combinerPushProgress(&handshake.root);

  EXPECT_EQ(handshake.executeCalls, 2u);
  EXPECT_EQ(write.executeCalls, 1u);
  EXPECT_EQ(object.root.initializationOp, 0u);
  EXPECT_NE(std::find(backend.handledSignals.begin(),
                      backend.handledSignals.end(),
                      COMBINER_TAG_PROGRESS_READ),
            backend.handledSignals.end());
  backend.drainCompletions();
  deleteOwner(backend, object);
}

TEST(core_initialization, failure_cancels_operations_queued_behind_it)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp connect(object, OPCODE_WRITE), read(object), write(object, OPCODE_WRITE);
  connect.setResults({aosPending, aosDisconnected});
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&connect.root);
  combinerPushOperation(&connect.root);
  combinerPushOperation(&read.root);
  combinerPushOperation(&write.root);

  combinerPushProgress(&connect.root);

  EXPECT_EQ(opGetStatus(&read.root), aosDisconnected);
  EXPECT_EQ(opGetStatus(&write.root), aosDisconnected);
  EXPECT_EQ(read.executeCalls, 0u);
  EXPECT_EQ(write.executeCalls, 0u);
  backend.drainCompletions();
  EXPECT_EQ(connect.callbackStatus, aosDisconnected);
  EXPECT_EQ(read.callbackStatus, aosDisconnected);
  EXPECT_EQ(write.callbackStatus, aosDisconnected);
  deleteOwner(backend, object);
}

TEST(core_initialization, proactor_cancel_waits_for_late_completion)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp connect(object, OPCODE_WRITE);
  connect.cancelResult = 0;
  connect.setResults({aosPending});
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&connect.root);
  combinerPushOperation(&connect.root);

  cancelIo(&object.root);

  EXPECT_EQ(connect.root.running, arCancelling);
  EXPECT_EQ(connect.releaseCalls, 0u);
  EXPECT_EQ(object.root.initializationOp, reinterpret_cast<uintptr_t>(&connect.root));

  combinerPushProgress(&connect.root);
  EXPECT_EQ(connect.releaseCalls, 1u);
  EXPECT_EQ(object.root.initializationOp, 0u);
  backend.drainCompletions();
  EXPECT_EQ(connect.callbackStatus, aosCanceled);
  deleteOwner(backend, object);
}

TEST(core_initialization, synchronous_cancel_releases_immediately)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp connect(object, OPCODE_WRITE);
  connect.cancelResult = 1;
  connect.setResults({aosPending});
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&connect.root);
  combinerPushOperation(&connect.root);

  cancelIo(&object.root);

  EXPECT_EQ(connect.cancelCalls, 1u);
  EXPECT_EQ(connect.releaseCalls, 1u);
  EXPECT_EQ(object.root.initializationOp, 0u);
  backend.drainCompletions();
  EXPECT_EQ(connect.callbackStatus, aosCanceled);
  deleteOwner(backend, object);
}

TEST(core_initialization, direct_reap_handles_running_and_waiting_terminal_states)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp running(object, OPCODE_WRITE), waiting(object, OPCODE_WRITE);
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&running.root);
  running.root.running = arRunning;
  ASSERT_TRUE(opSetStatus(&running.root, opGetGeneration(&running.root), aosTimeout));
  uint32_t needStart = 0;

  reapObject(&object.root, COMBINER_TAG_CANCEL, &needStart);
  EXPECT_EQ(running.cancelCalls, 1u);
  EXPECT_EQ(running.releaseCalls, 1u);
  EXPECT_EQ(object.root.initializationOp, 0u);

  object.root.initializationOp = reinterpret_cast<uintptr_t>(&waiting.root);
  waiting.root.running = arWaiting;
  ASSERT_TRUE(opSetStatus(&waiting.root, opGetGeneration(&waiting.root), aosCanceled));
  reapObject(&object.root, COMBINER_TAG_CANCEL, &needStart);
  EXPECT_EQ(waiting.cancelCalls, 0u);
  EXPECT_EQ(waiting.releaseCalls, 1u);
  EXPECT_EQ(object.root.initializationOp, 0u);
  EXPECT_EQ(needStart & (IO_EVENT_READ | IO_EVENT_WRITE),
            IO_EVENT_READ | IO_EVENT_WRITE);

  backend.drainCompletions();
  deleteOwner(backend, object);
}

TEST(core_initialization, process_is_noop_without_running_slot)
{
  TestBackend backend;
  TestObject object(backend);
  uint32_t needStart = 0;
  processInitializationOp(&object.root, &needStart);
  EXPECT_EQ(needStart, 0u);

  TestOp waiting(object, OPCODE_WRITE);
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&waiting.root);
  waiting.root.running = arWaiting;
  processInitializationOp(&object.root, &needStart);
  EXPECT_EQ(waiting.executeCalls, 0u);
  EXPECT_EQ(waiting.releaseCalls, 0u);

  object.root.initializationOp = 0;
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

TEST(core_initialization, terminal_status_from_child_releases_without_reexecution)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp connect(object, OPCODE_WRITE);
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&connect.root);
  connect.root.running = arRunning;
  ASSERT_TRUE(opSetStatus(&connect.root,
                          opGetGeneration(&connect.root),
                          aosUnknownError));
  uint32_t needStart = 0;

  processInitializationOp(&object.root, &needStart);

  EXPECT_EQ(connect.executeCalls, 0u);
  EXPECT_EQ(connect.releaseCalls, 1u);
  EXPECT_EQ(object.root.initializationOp, 0u);
  backend.drainCompletions();
  EXPECT_EQ(connect.callbackStatus, aosUnknownError);
  deleteOwner(backend, object);
}

TEST(core_initialization, operation_cancelled_before_start_is_never_executed)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp connect(object, OPCODE_WRITE);
  object.root.initializationOp = reinterpret_cast<uintptr_t>(&connect.root);
  ASSERT_TRUE(opSetStatus(&connect.root, opGetGeneration(&connect.root), aosCanceled));

  combinerPushOperation(&connect.root);

  EXPECT_EQ(connect.executeCalls, 0u);
  EXPECT_EQ(connect.releaseCalls, 1u);
  EXPECT_EQ(object.root.initializationOp, 0u);
  backend.drainCompletions();
  deleteOwner(backend, object);
}

TEST(core_progress, resume_parent_success_redrives_operation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp parent(object);
  parent.setResults({aosPending, aosSuccess});
  combinerPushOperation(&parent.root);

  resumeParent(&parent.root, aosSuccess);

  EXPECT_EQ(parent.executeCalls, 2u);
  EXPECT_EQ(parent.releaseCalls, 1u);
  backend.drainCompletions();
  EXPECT_EQ(parent.callbackStatus, aosSuccess);
  deleteOwner(backend, object);
}

TEST(core_progress, resume_parent_failure_finishes_without_reexecution)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp parent(object);
  parent.setResults({aosPending, aosSuccess});
  combinerPushOperation(&parent.root);

  resumeParent(&parent.root, aosUnknownError);

  EXPECT_EQ(parent.executeCalls, 1u);
  EXPECT_EQ(parent.releaseCalls, 1u);
  backend.drainCompletions();
  EXPECT_EQ(parent.callbackStatus, aosUnknownError);
  deleteOwner(backend, object);
}

TEST(core_progress, write_parent_routes_progress_to_write_queue)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp parent(object, OPCODE_WRITE);
  parent.setResults({aosPending, aosSuccess});
  combinerPushOperation(&parent.root);

  resumeParent(&parent.root, aosSuccess);

  EXPECT_EQ(parent.executeCalls, 2u);
  EXPECT_EQ(parent.releaseCalls, 1u);
  backend.drainCompletions();
  EXPECT_EQ(parent.callbackStatus, aosSuccess);
  deleteOwner(backend, object);
}

TEST(core_operation_release, explicit_execute_list_unlinks_before_enqueue)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp released(object), survivor(object);
  List list{};
  eqPushBack(&list, &released.root);
  eqPushBack(&list, &survivor.root);
  opForceStatus(&released.root, aosSuccess);

  opRelease(&released.root, aosSuccess, &list);

  EXPECT_EQ(list.head, &survivor.root);
  EXPECT_EQ(list.tail, &survivor.root);
  EXPECT_EQ(survivor.root.executeQueue.prev, nullptr);
  backend.drainCompletions();
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

TEST(core_delete_lifecycle, no_operations_destructs_exactly_once)
{
  TestBackend backend;
  TestObject object(backend);

  objectDelete(&object.root);

  EXPECT_EQ(object.root.DeletePending, 1u);
  EXPECT_EQ(object.destructorCallbacks, 1u);
  EXPECT_EQ(object.resourceDestructors, 1u);
}

TEST(core_delete_lifecycle, operation_reference_delays_destructor_until_callback)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.setResults({aosPending});
  combinerPushOperation(&op.root);

  objectDelete(&object.root);

  EXPECT_EQ(opGetStatus(&op.root), aosCanceled);
  EXPECT_EQ(object.destructorCallbacks, 0u);
  EXPECT_EQ(object.root.refs, 1u);
  backend.drainCompletions();
  EXPECT_EQ(op.finishCalls, 1u);
  EXPECT_EQ(object.destructorCallbacks, 1u);
  EXPECT_EQ(object.resourceDestructors, 1u);
}

TEST(core_delete_lifecycle, external_owner_delays_destructor)
{
  TestBackend backend;
  TestObject object(backend);
  objectIncrementReference(&object.root, 1);

  objectDelete(&object.root);
  EXPECT_EQ(object.destructorCallbacks, 0u);
  EXPECT_EQ(object.root.refs, 1u);

  objectDecrementReference(&object.root, 1);
  EXPECT_EQ(object.destructorCallbacks, 1u);
  EXPECT_EQ(object.resourceDestructors, 1u);
}

TEST(core_delete_lifecycle, callback_retain_extends_lifetime)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  op.setResults({aosSuccess});
  op.finishHook = [&](TestOp&) { objectIncrementReference(&object.root, 1); };
  combinerPushOperation(&op.root);
  objectDelete(&object.root);

  backend.drainCompletions();

  EXPECT_EQ(op.finishCalls, 1u);
  EXPECT_EQ(object.root.refs, 1u);
  EXPECT_EQ(object.destructorCallbacks, 0u);
  objectDecrementReference(&object.root, 1);
  EXPECT_EQ(object.destructorCallbacks, 1u);
}

TEST(core_delete_lifecycle, sticky_delete_sweeps_submission_captured_after_cancel)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp active(object), captured(object);
  active.setResults({aosPending});
  captured.setResults({aosPending});
  active.executeHook = [&](TestOp &op) {
    if (op.executeCalls == 1) {
      objectDelete(&object.root);
      combinerPushOperation(&captured.root);
    }
  };

  combinerPushOperation(&active.root);
  backend.drainCompletions();

  EXPECT_EQ(opGetStatus(&active.root), aosCanceled);
  EXPECT_EQ(opGetStatus(&captured.root), aosCanceled);
  EXPECT_EQ(captured.finishCalls, 1u);
  EXPECT_EQ(object.destructorCallbacks, 1u);
}


TEST(core_tagged_pointer, round_trips_aligned_pointer_and_low_bits)
{
  void *storage = alignedMalloc(128, TAGGED_POINTER_ALIGNMENT);
  ASSERT_NE(storage, nullptr);
  constexpr uintptr_t inputData = TAGGED_POINTER_DATA_MASK + 7;
  void *tagged = __tagged_pointer_make(storage, inputData);
  void *decoded = nullptr;
  uintptr_t data = 0;

  __tagged_pointer_decode(tagged, &decoded, &data);

  EXPECT_EQ(decoded, storage);
  EXPECT_EQ(data, inputData & TAGGED_POINTER_DATA_MASK);
  alignedFree(storage);
}

TEST(core_operation_tag, generation_survives_status_transitions)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  uintptr_t generation = opGetGeneration(&op.root);

  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_TRUE(opSetStatus(&op.root, generation, aosSuccess));
  EXPECT_FALSE(opSetStatus(&op.root, generation, aosTimeout));
  EXPECT_EQ(opGetStatus(&op.root), aosSuccess);
  opForceStatus(&op.root, aosCanceled);
  EXPECT_EQ(opGetStatus(&op.root), aosCanceled);
  EXPECT_EQ(opGetGeneration(&op.root), generation);
  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(core_operation_init, running_flag_and_flag_union_are_preserved)
{
  TestBackend backend;
  TestObject object(backend);
  AsyncFlags flags = afRealtime | afRunning;
  TestOp op(object, OPCODE_WRITE, flags);

  EXPECT_EQ(op.root.running, arRunning);
  EXPECT_EQ(op.root.flags, flags);
  EXPECT_NE(op.root.flags & afRealtime, 0);
  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(core_operation_allocation, regular_and_realtime_pools_reuse_aligned_storage)
{
  TestBackend backend;
  ConcurrentQueue regularPool{};
  ConcurrentQueue realtimePool{};
  asyncOpRoot *regular = nullptr;
  asyncOpRoot *realtime = nullptr;

  EXPECT_EQ(asyncOpAlloc(&backend.base,
                         sizeof(asyncOpRoot),
                         0,
                         &regularPool,
                         &realtimePool,
                         &regular), 1);
  ASSERT_NE(regular, nullptr);
  EXPECT_EQ(reinterpret_cast<uintptr_t>(regular) & (kCombinerAlignment - 1), 0u);
  EXPECT_EQ(regular->timerId, nullptr);
  EXPECT_EQ(regular->objectPool, &regularPool);

  concurrentQueuePush(&regularPool, regular);
  asyncOpRoot *reused = nullptr;
  EXPECT_EQ(asyncOpAlloc(&backend.base,
                         sizeof(asyncOpRoot),
                         0,
                         &regularPool,
                         &realtimePool,
                         &reused), 0);
  EXPECT_EQ(reused, regular);

  EXPECT_EQ(asyncOpAlloc(&backend.base,
                         sizeof(asyncOpRoot),
                         1,
                         &regularPool,
                         &realtimePool,
                         &realtime), 1);
  ASSERT_NE(realtime, nullptr);
  EXPECT_EQ(backend.initializeTimerCalls, 1u);
  EXPECT_NE(realtime->timerId, nullptr);
  EXPECT_EQ(realtime->objectPool, &realtimePool);

  alignedFree(reused);
  alignedFree(realtime);
  destroyConcurrentQueue(&regularPool);
  destroyConcurrentQueue(&realtimePool);
}

struct EventContext {
  unsigned finishes = 0;
  unsigned destructors = 0;
};

void eventFinish(asyncOpRoot *root)
{
  static_cast<EventContext*>(root->arg)->finishes++;
}

void eventDestructor(aioUserEvent*, void *arg)
{
  static_cast<EventContext*>(arg)->destructors++;
}

TEST(core_user_event, nonsemaphore_activation_coalesces_until_deactivated)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.root.arg = &context;
  event.root.finishMethod = eventFinish;
  event.root.opCode = OPCODE_OTHER;
  event.tag = 1;
  event.isSemaphore = 0;
  eventSetDestructorCb(&event, eventDestructor, &context);

  EXPECT_TRUE(eventTryActivate(&event));
  EXPECT_FALSE(eventTryActivate(&event));
  eventDeactivate(&event);
  EXPECT_TRUE(eventTryActivate(&event));
  eventDeactivate(&event);

  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 2u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 1u);
  EXPECT_EQ(context.destructors, 1u);

  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, &event);
}

TEST(core_user_event, semaphore_counts_each_activation)
{
  aioUserEvent event{};
  event.tag = 1;
  event.isSemaphore = 1;

  EXPECT_TRUE(eventTryActivate(&event));
  EXPECT_TRUE(eventTryActivate(&event));
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
  eventDeactivate(&event);
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
}

TEST(core_user_event, reference_increment_and_final_release_without_destructor_callback)
{
  TestBackend backend;
  aioUserEvent event{};
  event.root.objectPool = &backend.operationPool;
  event.tag = 1;

  EXPECT_EQ(eventIncrementReference(&event, 2), 1u);
  EXPECT_EQ(event.tag & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(eventDecrementReference(&event, 2) & TAG_EVENT_MASK, 3u);
  EXPECT_EQ(eventDecrementReference(&event, 1) & TAG_EVENT_MASK, 1u);

  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, &event);
}

TEST(core_global_queue, callbacks_release_operations_and_quit_marker_stops_drain)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp callbackOp(object);
  TestOp noCallbackOp(object, OPCODE_READ, afNone, 0, false);
  opForceStatus(&callbackOp.root, aosSuccess);
  opForceStatus(&noCallbackOp.root, aosCanceled);
  currentFinishedSync = 17;
  concurrentQueuePush(&backend.base.globalQueue, &callbackOp.root);
  concurrentQueuePush(&backend.base.globalQueue, &noCallbackOp.root);
  concurrentQueuePush(&backend.base.globalQueue, nullptr);

  EXPECT_EQ(executeGlobalQueue(&backend.base), 0);
  EXPECT_EQ(callbackOp.finishCalls, 1u);
  EXPECT_EQ(callbackOp.callbackStatus, aosSuccess);
  EXPECT_EQ(noCallbackOp.finishCalls, 0u);
  EXPECT_EQ(currentFinishedSync, 0u);
  EXPECT_EQ(object.root.refs, 1u);

  void *first = nullptr;
  void *second = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &first));
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &second));
  EXPECT_EQ(first, &callbackOp.root);
  EXPECT_EQ(second, &noCallbackOp.root);
  objectDelete(&object.root);
}

TEST(core_global_queue, user_event_branch_deactivates_and_returns_when_empty)
{
  TestBackend backend;
  EventContext context;
  aioUserEvent event{};
  event.root.opCode = actUserEvent;
  event.root.finishMethod = eventFinish;
  event.root.arg = &context;
  event.tag = TAG_EVENT_OP + 1;
  event.isSemaphore = 0;
  concurrentQueuePush(&backend.base.globalQueue, &event.root);

  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_EQ(context.finishes, 1u);
  EXPECT_EQ(event.tag, 1u);
}

TEST(core_clock, monotonic_ticks_are_non_decreasing)
{
  uint64_t first = getMonotonicTicks();
  uint64_t second = getMonotonicTicks();
  EXPECT_LE(first, second);
}

TEST(core_buffer, copy_handles_complete_and_partial_source_ranges)
{
  std::array<uint8_t, 6> source{{1, 2, 3, 4, 5, 6}};
  std::array<uint8_t, 8> destination{};
  ioBuffer buffer{source.data(), source.size(), source.size(), 1};
  size_t offset = 2;

  EXPECT_EQ(copyFromBuffer(destination.data(), &offset, &buffer, 5), 1);
  EXPECT_EQ(offset, 5u);
  EXPECT_EQ(buffer.offset, 4u);
  EXPECT_EQ(destination[2], 2u);
  EXPECT_EQ(destination[4], 4u);

  offset = 0;
  buffer.offset = 4;
  buffer.dataSize = 6;
  EXPECT_EQ(copyFromBuffer(destination.data(), &offset, &buffer, 5), 0);
  EXPECT_EQ(offset, 2u);
  EXPECT_EQ(buffer.offset, 0u);
  EXPECT_EQ(buffer.dataSize, 0u);
  EXPECT_EQ(destination[0], 5u);
  EXPECT_EQ(destination[1], 6u);
}

TEST(core_grace_period, active_thread_holds_retired_batch_until_quiescence)
{
  TestBackend backend;
  TestObject first(backend), second(backend);
  messageLoopThreadId = 0;
  graceThreadEnter(&backend.base);        // claims slot 0: counter 0, high water 1

  graceRetire(&backend.base, &first.root, TestObject::memoryRelease);
  graceRetire(&backend.base, &second.root, TestObject::memoryRelease);

  // First pass captures the batch against the unmoved counter: nothing frees
  graceReclaim(&backend.base);
  EXPECT_EQ(first.memoryReleases, 0u);
  EXPECT_EQ(second.memoryReleases, 0u);
  EXPECT_EQ(backend.base.graceLimbo, nullptr);
  EXPECT_EQ(backend.base.gracePending, &second.root);

  // The quiescent tick ripens the whole batch at once
  graceQuiesce(&backend.base);
  EXPECT_EQ(first.memoryReleases, 1u);
  EXPECT_EQ(second.memoryReleases, 1u);
  EXPECT_EQ(backend.base.gracePending, nullptr);
}

TEST(core_grace_period, objects_retired_after_capture_wait_for_the_next_batch)
{
  TestBackend backend;
  TestObject first(backend), second(backend);
  messageLoopThreadId = 0;
  graceThreadEnter(&backend.base);        // claims slot 0: counter 0, high water 1

  graceRetire(&backend.base, &first.root, TestObject::memoryRelease);
  graceReclaim(&backend.base);            // captures batch {first}
  graceRetire(&backend.base, &second.root, TestObject::memoryRelease);

  // The tick ripens the captured batch only; second was pushed after the
  // capture and needs a full grace period of its own
  graceQuiesce(&backend.base);
  EXPECT_EQ(first.memoryReleases, 1u);
  EXPECT_EQ(second.memoryReleases, 0u);
  EXPECT_EQ(backend.base.gracePending, &second.root);

  graceQuiesce(&backend.base);
  EXPECT_EQ(second.memoryReleases, 1u);
  EXPECT_EQ(backend.base.gracePending, nullptr);
}

TEST(core_grace_period, exited_slot_does_not_gate_the_pending_batch)
{
  TestBackend backend;
  TestObject object(backend);
  messageLoopThreadId = 0;
  graceThreadEnter(&backend.base);        // claims slot 0
  backend.base.graceSeen[1].seen = 5;     // a live foreign thread in slot 1
  backend.base.graceSlotCount = 2;

  graceRetire(&backend.base, &object.root, TestObject::memoryRelease);
  graceReclaim(&backend.base);            // captured against {0, 5}
  EXPECT_EQ(object.memoryReleases, 0u);

  // Slot 1 exits (quit path stamp), slot 0 ticks: the batch ripens
  backend.base.graceSeen[1].seen = UINTPTR_MAX;
  graceQuiesce(&backend.base);
  EXPECT_EQ(object.memoryReleases, 1u);
}

TEST(core_grace_period, frozen_reclamation_keeps_limbo_intact)
{
  TestBackend backend;
  TestObject object(backend);
  backend.base.graceFrozen = 1;
  graceRetire(&backend.base, &object.root, TestObject::memoryRelease);

  graceReclaim(&backend.base);

  EXPECT_EQ(object.memoryReleases, 0u);
  EXPECT_EQ(backend.base.graceLimbo, &object.root);
  backend.base.graceLimbo = nullptr;
}

TEST(core_grace_period, slot_claim_tracks_high_water_and_collision_freezes)
{
  TestBackend backend;
  messageLoopThreadId = 3;

  graceThreadEnter(&backend.base);
  EXPECT_EQ(backend.base.graceSeen[3].seen, 0u);   // counters start at zero
  EXPECT_EQ(backend.base.graceSlotCount, 4u);
  EXPECT_EQ(backend.base.graceFrozen, 0u);

  graceThreadEnter(&backend.base);
  EXPECT_EQ(backend.base.graceFrozen, 1u);
}

// TDD regression: graceQuiesce gates only on the slot limit,
// not on whether THIS thread's claim in graceThreadEnter succeeded. A loop
// thread that adopted an id whose slot is still owned by a live thread
// (elastic pool: an exiting thread decremented the counter while an older
// thread still runs) freezes reclamation but keeps ticking the owner's
// single-writer counter every loop turn - and stamps UINTPTR_MAX into it on
// exit - forging the owner's quiescence for any scanner still holding a
// captured batch: the exact use-after-free the freeze exists to prevent.
// Intentionally red until the tick (and the backend exit stamp) require a
// successful claim.
TEST(core_grace_period, failed_slot_claim_must_not_tick_the_owners_counter)
{
  TestBackend backend;
  TestObject object(backend);
  messageLoopThreadId = 3;
  graceThreadEnter(&backend.base);              // the owner claims slot 3
  ASSERT_EQ(backend.base.graceSeen[3].seen, 0u);
  ASSERT_EQ(backend.base.graceFrozen, 0u);

  graceThreadEnter(&backend.base);              // adopted id, owner still live
  ASSERT_EQ(backend.base.graceFrozen, 1u);

  // Something must be parked, or the tick fast path returns before writing
  graceRetire(&backend.base, &object.root, TestObject::memoryRelease);
  graceQuiesce(&backend.base);                  // the loser's loop tick

  EXPECT_EQ(backend.base.graceSeen[3].seen, 0u)
    << "a thread that failed its slot claim forged the owner's quiescence counter";
  EXPECT_EQ(object.memoryReleases, 0u);
  backend.base.graceLimbo = nullptr;            // parked object lives on the test stack
}

// TDD regression, exit-path half of the failed-claim defect: the stamp is gated only by the
// slot limit, not by claim ownership - a thread whose claim failed stamps
// UINTPTR_MAX into the live owner's slot on its way out, marking the owner
// exited: from then on the owner's kernel batches stop gating reclamation at
// all and every captured batch ripens under its feet. Intentionally red until
// the stamp requires a successful claim.
TEST(core_grace_period, failed_slot_claim_must_not_stamp_the_owners_slot_on_exit)
{
  TestBackend backend;
  messageLoopThreadId = 3;
  graceThreadEnter(&backend.base);              // the owner claims slot 3
  ASSERT_EQ(backend.base.graceSeen[3].seen, 0u);
  graceThreadEnter(&backend.base);              // adopted id, owner still live
  ASSERT_EQ(backend.base.graceFrozen, 1u);

  graceThreadExit(&backend.base);               // the loser leaves the loop

  EXPECT_EQ(backend.base.graceSeen[3].seen, 0u)
    << "an exiting thread that never owned the slot stamped the live owner out";
}

TEST(core_grace_period, out_of_range_thread_freezes_without_touching_slots)
{
  TestBackend backend;
  messageLoopThreadId = backend.base.graceSlotLimit;

  graceThreadEnter(&backend.base);
  graceQuiesce(&backend.base);

  EXPECT_EQ(backend.base.graceFrozen, 1u);
  EXPECT_EQ(backend.base.graceSlotCount, 0u);
}

TEST(core_delete_lifecycle, destructor_callback_is_optional)
{
  TestBackend backend;
  TestObject object(backend);
  object.root.destructorCb = nullptr;

  objectDelete(&object.root);

  EXPECT_EQ(object.destructorCallbacks, 0u);
  EXPECT_EQ(object.resourceDestructors, 1u);
}

enum class SyncResult {
  Inline,
  Pending,
  Terminal,
};

struct SyncScenario {
  TestObject *object;
  std::unique_ptr<TestOp> operation;
  std::function<void()> createHook;
  SyncResult result = SyncResult::Inline;
  int opCode = OPCODE_READ;
  unsigned creates = 0;
  unsigned syncCalls = 0;
  unsigned makeResults = 0;
  unsigned initCalls = 0;
  bool deleteDuringSync = false;

  explicit SyncScenario(TestObject &owner) : object(&owner) {}

  TestOp &createOperation(int opCode, AsyncFlags flags, uint64_t timeout, bool callback)
  {
    if (!operation)
      operation = std::make_unique<TestOp>(*object, opCode, flags, timeout, callback);
    return *operation;
  }

  static asyncOpRoot *create(aioObjectRoot*,
                             AsyncFlags flags,
                             uint64_t timeout,
                             void *callback,
                             void*,
                             int opCode,
                             void *context)
  {
    SyncScenario &scenario = *static_cast<SyncScenario*>(context);
    scenario.creates++;
    asyncOpRoot *op = &scenario.createOperation(opCode, flags, timeout, callback != nullptr).root;
    if (scenario.createHook)
      scenario.createHook();
    return op;
  }

  static asyncOpRoot *sync(aioObjectRoot *object,
                           AsyncFlags flags,
                           uint64_t timeout,
                           void *callback,
                           void*,
                           void *context)
  {
    SyncScenario &scenario = *static_cast<SyncScenario*>(context);
    scenario.syncCalls++;
    if (scenario.deleteDuringSync)
      objectDelete(object);
    if (scenario.result == SyncResult::Inline)
      return nullptr;
    TestOp &op = scenario.createOperation(scenario.opCode, flags, timeout, callback != nullptr);
    if (scenario.result == SyncResult::Terminal)
      opForceStatus(&op.root, aosSuccess);
    return &op.root;
  }

  static void makeResult(void *context)
  {
    static_cast<SyncScenario*>(context)->makeResults++;
  }

  static void init(asyncOpRoot*, void *context)
  {
    static_cast<SyncScenario*>(context)->initCalls++;
  }
};

void runSyncScenario(SyncScenario &scenario,
                     AsyncFlags flags,
                     void *callback = nullptr,
                     int opCode = OPCODE_READ)
{
  scenario.opCode = opCode;
  runAioOperation(&scenario.object->root,
                  SyncScenario::create,
                  SyncScenario::sync,
                  SyncScenario::makeResult,
                  SyncScenario::init,
                  flags,
                  0,
                  callback,
                  nullptr,
                  opCode,
                  &scenario);
}

struct IoScenario {
  SyncScenario sync;
  AsyncFlags flags = afNone;
  int opCode = OPCODE_READ;
  asyncOpRoot *result = nullptr;
  AsyncOpStatus status = aosUnknown;
  bool returned = false;

  explicit IoScenario(TestObject &object) : sync(object) {}

  static void run(void *arg)
  {
    IoScenario &scenario = *static_cast<IoScenario*>(arg);
    scenario.sync.opCode = scenario.opCode;
    scenario.result = runIoOperation(&scenario.sync.object->root,
                                     SyncScenario::create,
                                     SyncScenario::sync,
                                     SyncScenario::init,
                                     scenario.flags,
                                     0,
                                     scenario.opCode,
                                     &scenario.sync);
    if (scenario.result) {
      scenario.status = opGetStatus(scenario.result);
      releaseAsyncOp(scenario.result);
      void *recycled = nullptr;
      EXPECT_TRUE(concurrentQueuePop(scenario.result->objectPool, &recycled));
      EXPECT_EQ(recycled, scenario.result);
    }
    scenario.returned = true;
  }
};

coroutineTy *startIoScenario(IoScenario &scenario)
{
  coroutineTy *coroutine = coroutineNew(IoScenario::run, &scenario, 0x10000);
  EXPECT_NE(coroutine, nullptr);
  return coroutine;
}

void deliverCoroutineCompletion(TestBackend &backend)
{
  ASSERT_EQ(backend.completions.size(), 1u);
  asyncOpRoot *op = backend.completions.front();
  backend.completions.clear();
  coroutineTy *coroutine = reinterpret_cast<coroutineTy*>(op->finishMethod);
  EXPECT_EQ(coroutineCall(coroutine), 1);
}

TEST(core_sync_path, inline_result_avoids_allocation)
{
  TestBackend backend;
  TestObject object(backend);
  SyncScenario scenario(object);

  runSyncScenario(scenario, afNone);

  EXPECT_EQ(scenario.syncCalls, 1u);
  EXPECT_EQ(scenario.makeResults, 1u);
  EXPECT_EQ(scenario.creates, 0u);
  objectDelete(&object.root);
}

TEST(core_sync_path, callback_without_active_once_is_queued)
{
  TestBackend backend;
  TestObject object(backend);
  SyncScenario scenario(object);

  runSyncScenario(scenario, afNone, &scenario);

  EXPECT_EQ(scenario.syncCalls, 1u);
  EXPECT_EQ(scenario.makeResults, 0u);
  EXPECT_EQ(scenario.creates, 1u);
  EXPECT_EQ(scenario.initCalls, 1u);
  ASSERT_EQ(backend.completions.size(), 1u);
  backend.drainCompletions();
  EXPECT_EQ(scenario.operation->callbackStatus, aosSuccess);
  objectDelete(&object.root);
}

TEST(core_sync_path, active_once_budget_queues_completion_at_limit)
{
  TestBackend backend;
  TestObject object(backend);
  SyncScenario scenario(object);
  currentFinishedSync = MAX_SYNCHRONOUS_FINISHED_OPERATION;

  runSyncScenario(scenario, afActiveOnce, &scenario);

  EXPECT_EQ(scenario.makeResults, 0u);
  EXPECT_EQ(scenario.creates, 1u);
  EXPECT_EQ(currentFinishedSync, 0u);
  backend.drainCompletions();
  objectDelete(&object.root);
}

TEST(core_sync_path, pending_sync_result_is_started_by_combiner)
{
  TestBackend backend;
  TestObject object(backend);
  SyncScenario scenario(object);
  scenario.result = SyncResult::Pending;

  runSyncScenario(scenario, afNone, &scenario);

  ASSERT_NE(scenario.operation, nullptr);
  EXPECT_EQ(scenario.operation->executeCalls, 1u);
  EXPECT_EQ(scenario.operation->root.running, arRunning);
  cancelIo(&object.root);
  backend.drainCompletions();
  objectDelete(&object.root);
}

TEST(core_sync_path, terminal_sync_result_is_queued_without_execute)
{
  TestBackend backend;
  TestObject object(backend);
  SyncScenario scenario(object);
  scenario.result = SyncResult::Terminal;

  runSyncScenario(scenario, afNone, &scenario);

  ASSERT_NE(scenario.operation, nullptr);
  EXPECT_EQ(scenario.operation->executeCalls, 0u);
  ASSERT_EQ(backend.completions.size(), 1u);
  backend.drainCompletions();
  EXPECT_EQ(scenario.operation->callbackStatus, aosSuccess);
  objectDelete(&object.root);
}

TEST(core_sync_path, existing_queue_uses_async_submission_without_sync_attempt)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp active(object, OPCODE_WRITE);
  active.root.running = arRunning;
  eqPushBack(&object.root.writeQueue, &active.root);
  SyncScenario scenario(object);

  runSyncScenario(scenario, afNone, &scenario, OPCODE_WRITE);

  EXPECT_EQ(scenario.syncCalls, 0u);
  EXPECT_EQ(scenario.creates, 1u);
  EXPECT_EQ(listContents(object.root.writeQueue),
            (std::vector<asyncOpRoot*>{&active.root, &scenario.operation->root}));
  cancelIo(&object.root);
  backend.drainCompletions();
  objectDelete(&object.root);
}

TEST(core_sync_path, speculative_allocation_is_released_after_combiner_handoff)
{
  TestBackend backend;
  TestObject object(backend);
  SyncScenario scenario(object);
  std::atomic<unsigned> phase{0};
  AsyncOpTaggedPtr busy = taggedAsyncOpStub();
  object.root.Head = busy;
  scenario.createHook = [&]() {
    phase.store(1, std::memory_order_release);
    while (phase.load(std::memory_order_acquire) != 2)
      std::this_thread::yield();
  };

  std::thread submitter([&]() {
    runSyncScenario(scenario, afNone);
  });
  while (phase.load(std::memory_order_acquire) != 1)
    std::this_thread::yield();
  bool handedOff = __uintptr_atomic_compare_and_swap(&object.root.Head.data, busy.data, 0);
  phase.store(2, std::memory_order_release);
  submitter.join();

  ASSERT_TRUE(handedOff);
  EXPECT_EQ(scenario.creates, 1u);
  EXPECT_EQ(scenario.syncCalls, 1u);
  EXPECT_EQ(scenario.makeResults, 1u);
  ASSERT_NE(scenario.operation, nullptr);
  EXPECT_EQ(object.root.refs, 1u);
  void *recycled = nullptr;
  ASSERT_TRUE(concurrentQueuePop(&backend.operationPool, &recycled));
  EXPECT_EQ(recycled, &scenario.operation->root);
  objectDelete(&object.root);
}

TEST(core_sync_path, busy_combiner_publishes_async_node_without_sync_attempt)
{
  TestBackend backend;
  TestObject object(backend);
  SyncScenario scenario(object);
  TestOp active(object);
  active.setResults({aosPending});
  active.executeHook = [&](TestOp &op) {
    if (op.executeCalls == 1)
      runSyncScenario(scenario, afNone, &scenario);
  };

  combinerPushOperation(&active.root);

  EXPECT_EQ(scenario.syncCalls, 0u);
  EXPECT_EQ(scenario.creates, 1u);
  ASSERT_NE(scenario.operation, nullptr);
  EXPECT_EQ(listContents(object.root.readQueue),
            (std::vector<asyncOpRoot*>{&active.root, &scenario.operation->root}));
  cancelIo(&object.root);
  backend.drainCompletions();
  objectDelete(&object.root);
}

TEST(core_io_path, inline_result_returns_without_yield_or_allocation)
{
  TestBackend backend;
  TestObject object(backend);
  IoScenario scenario(object);
  coroutineTy *coroutine = startIoScenario(scenario);

  ASSERT_NE(coroutine, nullptr);
  EXPECT_EQ(coroutineCall(coroutine), 1);
  EXPECT_TRUE(scenario.returned);
  EXPECT_EQ(scenario.result, nullptr);
  EXPECT_EQ(scenario.sync.syncCalls, 1u);
  EXPECT_EQ(scenario.sync.creates, 0u);
  objectDelete(&object.root);
}

TEST(core_io_path, fairness_budget_queues_then_resumes_coroutine)
{
  TestBackend backend;
  TestObject object(backend);
  IoScenario scenario(object);
  scenario.flags = afRealtime | afWaitAll;
  currentFinishedSync = MAX_SYNCHRONOUS_FINISHED_OPERATION - 1;
  coroutineTy *coroutine = startIoScenario(scenario);

  ASSERT_NE(coroutine, nullptr);
  EXPECT_EQ(coroutineCall(coroutine), 0);
  EXPECT_FALSE(scenario.returned);
  EXPECT_EQ(scenario.sync.creates, 1u);
  EXPECT_EQ(scenario.sync.initCalls, 1u);
  ASSERT_NE(scenario.sync.operation, nullptr);
  EXPECT_EQ(scenario.sync.operation->root.flags & (afRealtime | afWaitAll),
            afRealtime | afWaitAll)
    << "the fairness fallback must preserve caller flags";
  deliverCoroutineCompletion(backend);
  EXPECT_TRUE(scenario.returned);
  EXPECT_EQ(scenario.status, aosSuccess);
  objectDelete(&object.root);
}

TEST(core_io_path, terminal_sync_operation_queues_then_resumes_coroutine)
{
  TestBackend backend;
  TestObject object(backend);
  IoScenario scenario(object);
  scenario.sync.result = SyncResult::Terminal;
  coroutineTy *coroutine = startIoScenario(scenario);

  ASSERT_NE(coroutine, nullptr);
  EXPECT_EQ(coroutineCall(coroutine), 0);
  EXPECT_EQ(scenario.sync.syncCalls, 1u);
  deliverCoroutineCompletion(backend);
  EXPECT_TRUE(scenario.returned);
  EXPECT_EQ(scenario.status, aosSuccess);
  objectDelete(&object.root);
}

TEST(core_io_path, pending_sync_operation_yields_until_cancel_completion)
{
  TestBackend backend;
  TestObject object(backend);
  IoScenario scenario(object);
  scenario.opCode = OPCODE_WRITE;
  scenario.sync.result = SyncResult::Pending;
  coroutineTy *coroutine = startIoScenario(scenario);

  ASSERT_NE(coroutine, nullptr);
  EXPECT_EQ(coroutineCall(coroutine), 0);
  ASSERT_NE(scenario.sync.operation, nullptr);
  EXPECT_EQ(scenario.sync.operation->root.running, arRunning);
  EXPECT_TRUE(backend.completions.empty());

  cancelIo(&object.root);
  deliverCoroutineCompletion(backend);
  EXPECT_TRUE(scenario.returned);
  EXPECT_EQ(scenario.status, aosCanceled);
  objectDelete(&object.root);
}

TEST(core_global_queue, coroutine_completion_resumes_and_recycles_operation)
{
  TestBackend backend;
  TestObject object(backend);
  IoScenario scenario(object);
  scenario.sync.result = SyncResult::Terminal;
  coroutineTy *coroutine = startIoScenario(scenario);

  ASSERT_NE(coroutine, nullptr);
  ASSERT_EQ(coroutineCall(coroutine), 0);
  ASSERT_EQ(backend.completions.size(), 1u);
  asyncOpRoot *op = backend.completions.front();
  backend.completions.clear();
  concurrentQueuePush(&backend.base.globalQueue, op);

  EXPECT_EQ(executeGlobalQueue(&backend.base), 1);
  EXPECT_TRUE(scenario.returned);
  EXPECT_EQ(scenario.status, aosSuccess);
  objectDelete(&object.root);
}

TEST(core_delete_lifecycle, sync_started_before_close_may_finish)
{
  TestBackend backend;
  TestObject object(backend);
  objectIncrementReference(&object.root, 1);
  SyncScenario scenario(object);
  scenario.deleteDuringSync = true;

  runSyncScenario(scenario, afNone);

  EXPECT_EQ(scenario.syncCalls, 1u);
  EXPECT_EQ(scenario.makeResults, 1u);
  EXPECT_EQ(object.root.DeletePending, 1u);
  EXPECT_EQ(object.destructorCallbacks, 0u);
  objectDecrementReference(&object.root, 1);
  EXPECT_EQ(object.destructorCallbacks, 1u);
}

TEST(core_delete_lifecycle, next_sync_operation_after_close_is_rejected)
{
  TestBackend backend;
  TestObject object(backend);
  objectIncrementReference(&object.root, 1);
  objectDelete(&object.root);
  ASSERT_EQ(object.root.DeletePending, 1u);
  SyncScenario scenario(object);

  runSyncScenario(scenario, afNone, &scenario);

  EXPECT_EQ(scenario.syncCalls, 0u)
    << "combinerAcquire entered syncImpl after the object became closing";
  EXPECT_EQ(scenario.makeResults, 0u);
  ASSERT_EQ(scenario.creates, 1u);
  // The rejected operation is routed through the combiner and dies in the
  // sticky delete sweep: exactly one canceled completion, no syscall
  backend.drainCompletions();
  EXPECT_EQ(opGetStatus(&scenario.operation->root), aosCanceled);
  EXPECT_EQ(scenario.operation->finishCalls, 1u);
  objectDecrementReference(&object.root, 1);
}


inline uint128Pair peekWheelSlot(TestBackend &backend, unsigned level, unsigned index)
{
  return __uint128_atomic_load(&backend.base.timerWheel.slots[level][index]);
}

// Detach a slot's list without delivering it: diagnostic recovery for tests
// that deliberately leave a link behind the checkpoint
inline asyncOpListLink *drainWheelSlot(TestBackend &backend, unsigned level, unsigned index)
{
  volatile uint128Pair *slot = &backend.base.timerWheel.slots[level][index];
  uint128Pair observed = __uint128_atomic_load(slot);
  uint128Pair desired;
  do {
    desired.low = 0;
    desired.high = observed.high;
  } while (!__uint128_atomic_compare_and_swap(slot, &observed, desired));
  return reinterpret_cast<asyncOpListLink*>(static_cast<uintptr_t>(observed.low));
}

TEST(core_timeout_grid, rounds_deadlines_up_and_delivers_each_window_once)
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

TEST(core_timeout_grid, expiration_cancels_and_releases_waiting_operation)
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
TEST(core_timeout_grid, late_arm_after_swept_checkpoint_expires_instead_of_being_stranded)
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
  EXPECT_EQ(statusAfterArm, aosTimeout)
    << "a timer armed after its window was swept was not expired immediately";
  EXPECT_EQ(stranded, nullptr)
    << "the late timer was published into the reopened slot and will fire a rotation late";

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

TEST(core_timeout_grid, current_or_stale_clock_reading_is_a_noop)
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

TEST(core_timeout_grid, stale_generation_link_does_not_cancel_reused_operation)
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
  link->next = nullptr;
  link->deadlineTick = 7;
  timerWheelInsert(&backend.base, link, 7);

  processTimeoutQueue(&backend.base, 8);

  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_EQ(object.root.Head.data, 0u);
  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(core_timeout_grid, ordinary_operation_arms_monotonic_grid_timer)
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

TEST(core_timer_wheel, detach_reopens_and_is_idempotent_per_incarnation)
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
  uint128Pair slotPair = peekWheelSlot(backend, 0, 5);
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

TEST(core_timer_wheel, arm_behind_multiple_rotations_expires_without_publication)
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

TEST(core_timer_wheel, publication_descends_into_open_lower_window_of_drained_upper)
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
  link->next = nullptr;
  link->deadlineTick = 1030;
  ASSERT_EQ(timerWheelInsert(&backend.base, link, 0), 1);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).low,
            reinterpret_cast<uint64_t>(link));
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);

  // Delivered by the normal sweep in its exact tick
  processTimeoutQueue(&backend.base, 1030);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  processTimeoutQueue(&backend.base, 1031);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(core_timer_wheel, expired_arm_never_starts_initialization_io)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_WRITE, afNone, 1);
  op.setResults({aosPending});
  // A connect whose timeout elapsed before the submission reached the
  // combiner: the deadline computed from the monotonic clock lies behind the
  // confirmed sweep position
  backend.base.timerCloseCursor = static_cast<uintptr_t>(getMonotonicTicks() + 1024);
  ASSERT_TRUE(__uintptr_atomic_compare_and_swap(&object.root.initializationOp, 0,
                                                reinterpret_cast<uintptr_t>(&op.root)));

  combinerPushOperation(&op.root);

  EXPECT_EQ(op.executeCalls, 0u)
    << "initialization I/O was issued after its timeout had already won";
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(__uintptr_atomic_load(&object.root.initializationOp, amoRelaxed), 0u);
  backend.drainCompletions();
  EXPECT_EQ(op.callbackStatus, aosTimeout);
  objectDelete(&object.root);
}

TEST(core_timer_wheel, helper_confirms_cursor_and_stalled_owner_keeps_its_chain)
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

TEST(core_timer_wheel, resurfaced_sweeper_with_stale_tick_cannot_rewind_cursor)
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

TEST(core_timer_wheel, boundary_deadline_migrates_and_fires_in_its_exact_tick)
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
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).low,
            reinterpret_cast<uint64_t>(inWindow.root.timerId));

  processTimeoutQueue(&backend.base, 1026);
  EXPECT_EQ(opGetStatus(&inWindow.root), aosTimeout);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(core_timer_wheel, cascade_chain_migrates_l2_to_l1_to_l0_and_fires_in_its_exact_tick)
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
  ASSERT_EQ(peekWheelSlot(backend, 2, 1).low,
            reinterpret_cast<uint64_t>(chained.root.timerId));
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
  ASSERT_EQ(peekWheelSlot(backend, 1, 1).low,
            reinterpret_cast<uint64_t>(chained.root.timerId));
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).high, l2Window + TIMER_WHEEL_SLOTS);

  // The level-1 boundary tick migrates it again, into the level-0 slot whose
  // current incarnation covers exactly the deadline tick
  processTimeoutQueue(&backend.base, chainedDeadline);
  EXPECT_EQ(opGetStatus(&chained.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 1, 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 0, 1).low,
            reinterpret_cast<uint64_t>(chained.root.timerId));
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).high, chainedDeadline);

  // The last hop is the delivery itself, in the exact deadline tick
  processTimeoutQueue(&backend.base, chainedDeadline + 1);
  EXPECT_EQ(opGetStatus(&chained.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1).low, 0u);

  objectDecrementReference(&object.root, 2);
  objectDelete(&object.root);
}

TEST(core_timer_wheel, boundary_deadline_at_top_level_window_start_fires_at_its_visit)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object);
  // Differs from cursor 0 in the bits 30..39 group: top-level routing, into
  // the [2^30, 2^31) window whose slot still holds its initial incarnation
  const uint64_t l3Window = uint64_t{1} << (3 * TIMER_WHEEL_LEVEL_BITS);
  op.root.endTime = l3Window * TIMER_TICK_MICROSECONDS;
  addToTimeoutQueue(&backend.base, &op.root);
  ASSERT_EQ(peekWheelSlot(backend, 3, 1).low,
            reinterpret_cast<uint64_t>(op.root.timerId));

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

TEST(core_timer_wheel, beyond_range_deadline_clamps_and_reclamps_keeping_its_exact_tick)
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
  ASSERT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 1).low,
            reinterpret_cast<uint64_t>(link));

  // The far slot's visit re-cascades; from this window's position the
  // deadline is still beyond range, so it clamps again - parked with its
  // exact deadline tick intact, neither delivered early nor dropped
  const uint64_t farWindow = (TIMER_WHEEL_SLOTS - 1) * topWidth;
  timerWheelProcessDetached(&backend.base, timerWheelDetach(&backend.base, 3, farWindow), farWindow);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);
  EXPECT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 1).low, 0u);
  ASSERT_EQ(peekWheelSlot(backend, 3, TIMER_WHEEL_SLOTS - 2).low,
            reinterpret_cast<uint64_t>(link));
  EXPECT_EQ(link->deadlineTick, wheelRange + 5);

  // Still pending and parked: the wheel teardown recycles the link without
  // delivering it
  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(core_timer_wheel, stalled_cascade_into_swept_window_delivers_exactly_once)
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
  ASSERT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).high,
            1030u + TIMER_WHEEL_SLOTS);
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  // The resumed cascade observes the terminal window and must deliver right
  // now (late, never early - the deadline tick is already swept), not park
  // the link into the reopened incarnation to fire a rotation later
  timerWheelProcessDetached(&backend.base, ownedChain, 1024);
  EXPECT_EQ(opGetStatus(&op.root), aosTimeout);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).low, 0u);
  EXPECT_EQ(peekWheelSlot(backend, 0, 1030 % TIMER_WHEEL_SLOTS).high,
            1030u + TIMER_WHEEL_SLOTS);

  objectDecrementReference(&object.root, 1);
  objectDelete(&object.root);
}

TEST(core_realtime_timer, terminal_completion_starts_and_stops_backend_timer)
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

TEST(core_realtime_timer, timeout_completion_does_not_stop_fired_timer)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  op.root.timerId = reinterpret_cast<void*>(1);
  op.setResults({aosPending});
  combinerPushOperation(&op.root);

  opCancel(&op.root, opGetGeneration(&op.root), aosTimeout);

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
TEST(core_reactor_timer, stale_doorbell_must_not_expire_a_rearmed_operation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{};  // production timers come from alignedMalloc
  timer.root.base = &backend.base;
  timer.root.type = ioObjectTimer;
  timer.op = &op.root;
  timer.fd = static_cast<intptr_t>(uintptr_t{7} << rtIdentSeqBits);  // creation seeds the base id
  op.root.timerId = &timer;

  // The first incarnation arms its realtime timer; the kernel keeps both the
  // udata and the ident of this arming inside the harvested event
  void *udata = reactorTimerArmIdent(&timer, &op.root);
  uintptr_t staleIdent = static_cast<uintptr_t>(timer.fd);
  void *decodedTimer = nullptr;
  uintptr_t staleBits = 0;
  __tagged_pointer_decode(udata, &decodedTimer, &staleBits);
  ASSERT_EQ(decodedTimer, &timer);

  // The operation completes; its storage is recycled and re-armed for a new
  // submission with a fresh deadline before the doorbell gets processed
  uintptr_t firstGeneration = opGetGeneration(&op.root);
  op.root.tag = ((firstGeneration + 1) << TAG_STATUS_SIZE) | aosPending;
  reactorTimerArmIdent(&timer, &op.root);

  // Another loop thread finally processes the stale doorbell
  uintptr_t armedGeneration = 0;
  EXPECT_EQ(reactorTimerDecodeIdent(&timer, staleBits, staleIdent, &armedGeneration), rteIgnore)
    << "a doorbell carrying the previous arming's ident matched the re-armed timer";
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  // The current arming's own doorbell still expires it, with its generation
  EXPECT_EQ(reactorTimerDecodeIdent(&timer, staleBits, static_cast<uintptr_t>(timer.fd), &armedGeneration),
            rteExpireOperation);
  EXPECT_EQ(armedGeneration, opGetGeneration(&op.root));

  reactorTimerDisarm(&timer);
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

// Same scenario through the epoll/count protocol: the re-arm's settime reset
// the fd's expiration counter, so the stale doorbell reads 0 and is dropped;
// a positive count proves the CURRENT arming expired, and expiring it with
// its own generation is correct whoever's envelope delivered the wakeup.
TEST(core_reactor_timer, stale_count_doorbell_must_not_expire_a_rearmed_operation)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{};  // production timers come from alignedMalloc
  timer.root.base = &backend.base;
  timer.root.type = ioObjectTimer;
  timer.op = &op.root;
  op.root.timerId = &timer;

  void *udata = reactorTimerArmCount(&timer, &op.root);
  void *decodedTimer = nullptr;
  uintptr_t staleBits = 0;
  __tagged_pointer_decode(udata, &decodedTimer, &staleBits);
  ASSERT_EQ(decodedTimer, &timer);

  uintptr_t firstGeneration = opGetGeneration(&op.root);
  op.root.tag = ((firstGeneration + 1) << TAG_STATUS_SIZE) | aosPending;
  reactorTimerArmCount(&timer, &op.root);

  uintptr_t armedTag = reactorTimerArmedCountTag(&timer);
  ASSERT_NE(armedTag, 0u);
  uintptr_t armedGeneration = 0;
  EXPECT_EQ(reactorTimerDecodeCount(armedTag, staleBits, 0, &armedGeneration), rteIgnore)
    << "a stale doorbell of the re-armed timer expired an operation whose deadline is ahead";
  EXPECT_EQ(opGetStatus(&op.root), aosPending);

  EXPECT_EQ(reactorTimerDecodeCount(armedTag, staleBits, 1, &armedGeneration), rteExpireOperation);
  EXPECT_EQ(armedGeneration, opGetGeneration(&op.root));

  reactorTimerDisarm(&timer);
  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

// User events restart with an UNCHANGED operation generation, so the arming
// identity - not the generation - must distinguish restarts (the old
// "compare timer and event tag" TODO). kqueue/ident protocol: every arming
// takes a fresh ident, a doorbell of the previous one no longer matches.
TEST(core_reactor_timer, stale_doorbell_must_not_activate_a_restarted_user_event)
{
  aioUserEvent event{};
  event.root.opCode = actUserEvent;
  event.root.tag = uintptr_t{1} << TAG_STATUS_SIZE;  // generation 1, stable across restarts
  event.tag = 1;
  event.counter = 5;
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{};  // production timers come from alignedMalloc
  timer.op = &event.root;
  timer.fd = static_cast<intptr_t>(uintptr_t{2} << rtIdentSeqBits);
  event.root.timerId = &timer;

  void *udata = reactorTimerArmIdent(&timer, &event.root);   // first arming
  uintptr_t staleIdent = static_cast<uintptr_t>(timer.fd);
  void *decodedTimer = nullptr;
  uintptr_t staleBits = 0;
  __tagged_pointer_decode(udata, &decodedTimer, &staleBits);

  reactorTimerDisarm(&timer);                                // userEventStopTimer
  reactorTimerArmIdent(&timer, &event.root);                 // userEventStartTimer again

  uintptr_t armedGeneration = 0;
  EXPECT_EQ(reactorTimerDecodeIdent(&timer, staleBits, staleIdent, &armedGeneration), rteIgnore)
    << "a doorbell of the previous arming would activate the restarted user event and consume its tick";

  // The current arming's own tick still delivers
  EXPECT_EQ(reactorTimerDecodeIdent(&timer, staleBits, static_cast<uintptr_t>(timer.fd), &armedGeneration),
            rteUserEvent);
}

// Same restart through the epoll/count protocol: the restart's settime reset
// the counter - an early doorbell of the previous arming reads 0 and is
// dropped instead of consuming a counted tick ahead of schedule.
TEST(core_reactor_timer, stale_count_doorbell_must_not_activate_a_restarted_user_event)
{
  aioUserEvent event{};
  event.root.opCode = actUserEvent;
  event.root.tag = uintptr_t{1} << TAG_STATUS_SIZE;
  event.tag = 1;
  event.counter = 5;
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{};  // production timers come from alignedMalloc
  timer.op = &event.root;
  event.root.timerId = &timer;

  void *udata = reactorTimerArmCount(&timer, &event.root);   // first arming
  void *decodedTimer = nullptr;
  uintptr_t staleBits = 0;
  __tagged_pointer_decode(udata, &decodedTimer, &staleBits);

  reactorTimerDisarm(&timer);                                // userEventStopTimer
  reactorTimerArmCount(&timer, &event.root);                 // userEventStartTimer again

  uintptr_t armedTag = reactorTimerArmedCountTag(&timer);
  ASSERT_NE(armedTag, 0u);
  uintptr_t armedGeneration = 0;
  EXPECT_EQ(reactorTimerDecodeCount(armedTag, staleBits, 0, &armedGeneration), rteIgnore)
    << "an early doorbell of the previous arming consumed a tick of the restarted event";
  EXPECT_EQ(reactorTimerDecodeCount(armedTag, staleBits, 1, &armedGeneration), rteUserEvent);
}

// The "tag == 0 means disarmed" sentinel must not collide with a legally
// wrapped generation. On a 32-bit target the generation field is 24 bits
// wide: after 2^24 recycles of one operation slot initAsyncOpRoot wraps it
// to zero - the armed encoding must stay disjoint from the sentinel anyway,
// or every delivery reads "stale doorbell" and the timeout is dropped
// forever (the operation hangs). kqueue: the ident's seq part skips 0;
// epoll: the armed bit in (generation << 1) | 1.
TEST(core_reactor_timer, arming_a_wrapped_generation_must_not_publish_the_disarmed_sentinel)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{};  // production timers come from alignedMalloc
  timer.op = &op.root;
  timer.fd = 0;  // even a zero base id must not let the composite ident hit 0
  op.root.timerId = &timer;

  // The slot's generation legally wraps around to zero (2^24 recycles on a
  // 32-bit target)
  op.root.tag = (uintptr_t{0} << TAG_STATUS_SIZE) | aosPending;
  void *udata = reactorTimerArmIdent(&timer, &op.root);
  void *decodedTimer = nullptr;
  uintptr_t udataBits = 0;
  __tagged_pointer_decode(udata, &decodedTimer, &udataBits);

  uintptr_t armedGeneration = ~uintptr_t{0};
  EXPECT_NE(static_cast<uintptr_t>(timer.fd), 0u);
  EXPECT_NE(reactorTimerDecodeIdent(&timer, udataBits, static_cast<uintptr_t>(timer.fd), &armedGeneration),
            rteIgnore)
    << "an armed timer with generation 0 is indistinguishable from a disarmed one: its timeout is lost";
  EXPECT_EQ(armedGeneration, 0u);
  reactorTimerDisarm(&timer);

  // epoll flavor: the armed bit keeps generation 0 away from the sentinel
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer countTimer{};  // production timers come from alignedMalloc
  countTimer.op = &op.root;
  op.root.timerId = &countTimer;
  reactorTimerArmCount(&countTimer, &op.root);
  uintptr_t armedTag = reactorTimerArmedCountTag(&countTimer);
  EXPECT_NE(armedTag, 0u)
    << "the armed count encoding of generation 0 collided with the disarmed sentinel";
  armedGeneration = ~uintptr_t{0};
  EXPECT_EQ(reactorTimerDecodeCount(armedTag, udataBits, 1, &armedGeneration), rteExpireOperation);
  EXPECT_EQ(armedGeneration, 0u);
  reactorTimerDisarm(&countTimer);

  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

#ifdef __linux__
// The kernel dependency the count protocol stands on: timerfd_settime with a
// new value RESETS the fd's expiration counter, so a doorbell of a previous
// arming reads 0 after a re-arm instead of inheriting stale expirations. If
// this ever breaks on a target kernel, reactorTimerDecodeCount loses its
// oracle and the stop-side drain needs rethinking.
TEST(core_reactor_timer, timerfd_settime_resets_the_expiration_counter)
{
  int fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  ASSERT_NE(fd, -1);
  itimerspec its{};
  its.it_value.tv_nsec = 1000000;  // 1ms
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
  EXPECT_TRUE(result == -1 && errno == EAGAIN)
    << "settime did not reset the expiration counter: read returned " << result
    << " with count " << expirations << " - a stale doorbell would expire the fresh arming";

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

// Regression: a user-event tick whose activation collided with a concurrent
// userEventActivate (the AIOPing pattern) is dropped - but on a backend whose
// registration is consumed by every delivery (epoll EPOLLONESHOT) the
// registration must still be re-armed. Losing the only oneshot registration
// silences the periodic event forever: the kernel timer keeps ticking, epoll
// never reports it again until a manual userEventStartTimer.
TEST(core_reactor_timer, failed_activation_must_still_rearm_a_oneshot_registration)
{
  ReactorUserEventTickPlan plan = reactorTimerUserEventTick(0, 0, 1);
  EXPECT_EQ(plan.activate, 0);
  EXPECT_EQ(plan.stopTimer, 0);
  EXPECT_EQ(plan.rearm, 1)
    << "a dropped tick consumed the oneshot registration without re-arming it";

  // kqueue: the periodic kernel timer stays armed, a dropped tick needs no action
  plan = reactorTimerUserEventTick(0, 0, 0);
  EXPECT_EQ(plan.activate, 0);
  EXPECT_EQ(plan.stopTimer, 0);
  EXPECT_EQ(plan.rearm, 0);
}

// Pins the green user-event delivery paths of both backends around the plan.
TEST(core_reactor_timer, user_event_tick_plan_pins_the_green_paths)
{
  // Counted tick with ticks remaining: deliver, re-arm the consumed oneshot
  ReactorUserEventTickPlan plan = reactorTimerUserEventTick(1, 0, 1);
  EXPECT_EQ(plan.activate, 1);
  EXPECT_EQ(plan.stopTimer, 0);
  EXPECT_EQ(plan.rearm, 1);

  // Final counted tick: stop the timer, the dead registration is not re-armed
  plan = reactorTimerUserEventTick(1, 1, 1);
  EXPECT_EQ(plan.activate, 1);
  EXPECT_EQ(plan.stopTimer, 1);
  EXPECT_EQ(plan.rearm, 0);

  // kqueue twins: never re-arm, the kernel-side periodic timer is standing
  plan = reactorTimerUserEventTick(1, 0, 0);
  EXPECT_EQ(plan.activate, 1);
  EXPECT_EQ(plan.stopTimer, 0);
  EXPECT_EQ(plan.rearm, 0);
  plan = reactorTimerUserEventTick(1, 1, 0);
  EXPECT_EQ(plan.activate, 1);
  EXPECT_EQ(plan.stopTimer, 1);
  EXPECT_EQ(plan.rearm, 0);
}

// Every kernel publication of a timer pointer must carry the udataTimer
// discriminator - including the disabled registrations (epoll InitializeTimer
// CTL_ADD with mask 0, StopTimer CTL_MOD to mask 0): EPOLLERR/EPOLLHUP are
// mask-exempt, and a bare timer pointer would route such an event into the
// fd-object branch, pushing a combiner signal through the aioTimer's
// never-initialized Head. The seam owns the single udata constructor; this
// pins its bits for every flavor.
TEST(core_reactor_timer, every_timer_udata_publication_carries_the_timer_bit)
{
  TestBackend backend;
  TestObject object(backend);
  TestOp op(object, OPCODE_READ, afRealtime, 10);
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer timer{};  // production timers come from alignedMalloc
  timer.op = &op.root;
  op.root.timerId = &timer;

  auto bitsOf = [](void *udata, aioTimer *expected) {
    void *ptr = nullptr;
    uintptr_t bits = 0;
    __tagged_pointer_decode(udata, &ptr, &bits);
    EXPECT_EQ(ptr, expected) << "udata does not point back at the timer";
    return bits;
  };

  // The registration-only udata (disabled epoll registrations)
  EXPECT_EQ(bitsOf(reactorTimerUdata(&timer, 0), &timer) & udataTimer, uintptr_t{udataTimer});
  EXPECT_EQ(bitsOf(reactorTimerUdata(&timer, 1), &timer) & (udataTimer | udataUserEvent),
            uintptr_t{udataTimer | udataUserEvent});

  // Both arm flavors of a realtime operation: timer bit set, user-event bit clear
  uintptr_t identBits = bitsOf(reactorTimerArmIdent(&timer, &op.root), &timer);
  EXPECT_EQ(identBits & udataTimer, uintptr_t{udataTimer});
  EXPECT_EQ(identBits & udataUserEvent, 0u);
  reactorTimerDisarm(&timer);
  uintptr_t countBits = bitsOf(reactorTimerArmCount(&timer, &op.root), &timer);
  EXPECT_EQ(countBits & udataTimer, uintptr_t{udataTimer});
  EXPECT_EQ(countBits & udataUserEvent, 0u);
  reactorTimerDisarm(&timer);

  // User-event arms additionally mark the udata so the decode can branch
  // without touching the operation's memory
  aioUserEvent event{};
  event.root.opCode = actUserEvent;
  event.root.tag = uintptr_t{1} << TAG_STATUS_SIZE;
  alignas(TAGGED_POINTER_ALIGNMENT) aioTimer eventTimer{};  // production timers come from alignedMalloc
  eventTimer.op = &event.root;
  event.root.timerId = &eventTimer;
  EXPECT_EQ(bitsOf(reactorTimerArmIdent(&eventTimer, &event.root), &eventTimer) & (udataTimer | udataUserEvent),
            uintptr_t{udataTimer | udataUserEvent});
  reactorTimerDisarm(&eventTimer);
  EXPECT_EQ(bitsOf(reactorTimerArmCount(&eventTimer, &event.root), &eventTimer) & (udataTimer | udataUserEvent),
            uintptr_t{udataTimer | udataUserEvent});
  reactorTimerDisarm(&eventTimer);

  objectDecrementReference(&object.root, 1);
  deleteOwner(backend, object);
}

} // namespace
