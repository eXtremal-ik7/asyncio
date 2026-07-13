#include "coretest.h"

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"

#include <algorithm>
#include <atomic>
#include <cstring>
#include <memory>
#include <thread>

namespace {

static_assert(COMBINER_TAG_PROGRESS_READ == IO_EVENT_READ, "READ tag must map directly to backend events");
static_assert(COMBINER_TAG_PROGRESS_WRITE == IO_EVENT_WRITE, "WRITE tag must map directly to backend events");
static_assert(COMBINER_TAG_ERROR == IO_EVENT_ERROR, "ERROR tag must map directly to backend events");
static_assert(COMBINER_TAG_CANCEL == (1u << 3), "CANCEL must precede DELETE");
static_assert(COMBINER_TAG_DELETE == (1u << 4), "DELETE must remain the final Head tag");

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

TEST(core_grace_period, retirement_kicks_a_loop_only_from_outside_and_once_per_episode)
{
  TestBackend backend;
  TestObject first(backend), second(backend), third(backend);

  // graceLoopBase is unset (an application thread): the retirement that
  // finds the limbo empty opens the episode and wakes one sleeper - parked
  // loops wait with no timeout and cannot notice the limbo on their own
  graceRetire(&backend.base, &first.root, TestObject::memoryRelease);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  // A follow-up retirement rides on the episode's kick: a woken loop polls
  // at the drain period until the limbo is empty
  graceRetire(&backend.base, &second.root, TestObject::memoryRelease);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  // No slot was ever claimed, so the batch ripens within one scan
  graceReclaim(&backend.base);
  EXPECT_EQ(first.memoryReleases, 1u);
  EXPECT_EQ(second.memoryReleases, 1u);

  // The next retirement after the drain opens a new episode
  graceRetire(&backend.base, &third.root, TestObject::memoryRelease);
  EXPECT_EQ(backend.wakeupCalls, 2u);
  graceReclaim(&backend.base);
  EXPECT_EQ(third.memoryReleases, 1u);
}

TEST(core_grace_period, retirement_from_the_bases_own_loop_thread_does_not_kick)
{
  TestBackend backend;
  TestObject object(backend);
  graceThreadEnter(&backend.base);   // claims slot 0, takes the loop identity

  // The caller is a loop thread of this base: it is awake and its next
  // sleep decision sees the non-empty limbo itself - a kick would be a
  // wasted syscall on the hot delete path
  graceRetire(&backend.base, &object.root, TestObject::memoryRelease);
  EXPECT_EQ(backend.wakeupCalls, 0u);

  graceThreadExit(&backend.base);    // stamps out and drains as the last thread
  EXPECT_EQ(object.memoryReleases, 1u);
}

TEST(core_grace_period, retirement_into_a_foreign_base_counts_as_outside)
{
  TestBackend own, foreign;
  TestObject object(foreign);
  graceThreadEnter(&own.base);       // a loop thread - but of another base

  graceRetire(&foreign.base, &object.root, TestObject::memoryRelease);
  EXPECT_EQ(foreign.wakeupCalls, 1u);
  EXPECT_EQ(own.wakeupCalls, 0u);

  graceThreadExit(&own.base);
  graceReclaim(&foreign.base);
  EXPECT_EQ(object.memoryReleases, 1u);
}

TEST(core_grace_period, reclaim_chases_a_blocked_batch_with_one_kick_per_pass)
{
  TestBackend backend;
  TestObject object(backend);

  // Claim slots 0 and 1 (the mock plays both loop threads from this one):
  // a captured batch then waits on both quiescence counters
  messageLoopThreadId = 0;
  graceThreadEnter(&backend.base);
  messageLoopThreadId = 1;
  graceThreadEnter(&backend.base);

  // Both loops park in unbounded waits; the delete comes from an
  // application thread and opens the episode with a kick
  backend.sleepSlots[0].wakeTick = timerSleepEternal;
  backend.sleepSlots[1].wakeTick = timerSleepEternal;
  graceLoopBase = nullptr;
  graceRetire(&backend.base, &object.root, TestObject::memoryRelease);
  EXPECT_EQ(backend.wakeupCalls, 1u);

  // The scan captures the batch against both live counters and cannot ripen
  // it: one chase kick per pass, aimed only while a blocker actually sleeps
  graceReclaim(&backend.base);
  ASSERT_EQ(backend.base.gracePending, &object.root);
  EXPECT_EQ(backend.wakeupCalls, 2u);
  EXPECT_EQ(object.memoryReleases, 0u);

  // Thread 0 wakes on the kick, ticks and scans: the batch is still blocked
  // by sleeping thread 1, so the chain passes the kick on
  messageLoopThreadId = 0;
  backend.sleepSlots[0].wakeTick = UINTPTR_MAX;
  graceQuiesce(&backend.base);
  EXPECT_EQ(backend.wakeupCalls, 3u);
  EXPECT_EQ(object.memoryReleases, 0u);

  // Thread 1 wakes and ticks: every captured counter has moved, the batch
  // ripens and an unblocked grace attracts no further kicks
  messageLoopThreadId = 1;
  backend.sleepSlots[1].wakeTick = UINTPTR_MAX;
  graceQuiesce(&backend.base);
  EXPECT_EQ(object.memoryReleases, 1u);
  EXPECT_EQ(backend.wakeupCalls, 3u);

  graceThreadExit(&backend.base);    // stamps out slot 1, the identity held
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
  bool handedOff = __uintptr_atomic_compare_and_swap(&object.root.Head.data,
                                                      busy.data,
                                                      0,
                                                      amoSeqCst);
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


} // namespace
