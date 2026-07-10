#include "unittest.h"

#include "asyncio/socket.h"
#include "atomic.h"

#include <algorithm>
#include <thread>

// The lifetime suite pins the destruction contract that callers rely on to
// free their own state:
// - every submitted operation reports exactly once; cancellation delivers
//   aosCanceled through the regular callback, nothing is silently dropped;
// - the destructor callback fires exactly once, strictly after the last
//   operation callback of the object, and no callback fires after it;
// - cancelIo cancels everything pending but leaves the object usable.
// Everything here is deterministic on a single loop thread: completions and
// the delete tag travel through the global queue in submission order, so the
// destructor is always the last word. The concurrent side of the same
// contract (destruction racing a combiner held by another thread) has no
// deterministic interleaving reachable from the public API and is exercised
// by the lifetimetest stress binary instead.
struct LifetimeContext {
  asyncBase *base;
  unsigned canceledCallbacks = 0;
  unsigned successCallbacks = 0;
  unsigned unexpectedCallbacks = 0;
  unsigned callbacksAfterDestructor = 0;
  unsigned destructorCalls = 0;
  unsigned eventFires = 0;
  AsyncOpStatus connectStatus = aosUnknown;
  LifetimeContext(asyncBase *baseArg) : base(baseArg) {}
};

static void lifetimeAccount(LifetimeContext *ctx, AsyncOpStatus status)
{
  if (ctx->destructorCalls)
    ctx->callbacksAfterDestructor++;
  if (status == aosCanceled)
    ctx->canceledCallbacks++;
  else if (status == aosSuccess)
    ctx->successCallbacks++;
  else
    ctx->unexpectedCallbacks++;
}

static void lifetimeReadMsgCb(AsyncOpStatus status, aioObject*, HostAddress, size_t, void *arg)
{
  lifetimeAccount(static_cast<LifetimeContext*>(arg), status);
}

static void lifetimeIoCb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  lifetimeAccount(static_cast<LifetimeContext*>(arg), status);
}

static void lifetimeConnectCb(AsyncOpStatus status, aioObject*, void *arg)
{
  LifetimeContext *ctx = static_cast<LifetimeContext*>(arg);
  if (ctx->destructorCalls)
    ctx->callbacksAfterDestructor++;
  ctx->connectStatus = status;
}

static void lifetimeDestructorCb(aioObjectRoot*, void *arg)
{
  LifetimeContext *ctx = static_cast<LifetimeContext*>(arg);
  ctx->destructorCalls++;
  postQuitOperation(ctx->base);
}

TEST(lifetime, delete_with_parked_reads)
{
  LifetimeContext context(gBase);
  aioObject *object = initializeUDPClient(gBase);
  ASSERT_NE(object, nullptr);
  objectSetDestructorCb(aioObjectHandle(object), lifetimeDestructorCb, &context);

  // The timeouts protect the test, not the contract: were cancellation to
  // lose an operation, it would surface as aosTimeout in unexpectedCallbacks
  // instead of hanging the loop forever
  constexpr unsigned opsNum = 16;
  static uint32_t buffers[opsNum];
  for (unsigned i = 0; i < opsNum; i++)
    aioReadMsg(object, &buffers[i], sizeof(uint32_t), afNone, 3000000, lifetimeReadMsgCb, &context);
  deleteAioObject(object);

  asyncLoop(gBase);
  EXPECT_EQ(context.canceledCallbacks, opsNum);
  EXPECT_EQ(context.successCallbacks, 0u);
  EXPECT_EQ(context.unexpectedCallbacks, 0u);
  EXPECT_EQ(context.destructorCalls, 1u);
  EXPECT_EQ(context.callbacksAfterDestructor, 0u);
}

// The successful probe read deletes the object from its own callback, so the
// destructor ordering is exercised from a loop-thread callback as well
static void lifetimeProbeReadCb(AsyncOpStatus status, aioObject *socket, HostAddress, size_t transferred, void *arg)
{
  LifetimeContext *ctx = static_cast<LifetimeContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    EXPECT_EQ(transferred, sizeof(uint32_t));
  }
  lifetimeAccount(ctx, status);
  deleteAioObject(socket);
}

TEST(lifetime, cancel_io_keeps_object_alive)
{
  LifetimeContext context(gBase);
  aioObject *server = startUDPServer(gBase, nullptr, nullptr, nullptr, 0, gPort);
  aioObject *client = initializeUDPClient(gBase);
  ASSERT_NE(server, nullptr);
  ASSERT_NE(client, nullptr);
  objectSetDestructorCb(aioObjectHandle(server), lifetimeDestructorCb, &context);

  constexpr unsigned opsNum = 8;
  static uint32_t buffers[opsNum];
  for (unsigned i = 0; i < opsNum; i++)
    aioReadMsg(server, &buffers[i], sizeof(uint32_t), afNone, 3000000, lifetimeReadMsgCb, &context);
  cancelIo(aioObjectHandle(server));

  // Submitted after cancelIo: must survive it and complete with the datagram
  static uint32_t probeBuffer;
  aioReadMsg(server, &probeBuffer, sizeof(probeBuffer), afNone, 3000000, lifetimeProbeReadCb, &context);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  uint32_t payload = 0xbaadf00d;
  aioWriteMsg(client, &address, &payload, sizeof(payload), afNone, 0, nullptr, nullptr);

  asyncLoop(gBase);
  EXPECT_EQ(context.canceledCallbacks, opsNum);
  EXPECT_EQ(context.successCallbacks, 1u);
  EXPECT_EQ(context.unexpectedCallbacks, 0u);
  EXPECT_EQ(context.destructorCalls, 1u);
  EXPECT_EQ(context.callbacksAfterDestructor, 0u);
  EXPECT_EQ(probeBuffer, 0xbaadf00d);
  deleteAioObject(client);
}

// Operations submitted behind a connect sit frozen in the read/write queues
// (the exclusive slot gates them); deleteAioObject must cancel the parked
// connect and both gated operations, then destruct - one aosCanceled per
// operation, nothing leaks, nothing outlives the destructor callback. The
// blackhole address keeps the connect in flight, but no loop iteration runs
// between the submissions and the delete, so the test does not depend on the
// network answering or staying silent
TEST(lifetime, delete_with_connect_in_flight)
{
  LifetimeContext context(gBase);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);
  aioObject *client = newSocketIo(gBase, clientSocket);
  objectSetDestructorCb(aioObjectHandle(client), lifetimeDestructorCb, &context);

  address.ipv4 = inet_addr("192.0.2.1");
  address.port = 9;
  aioConnect(client, &address, 3000000, lifetimeConnectCb, &context);

  static char readBuffer[16];
  static const char writeBuffer[16] = {};
  aioRead(client, readBuffer, sizeof(readBuffer), afNone, 3000000, lifetimeIoCb, &context);
  aioWrite(client, writeBuffer, sizeof(writeBuffer), afNone, 3000000, lifetimeIoCb, &context);
  deleteAioObject(client);

  asyncLoop(gBase);
  EXPECT_EQ(context.connectStatus, aosCanceled);
  EXPECT_EQ(context.canceledCallbacks, 2u);
  EXPECT_EQ(context.successCallbacks, 0u);
  EXPECT_EQ(context.unexpectedCallbacks, 0u);
  EXPECT_EQ(context.destructorCalls, 1u);
  EXPECT_EQ(context.callbacksAfterDestructor, 0u);
}

// Same contract for user events, which have their own reference mechanics:
// deleting the event from inside its own callback must still deliver the
// destructor callback exactly once and nothing may fire afterwards
static void lifetimeEventCb(aioUserEvent *event, void *arg)
{
  LifetimeContext *ctx = static_cast<LifetimeContext*>(arg);
  if (ctx->destructorCalls)
    ctx->callbacksAfterDestructor++;
  if (++ctx->eventFires == 3)
    deleteUserEvent(event);
}

static void lifetimeEventDestructorCb(aioUserEvent*, void *arg)
{
  LifetimeContext *ctx = static_cast<LifetimeContext*>(arg);
  ctx->destructorCalls++;
  postQuitOperation(ctx->base);
}

TEST(lifetime, user_event_destructor)
{
  LifetimeContext context(gBase);
  aioUserEvent *event = newUserEvent(gBase, 0, lifetimeEventCb, &context);
  ASSERT_NE(event, nullptr);
  eventSetDestructorCb(event, lifetimeEventDestructorCb, &context);
  userEventStartTimer(event, 1000, 3);

  asyncLoop(gBase);
  EXPECT_EQ(context.eventFires, 3u);
  EXPECT_EQ(context.destructorCalls, 1u);
  EXPECT_EQ(context.callbacksAfterDestructor, 0u);
}
void test_delete_object_eventcb(aioUserEvent *event, void *arg)
{
  __UNUSED(event);
  TestContext *ctx = static_cast<TestContext*>(arg);
  deleteAioObject(ctx->serverSocket);
}

void test_delete_object_readcb(AsyncOpStatus status, aioObject *socket, HostAddress address, size_t transferred, void *arg)
{
  __UNUSED(socket);
  __UNUSED(address);
  __UNUSED(transferred);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosCanceled);
  if (status == aosCanceled) {
    ctx->serverState++;
    if (ctx->serverState == 1000) {
      ctx->success = true;
      postQuitOperation(ctx->base);
    }
  } else {
    postQuitOperation(ctx->base);
  }
}

TEST(lifetime, test_delete_object)
{
  TestContext context(gBase);
  context.serverSocket = startUDPServer(gBase, nullptr, &context, context.serverBuffer, sizeof(context.serverBuffer), gPort);
  ASSERT_NE(context.serverSocket, nullptr);

  for (int i = 0; i < 1000; i++)
    aioReadMsg(context.serverSocket, context.serverBuffer, sizeof(context.serverBuffer), afNone, 3*1000000, test_delete_object_readcb, &context);

  aioUserEvent *event = newUserEvent(gBase, 0, test_delete_object_eventcb, &context);
  userEventStartTimer(event, 5000, 1);
  asyncLoop(gBase);
  deleteUserEvent(event);
  ASSERT_TRUE(context.success);
}

void test_userevent_cb(aioUserEvent *event, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  unsigned value = __uint_atomic_fetch_and_add(reinterpret_cast<unsigned*>(&ctx->serverState), 1);
  if (value == 256) {
    userEventActivate(event);
  } else if (value == 257) {
    ctx->success = true;
    postQuitOperation(ctx->base);
  }
}

TEST(user_event, test_userevent)
{
  TestContext context(gBase);
  aioUserEvent *event = newUserEvent(gBase, 1, test_userevent_cb, &context);
  userEventStartTimer(event, 400, 256);
  userEventActivate(event);
  std::thread threads[4];
  for (unsigned i = 0; i < 4; i++) {
    threads[i] = std::thread([](){
      asyncLoop(gBase);
    });
  }
  std::for_each(threads, threads+4, [](std::thread &thread) {
    thread.join();
  });
  deleteUserEvent(event);
  ASSERT_TRUE(context.success);
}
