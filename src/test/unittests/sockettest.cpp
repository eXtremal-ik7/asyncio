#include "unittest.h"

#include "asyncio/socket.h"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

#ifdef OS_COMMONUNIX
#include <unistd.h>
#endif

void test_connect_accept_readcb(AsyncOpStatus status, aioObject *socket, size_t transferred, void *arg)
{
  __UNUSED(transferred);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosDisconnected);
  if (status == aosDisconnected) {
    if (ctx->serverState == 1)
      ctx->success = true;
  }

  deleteAioObject(socket);
  postQuitOperation(ctx->base);
}

void test_connect_accept_acceptcb(AsyncOpStatus status, aioObject *listener, HostAddress client, socketTy acceptSocket, void *arg)
{
  __UNUSED(listener);
  __UNUSED(client);
  TestContext *ctx = static_cast<TestContext*>(arg);
  if (status == aosSuccess) {
    aioObject *newSocketOp = newSocketIo(ctx->base, acceptSocket);
    aioRead(newSocketOp, ctx->serverBuffer, sizeof(ctx->serverBuffer), afNone, 1000000, test_connect_accept_readcb, ctx);
  } else {
    postQuitOperation(ctx->base);
  }
}

void test_connect_accept_connectcb(AsyncOpStatus status, aioObject *object, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  TestContext *ctx = static_cast<TestContext*>(arg);
  if (status == aosSuccess)
    ctx->serverState = 1;
  deleteAioObject(object);
}

TEST(socket, test_connect_accept)
{
  TestContext context(gBase);
  context.serverSocket = startTCPServer(gBase, test_connect_accept_acceptcb, &context, gPort);
  context.clientSocket = initializeTCPClient(gBase, test_connect_accept_connectcb, &context, gPort);
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);

  asyncLoop(gBase);
  deleteAioObject(context.serverSocket);
  ASSERT_TRUE(context.success);
}

void test_tcp_rw_client_read(AsyncOpStatus status, aioObject *socket, size_t transferred, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  EXPECT_EQ(transferred, 7u);
  EXPECT_EQ(ctx->serverState, 1);
  if (status == aosSuccess && transferred == 7 && ctx->serverState == 1) {
    EXPECT_STREQ(reinterpret_cast<const char*>(ctx->clientBuffer), "234567");
    ctx->success = strcmp(reinterpret_cast<const char*>(ctx->clientBuffer), "234567") == 0;
  }

  deleteAioObject(socket);
}

void test_tcp_rw_connectcb(AsyncOpStatus status, aioObject *object, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->serverState = 1;
    aioWrite(object, "123456", 7, afWaitAll, 0, nullptr, nullptr);
    aioRead(object, ctx->clientBuffer, 7, afWaitAll, 333000, test_tcp_rw_client_read, ctx);
  } else {
    deleteAioObject(object);
    postQuitOperation(ctx->base);
  }
}

void test_tcp_rw_server_readcb(AsyncOpStatus status, aioObject *socket, size_t transferred, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  if (status == aosSuccess) {
    for (size_t i = 0; i < transferred-1; i++)
      ctx->serverBuffer[i]++;
    aioWrite(socket, ctx->serverBuffer, transferred, afNone, 0, nullptr, nullptr);
    aioRead(socket, ctx->serverBuffer, sizeof(ctx->serverBuffer), afNone, 0, test_tcp_rw_server_readcb, ctx);
  } else {
    deleteAioObject(socket);
    postQuitOperation(ctx->base);
  }
}

void test_tcp_rw_acceptcb(AsyncOpStatus status, aioObject *listener, HostAddress client, socketTy acceptSocket, void *arg)
{
  __UNUSED(listener);
  __UNUSED(client);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    aioObject *newSocketOp = newSocketIo(ctx->base, acceptSocket);
    aioRead(newSocketOp, ctx->serverBuffer, sizeof(ctx->serverBuffer), afNone, 0, test_tcp_rw_server_readcb, ctx);
  } else {
    postQuitOperation(ctx->base);
  }
}

TEST(socket, test_tcp_rw)
{
  TestContext context(gBase);
  context.serverSocket = startTCPServer(gBase, test_tcp_rw_acceptcb, &context, gPort);
  context.clientSocket = initializeTCPClient(gBase, test_tcp_rw_connectcb, &context, gPort);
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);

  asyncLoop(gBase);
  deleteAioObject(context.serverSocket);
  ASSERT_TRUE(context.success);
}

// IPv6 counterpart of test_tcp_rw. Both endpoints are set up through the
// public API only: socketBind() with an AF_INET6 HostAddress ([::1] listener,
// [::] client), then aioConnect/aioAccept over IPv6 loopback. The accept
// callback must see the peer as AF_INET6 loopback with a non-zero port.
void test_tcp_rw_ipv6_acceptcb(AsyncOpStatus status, aioObject *listener, HostAddress client, socketTy acceptSocket, void *arg)
{
  __UNUSED(listener);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    EXPECT_EQ(client.family, AF_INET6);
    EXPECT_EQ(memcmp(client.ipv6, &in6addr_loopback, sizeof(client.ipv6)), 0);
    EXPECT_NE(client.port, 0);
    aioObject *newSocketOp = newSocketIo(ctx->base, acceptSocket);
    aioRead(newSocketOp, ctx->serverBuffer, sizeof(ctx->serverBuffer), afNone, 0, test_tcp_rw_server_readcb, ctx);
  } else {
    postQuitOperation(ctx->base);
  }
}

TEST(socket, test_tcp_rw_ipv6)
{
  TestContext context(gBase);

  HostAddress serverAddress;
  ASSERT_EQ(hostAddressFromAscii("::1", &serverAddress), 1);
  serverAddress.port = gPort;
  socketTy serverSocket = socketCreate(AF_INET6, SOCK_STREAM, IPPROTO_TCP, 1);
  socketReuseAddr(serverSocket);
  ASSERT_EQ(socketBind(serverSocket, &serverAddress), 0);
  ASSERT_EQ(socketListen(serverSocket), 0);
  context.serverSocket = newSocketIo(gBase, serverSocket);
  aioAccept(context.serverSocket, 333000, test_tcp_rw_ipv6_acceptcb, &context);

  // the client socket must be bound before aioConnect: iocp ConnectEx
  // requires an already bound socket
  HostAddress clientAddress;
  ASSERT_EQ(hostAddressFromAscii("::", &clientAddress), 1);
  socketTy clientSocket = socketCreate(AF_INET6, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &clientAddress), 0);
  context.clientSocket = newSocketIo(gBase, clientSocket);
  aioConnect(context.clientSocket, &serverAddress, 333000, test_tcp_rw_connectcb, &context);

  asyncLoop(gBase);
  deleteAioObject(context.serverSocket);
  ASSERT_TRUE(context.success);
}

struct IoAcceptTimeoutContext {
  asyncBase *base;
  aioObject *listener;
  int status;
  socketTy acceptedSocket;
  HostAddress remoteAddress;

  explicit IoAcceptTimeoutContext(asyncBase *baseArg)
    : base(baseArg), listener(nullptr), status(1), acceptedSocket(0)
  {
    memset(&remoteAddress, 0xff, sizeof(remoteAddress));
  }
};

static void ioAcceptTimeoutProc(void *arg)
{
  IoAcceptTimeoutContext *ctx = static_cast<IoAcceptTimeoutContext*>(arg);
  ctx->status = ioAccept(ctx->listener,
                         &ctx->acceptedSocket,
                         &ctx->remoteAddress,
                         10000);
  postQuitOperation(ctx->base);
}

TEST(socket, io_accept_timeout_returns_status_separately)
{
  IoAcceptTimeoutContext ctx(gBase);
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy listenerSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_NE(listenerSocket, INVALID_SOCKET);
  ASSERT_EQ(socketBind(listenerSocket, &address), 0);
  ASSERT_EQ(socketListen(listenerSocket), 0);
  ctx.listener = newSocketIo(gBase, listenerSocket);
  ASSERT_NE(ctx.listener, nullptr);

  coroutineTy *coroutine = coroutineNew(ioAcceptTimeoutProc, &ctx, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  ASSERT_EQ(coroutineCall(coroutine), 0);
  asyncLoop(gBase);

  EXPECT_EQ(ctx.status, -aosTimeout);
  EXPECT_EQ(ctx.acceptedSocket, INVALID_SOCKET);
  EXPECT_EQ(ctx.remoteAddress.family, AF_UNSPEC);
  deleteAioObject(ctx.listener);
}

// Sleep-handshake regression: a grid timeout armed from an application
// thread while the only loop thread is deep in its long idle wait must be
// delivered within about a grid step - the producer observes the published
// sleep horizon and kicks the sleeper. Without the kick the delivery would
// wait out the rest of the idle sleep (~450 ms on epoll/iocp, ~950 ms on
// kqueue with the 50 ms head start below), which the duration bound catches.
struct SleepingArmContext {
  asyncBase *base;
  std::chrono::steady_clock::time_point armed;
  AsyncOpStatus status;
  long deliveryMs;
  SleepingArmContext(asyncBase *baseArg) : base(baseArg), status(aosUnknown), deliveryMs(-1) {}
};

static void sleepingArmReadCb(AsyncOpStatus status, aioObject*, HostAddress, size_t, void *arg)
{
  SleepingArmContext *ctx = static_cast<SleepingArmContext*>(arg);
  ctx->status = status;
  ctx->deliveryMs = static_cast<long>(std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - ctx->armed).count());
  postQuitOperation(ctx->base);
}

TEST(socket, timeout_armed_against_sleeping_loop_is_delivered_within_grid_step)
{
  SleepingArmContext ctx(gBase);
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy udpSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  ASSERT_NE(udpSocket, INVALID_SOCKET);
  ASSERT_EQ(socketBind(udpSocket, &address), 0);
  aioObject *object = newSocketIo(gBase, udpSocket);

  std::thread loop([] { asyncLoop(gBase); });
  // Let the loop thread settle into its idle wait with an empty wheel
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  uint8_t buffer[16];
  ctx.armed = std::chrono::steady_clock::now();
  aioReadMsg(object, buffer, sizeof(buffer), afNone, 125000, sleepingArmReadCb, &ctx);
  loop.join();

  EXPECT_EQ(ctx.status, aosTimeout);
  EXPECT_GE(ctx.deliveryMs, 0);
  EXPECT_LT(ctx.deliveryMs, 400)
    << "the sleeping loop was not kicked and the timeout waited out the idle sleep";
  deleteAioObject(object);
}

#ifdef OS_COMMONUNIX
// Regression test: aioWrite() to a connection dropped by the peer must
// report an error through the operation status, never kill the process with
// SIGPIPE. The protection is per-fd: MSG_NOSIGNAL on send paths where the
// flag exists (Linux/BSD), SO_NOSIGPIPE as a descriptor property on
// Darwin/BSD (set in socketCreate and in newSocketIo - the latter also
// covers accept()ed sockets, which never pass through socketCreate).
// The death-test child must reach exit(0).
//
// Determinism:
// - the peer closes with SO_LINGER{1,0}, so the client gets RST, not FIN;
// - the client socket is blocking: recv() returns ECONNRESET exactly when the
//   client TCP has processed that RST (the kernel marks the socket
//   send-closed before waking the reader), after which an unprotected send()
//   is guaranteed to raise SIGPIPE - no sleeps or timing assumptions;
// - two writes in a row: on stacks where the first send() after a reset
//   reports ECONNRESET without raising the signal, the second one hits
//   EPIPE + SIGPIPE;
// - the asyncBase is created inside the death-test child: kqueue descriptors
//   do not survive fork(), the shared gBase must not be touched here.
static void writeTwiceAfterPeerReset()
{
  alarm(30);  // if anything blocks, fail the death test instead of hanging it

  socketTy listenSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  sockaddr_in address;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;  // ephemeral port: immune to gPort clashes and TIME_WAIT
  ASSERT_EQ(bind(listenSocket, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(listen(listenSocket, 1), 0);
  socklen_t addressLength = sizeof(address);
  ASSERT_EQ(getsockname(listenSocket, reinterpret_cast<sockaddr*>(&address), &addressLength), 0);

  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  ASSERT_EQ(connect(clientSocket, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  socketTy peerSocket = accept(listenSocket, nullptr, nullptr);
  ASSERT_GE(peerSocket, 0);

  linger abortiveClose;
  abortiveClose.l_onoff = 1;
  abortiveClose.l_linger = 0;
  ASSERT_EQ(setsockopt(peerSocket, SOL_SOCKET, SO_LINGER, &abortiveClose, sizeof(abortiveClose)), 0);
  socketClose(peerSocket);

  char c;
  ASSERT_EQ(recv(clientSocket, &c, 1, 0), -1);
  ASSERT_EQ(errno, ECONNRESET);

  aioObject *object = newSocketIo(createAsyncBase(amOSDefault, 1), clientSocket);
  const char payload[] = "ping";
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  exit(0);
}

TEST(SocketDeathTest, write_after_peer_reset)
{
  EXPECT_EXIT(writeTwiceAfterPeerReset(), ::testing::ExitedWithCode(0), "");
}
#endif

// Pipelined "aioConnect; aioRead" on one TCP socket: an operation submitted
// after the connect must wait for the connect outcome instead of completing
// ahead of it. On epoll/kqueue the kernel provides this gate (read() during
// SYN_SENT returns EAGAIN, the op parks and later times out - after the
// connect did); on iocp WSARecv on a not-yet-connected socket fails
// synchronously with WSAENOTCONN, so the read completes with aosUnknownError
// while the connect is still in flight. The connect target is a TEST-NET-1
// blackhole (RFC 5737, never answers), which keeps the connect pending until
// its timeout; if the local network answers it with an ICMP error instead,
// the gating scenario cannot be exercised and the test skips.
struct TcpPipelineContext {
  asyncBase *base;
  AsyncOpStatus connectStatus;
  AsyncOpStatus readStatus;
  int events;
  int connectOrder;
  int readOrder;
  uint8_t buffer[16];
  TcpPipelineContext(asyncBase *baseArg) :
    base(baseArg), connectStatus(aosUnknown), readStatus(aosUnknown),
    events(0), connectOrder(-1), readOrder(-1) {}
};

static void tcpPipelineConnectCb(AsyncOpStatus status, aioObject*, void *arg)
{
  TcpPipelineContext *ctx = static_cast<TcpPipelineContext*>(arg);
  ctx->connectStatus = status;
  ctx->connectOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

static void tcpPipelineReadCb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  TcpPipelineContext *ctx = static_cast<TcpPipelineContext*>(arg);
  ctx->readStatus = status;
  ctx->readOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

TEST(socket, test_tcp_read_pipelined_with_connect)
{
  TcpPipelineContext ctx(gBase);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);
  aioObject *client = newSocketIo(gBase, clientSocket);

  address.ipv4 = inet_addr("192.0.2.1");
  address.port = 9;
  aioConnect(client, &address, 150000, tcpPipelineConnectCb, &ctx);
  aioRead(client, ctx.buffer, sizeof(ctx.buffer), afNone, 2000000, tcpPipelineReadCb, &ctx);

  asyncLoop(gBase);
  deleteAioObject(client);

  if (ctx.connectStatus != aosTimeout)
    GTEST_SKIP() << "blackhole answered (connect status " << ctx.connectStatus
                 << "), connect gating cannot be exercised on this network";
  EXPECT_LT(ctx.connectOrder, ctx.readOrder)
    << "read completed (status " << ctx.readStatus
    << ") while the connect was still in flight";
}

// Connect is the object's one-shot initialization: it occupies the single
// aioObjectRoot::initializationOp slot. A second connect submitted while the
// first is still in flight must be rejected immediately instead of queueing
// or corrupting the slot.
struct DoubleConnectContext {
  asyncBase *base;
  AsyncOpStatus firstStatus;
  AsyncOpStatus secondStatus;
  int events;
  int firstOrder;
  int secondOrder;
  DoubleConnectContext(asyncBase *baseArg) :
    base(baseArg), firstStatus(aosUnknown), secondStatus(aosUnknown),
    events(0), firstOrder(-1), secondOrder(-1) {}
};

static void doubleConnectFirstCb(AsyncOpStatus status, aioObject*, void *arg)
{
  DoubleConnectContext *ctx = static_cast<DoubleConnectContext*>(arg);
  ctx->firstStatus = status;
  ctx->firstOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

static void doubleConnectSecondCb(AsyncOpStatus status, aioObject*, void *arg)
{
  DoubleConnectContext *ctx = static_cast<DoubleConnectContext*>(arg);
  ctx->secondStatus = status;
  ctx->secondOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

TEST(socket, double_connect_rejected)
{
  DoubleConnectContext ctx(gBase);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);
  aioObject *client = newSocketIo(gBase, clientSocket);

  address.ipv4 = inet_addr("192.0.2.1");
  address.port = 9;
  aioConnect(client, &address, 150000, doubleConnectFirstCb, &ctx);
  aioConnect(client, &address, 150000, doubleConnectSecondCb, &ctx);

  asyncLoop(gBase);
  deleteAioObject(client);

  if (ctx.firstStatus != aosTimeout)
    GTEST_SKIP() << "blackhole answered (first connect status " << ctx.firstStatus
                 << "), initialization slot contention cannot be exercised on this network";
  EXPECT_EQ(ctx.secondStatus, aosUnknownError);
  EXPECT_LT(ctx.secondOrder, ctx.firstOrder)
    << "the second connect was not rejected while the first was in flight";
}

// deleteAioObject on an object with a connect in flight: the internal cancelIo
// sweep must find the operation in the initialization slot (it is not in the
// read/write queues anymore), cancel it and let the object die instead of
// leaking a parked connect and a permanently frozen object.
struct DeleteWhileConnectingContext {
  asyncBase *base;
  AsyncOpStatus status;
  DeleteWhileConnectingContext(asyncBase *baseArg) : base(baseArg), status(aosUnknown) {}
};

static void deleteWhileConnectingCb(AsyncOpStatus status, aioObject*, void *arg)
{
  DeleteWhileConnectingContext *ctx = static_cast<DeleteWhileConnectingContext*>(arg);
  ctx->status = status;
  postQuitOperation(ctx->base);
}

TEST(socket, delete_object_while_connecting)
{
  DeleteWhileConnectingContext ctx(gBase);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);
  aioObject *client = newSocketIo(gBase, clientSocket);

  address.ipv4 = inet_addr("192.0.2.1");
  address.port = 9;
  aioConnect(client, &address, 3000000, deleteWhileConnectingCb, &ctx);

  std::thread deleter([client]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    deleteAioObject(client);
  });

  auto started = std::chrono::steady_clock::now();
  asyncLoop(gBase);
  deleter.join();
  auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
    std::chrono::steady_clock::now() - started).count();

  if (ctx.status != aosCanceled && ctx.status != aosTimeout)
    GTEST_SKIP() << "blackhole answered (connect status " << ctx.status
                 << "), cancellation of an in-flight connect cannot be exercised on this network";
  EXPECT_EQ(ctx.status, aosCanceled);
  EXPECT_LT(elapsedMs, 2000)
    << "connect was not cancelled by deleteAioObject, it ran to its own timeout";
}

// A connect that resolves synchronously must report right away. The reactor
// backends do that: connect() failing with anything but EINPROGRESS becomes
// an immediate error status. The proactor path must match: ConnectEx
// returning FALSE with a real error (not WSA_IO_PENDING) means no completion
// packet will ever arrive, and treating it as pending - which is what the
// WSARecv-style check in iocpAsyncConnect does, reading BOOL FALSE as the
// WSA*-convention success (AcceptEx in iocpAsyncAccept has the same shape) -
// parks the operation beyond recovery: the timeout pushes a cancel, the
// cancel leans on CancelIoEx, but nothing was ever submitted, so no abort
// packet comes and the operation sits in the initialization slot forever, holding
// the object and freezing its queues. A hang, not a late error - hence a
// death-test child, where the watchdog turns the hang into a verdict.
// The trigger is a second connect on an already-connected socket (submitted
// from the first connect's callback: the initialization slot is released before
// the finish callback runs). What it reports is per-kernel semantics: macOS
// gives EISCONN (the TCP layer advances the socket state at handshake time),
// Linux returns 0 and reports success (the state advances lazily on the next
// connect() call - SO_ERROR confirmation does not move it, so the retry
// legitimately completes the first connect), Windows fails ConnectEx
// synchronously with WSAEISCONN - probed on a real system - with no packet
// following. So the asserted contract is promptness alone, not the status.
struct ConnectedReconnectContext {
  asyncBase *base;
  aioObject *client;
  AsyncOpStatus firstStatus;
  AsyncOpStatus secondStatus;
};

static void connectedReconnectSecondCb(AsyncOpStatus status, aioObject*, void *arg)
{
  ConnectedReconnectContext *ctx = static_cast<ConnectedReconnectContext*>(arg);
  ctx->secondStatus = status;
  postQuitOperation(ctx->base);
}

static void connectedReconnectFirstCb(AsyncOpStatus status, aioObject*, void *arg)
{
  ConnectedReconnectContext *ctx = static_cast<ConnectedReconnectContext*>(arg);
  ctx->firstStatus = status;
  if (status != aosSuccess)
    exit(2);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioConnect(ctx->client, &address, 1000000, connectedReconnectSecondCb, ctx);
}

static void connectedReconnectScenario()
{
  armDeathTestWatchdog(10);

  ConnectedReconnectContext ctx;
  ctx.base = createAsyncBase(amOSDefault, 1); // own base: gBase must not be touched after fork()
  ctx.client = nullptr;
  ctx.firstStatus = aosUnknown;
  ctx.secondStatus = aosUnknown;

  // A live listener; accepting is not needed, the loopback handshake
  // completes through the backlog
  if (!startTCPServer(ctx.base, nullptr, nullptr, gPort))
    exit(2);
  ctx.client = initializeTCPClient(ctx.base, connectedReconnectFirstCb, &ctx, gPort);
  if (!ctx.client)
    exit(2);

  asyncLoop(ctx.base);
  // aosTimeout would mean the synchronous resolution was parked until the
  // operation timeout; anything else that arrived at all arrived promptly
  exit(ctx.secondStatus == aosTimeout || ctx.secondStatus == aosUnknown ? 1 : 0);
}

TEST(ConnectDeathTest, connect_on_connected_socket_reports_promptly)
{
  EXPECT_EXIT(connectedReconnectScenario(), ::testing::ExitedWithCode(0), "");
}

// A client that completes the handshake and dies (abortive close, RST) while
// still parked in the listen backlog must not fail the accept operation: a
// flaky or hostile remote peer would otherwise kill the server's pending
// accept at will.
// Kernel semantics for this scenario, probed on real systems:
// - Linux and macOS hand the dead connection out of accept() successfully;
//   the first read on it reports the reset. Neither prunes the queue nor
//   returns ECONNABORTED (the BSD classic survives on FreeBSD at most), so
//   the reactor backends never even see accept() == -1 here and pass as is.
// - Windows depends on the ordering. An AcceptEx already in flight when the
//   RST lands grabs the connection and completes with success, like POSIX.
//   But an AcceptEx posted when the queued connection is already dead - this
//   test - completes with ERROR_NETNAME_DELETED, which the completion path
//   maps to aosDisconnected, and the operation dies instead of re-posting
//   AcceptEx and waiting on.
// The asserted contract is aosSuccess: either the dead socket itself (POSIX
// semantics - detecting the reset is the application's job on first I/O) or,
// once the backend retries, the live client queued behind it.
struct DeadBacklogAcceptContext {
  asyncBase *base;
  AsyncOpStatus status;
  socketTy acceptedSocket;
  DeadBacklogAcceptContext(asyncBase *baseArg) : base(baseArg), status(aosUnknown), acceptedSocket(0) {}
};

static void deadBacklogAcceptCb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  DeadBacklogAcceptContext *ctx = static_cast<DeadBacklogAcceptContext*>(arg);
  ctx->status = status;
  ctx->acceptedSocket = acceptSocket;
  postQuitOperation(ctx->base);
}

TEST(socket, client_reset_in_backlog_does_not_fail_accept)
{
  DeadBacklogAcceptContext ctx(gBase);
  aioObject *listener = startTCPServer(gBase, nullptr, nullptr, gPort);
  ASSERT_TRUE(listener);

  sockaddr_in address;
  memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = inet_addr("127.0.0.1");
  address.sin_port = htons(gPort);

  // blocking socket: connect() returning 0 means the handshake completed and
  // the connection is parked in the backlog
  socketTy victim = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  ASSERT_EQ(connect(victim, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  linger abortiveClose;
  abortiveClose.l_onoff = 1;
  abortiveClose.l_linger = 0;
  ASSERT_EQ(setsockopt(victim, SOL_SOCKET, SO_LINGER,
                       reinterpret_cast<const char*>(&abortiveClose), sizeof(abortiveClose)), 0);
  socketClose(victim);
  // the RST travels through loopback and is processed in microseconds; the
  // margin only has to cover scheduling noise
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  aioAccept(listener, 2000000, deadBacklogAcceptCb, &ctx);

  // the live client the accept should end up with once the dead one is dealt
  // with; queued behind the dead connection before the loop runs
  socketTy liveClient = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  ASSERT_EQ(connect(liveClient, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.status, aosSuccess)
    << "a client that died in the backlog must not fail the accept operation";
  if (ctx.status == aosSuccess)
    socketClose(ctx.acceptedSocket);
  socketClose(liveClient);
  deleteAioObject(listener);
}
// Operations pushed while an object's combiner is held by another thread are
// stacked LIFO (Treiber stack, api.h) and the combiner drains the captured
// chain from its head, newest-first, without reversing it (asyncioImpl.c):
// two writes submitted back-to-back by one thread land in the socket in
// swapped order - silent TCP stream corruption. The producer (test main
// thread) writes strictly increasing 4-byte counters while a server thread
// floods the client socket with garbage so the loop thread keeps entering
// the client's combiner for read-readiness; the server verifies that the
// counter stream it receives back is monotonic.
struct CombinerOrderContext {
  asyncBase *base;
  aioObject *client;
  std::atomic<int> connected;
  std::atomic<int> stopFlood;
  std::atomic<int> serverDone;
  std::atomic<long> inversions;
  std::atomic<long> valuesReceived;
  uint8_t sink[65536];
  CombinerOrderContext() :
    base(nullptr), client(nullptr), connected(0), stopFlood(0), serverDone(0),
    inversions(0), valuesReceived(0) {}
};

static void combinerOrderFloodReadCb(AsyncOpStatus status, aioObject *object, size_t, void *arg)
{
  CombinerOrderContext *ctx = static_cast<CombinerOrderContext*>(arg);
  if (status == aosSuccess && !ctx->stopFlood.load())
    aioRead(object, ctx->sink, sizeof(ctx->sink), afNone, 0, combinerOrderFloodReadCb, ctx);
}

static void combinerOrderConnectCb(AsyncOpStatus status, aioObject *object, void *arg)
{
  CombinerOrderContext *ctx = static_cast<CombinerOrderContext*>(arg);
  if (status == aosSuccess) {
    aioRead(object, ctx->sink, sizeof(ctx->sink), afNone, 0, combinerOrderFloodReadCb, ctx);
    ctx->connected.store(1);
  } else {
    ctx->connected.store(-1);
  }
}

static void combinerOrderServer(socketTy listenSocket, CombinerOrderContext *ctx, uint32_t total)
{
  socketTy fd = accept(listenSocket, nullptr, nullptr);
  if (fd == (socketTy)(-1)) {
    ctx->serverDone.store(1);
    return;
  }

  std::vector<uint8_t> garbage(16384, 0xAA);
  std::vector<uint8_t> rx(65536);
  uint8_t tail[4];
  size_t tailSize = 0;
  uint32_t expected = 0;
  long flooded = 0;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
  while (ctx->valuesReceived.load() < (long)total && std::chrono::steady_clock::now() < deadline) {
    // keep the client's combiner busy on the loop thread, but bounded
    if (!ctx->stopFlood.load() && flooded < 32*1024*1024) {
      send(fd, (const char*)garbage.data(), (int)garbage.size(), 0);
      flooded += (long)garbage.size();
    }

    int r = (int)recv(fd, (char*)rx.data(), (int)rx.size(), 0);
    if (r <= 0)
      break;

    size_t offset = 0;
    while (offset < (size_t)r) {
      size_t take = (size_t)4 - tailSize;  // windows.h defines a min() macro, avoid std::min
      if (take > (size_t)r - offset)
        take = (size_t)r - offset;
      memcpy(tail + tailSize, rx.data() + offset, take);
      tailSize += take;
      offset += take;
      if (tailSize == 4) {
        uint32_t value;
        memcpy(&value, tail, 4);
        if (value != expected)
          ctx->inversions++;
        expected = value + 1;
        tailSize = 0;
        ctx->valuesReceived++;
      }
    }
  }

  ctx->serverDone.store(1);
  socketClose(fd);
}

TEST(socket, write_submission_order)
{
  constexpr uint32_t total = 100000;

  CombinerOrderContext ctx;
  ctx.base = createAsyncBase(amOSDefault, 1);  // own base: MT hammering stays off gBase

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = gPort;
  socketTy listenSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  socketReuseAddr(listenSocket);
  ASSERT_EQ(socketBind(listenSocket, &address), 0);
  ASSERT_EQ(socketListen(listenSocket), 0);
  std::thread serverThread(combinerOrderServer, listenSocket, &ctx, total);

  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);
  ctx.client = newSocketIo(ctx.base, clientSocket);

  std::thread loopThread([&ctx]() { asyncLoop(ctx.base); });

  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioConnect(ctx.client, &address, 3000000, combinerOrderConnectCb, &ctx);

  auto connectDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (ctx.connected.load() == 0 && std::chrono::steady_clock::now() < connectDeadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));

  if (ctx.connected.load() == 1) {
    for (uint32_t i = 0; i < total; i++)
      aioWrite(ctx.client, &i, sizeof(i), afWaitAll, 0, nullptr, nullptr);

    auto verdictDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
    while (!ctx.serverDone.load() && std::chrono::steady_clock::now() < verdictDeadline)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  ctx.stopFlood.store(1);
  postQuitOperation(ctx.base);
  loopThread.join();
  serverThread.join();
  socketClose(listenSocket);
  deleteAioObject(ctx.client);

  ASSERT_EQ(ctx.connected.load(), 1);
  EXPECT_EQ(ctx.valuesReceived.load(), (long)total);
  EXPECT_EQ(ctx.inversions.load(), 0)
    << "single-thread submission order was not preserved on the wire";
}

void test_udp_rw_client_readcb(AsyncOpStatus status, aioObject *socket, HostAddress address, size_t transferred, void *arg)
{
  __UNUSED(address);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  EXPECT_EQ(transferred, 7u);
  EXPECT_EQ(ctx->serverState, 1);
  if (status == aosSuccess &&
      transferred == 7 &&
      ctx->serverState == 1) {
    EXPECT_STREQ(reinterpret_cast<const char*>(ctx->clientBuffer), "234567");
    if (strcmp(reinterpret_cast<const char*>(ctx->clientBuffer), "234567") == 0)
      ctx->serverState++;
  }

  deleteAioObject(socket);
  deleteAioObject(ctx->serverSocket);
}

void test_udp_rw_server_readcb(AsyncOpStatus status, aioObject *socket, HostAddress address, size_t transferred, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  if (status == aosSuccess) {
    ctx->serverState++;
    for (size_t i = 0; i < transferred-1; i++)
      ctx->serverBuffer[i]++;
    aioWriteMsg(socket, &address, ctx->serverBuffer, transferred, afNone, 0, nullptr, nullptr);
    aioReadMsg(socket, ctx->serverBuffer, sizeof(ctx->serverBuffer), afNone, 1000000, test_udp_rw_server_readcb, ctx);
  } else {
    if (status == aosCanceled)
      ctx->success = (ctx->serverState == 2);
    postQuitOperation(ctx->base);
  }
}

TEST(socket, test_udp_rw)
{
  TestContext context(gBase);
  context.serverSocket = startUDPServer(gBase, test_udp_rw_server_readcb, &context, context.serverBuffer, sizeof(context.serverBuffer), gPort);
  context.clientSocket = initializeUDPClient(gBase);
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioWriteMsg(context.clientSocket, &address, "123456", 7, afNone, 0, nullptr, nullptr);
  aioReadMsg(context.clientSocket, context.clientBuffer, sizeof(context.clientBuffer), afNone, 1000000, test_udp_rw_client_readcb, &context);
  asyncLoop(gBase);
  ASSERT_TRUE(context.success);
}

// aioReadMsg reports the sender address through two code paths that must
// agree. A read armed before the datagram arrives completes through the event
// loop, which fills HostAddress from sockaddr_storage; a datagram already
// buffered at aioReadMsg call time is consumed by the synchronous fast path.
// Both datagrams below are placed into the server socket buffer inside the
// loopback sendto calls, before asyncLoop() starts, so path selection is
// deterministic: the first one is picked up by the parked operation (event
// loop path), the second is found in the buffer by the re-armed read (fast
// path). Without a valid 'family' the HostAddress union cannot be interpreted
// and aioWriteMsg cannot reply to the sender.
void test_udp_sender_address_readcb(AsyncOpStatus status, aioObject *socket, HostAddress address, size_t transferred, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->serverState++;
    EXPECT_EQ(transferred, 5u);
    EXPECT_EQ(address.family, AF_INET);
    EXPECT_EQ(address.ipv4, inet_addr("127.0.0.1"));
    EXPECT_NE(address.port, 0);
    if (ctx->serverState < 2) {
      aioReadMsg(socket, ctx->serverBuffer, sizeof(ctx->serverBuffer), afNone, 1000000, test_udp_sender_address_readcb, ctx);
      return;
    }
    ctx->success = true;
  }

  deleteAioObject(ctx->clientSocket);
  deleteAioObject(ctx->serverSocket);
  postQuitOperation(ctx->base);
}

TEST(socket, test_udp_sender_address)
{
  TestContext context(gBase);
  context.serverSocket = startUDPServer(gBase, test_udp_sender_address_readcb, &context, context.serverBuffer, sizeof(context.serverBuffer), gPort);
  context.clientSocket = initializeUDPClient(gBase);
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioWriteMsg(context.clientSocket, &address, "ping", 5, afNone, 0, nullptr, nullptr);
  aioWriteMsg(context.clientSocket, &address, "ping", 5, afNone, 0, nullptr, nullptr);
  asyncLoop(gBase);
  ASSERT_TRUE(context.success);
}

// Same two-path check for an IPv6 socket: the sender address must survive
// both paths as AF_INET6 with intact address bytes. Both endpoints are set up
// through the public API only (socketCreate + socketBind with an AF_INET6
// HostAddress).
void test_udp_sender_address_ipv6_readcb(AsyncOpStatus status, aioObject *socket, HostAddress address, size_t transferred, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->serverState++;
    EXPECT_EQ(transferred, 5u);
    EXPECT_EQ(address.family, AF_INET6);
    EXPECT_EQ(memcmp(address.ipv6, &in6addr_loopback, sizeof(address.ipv6)), 0);
    EXPECT_NE(address.port, 0);
    if (ctx->serverState < 2) {
      aioReadMsg(socket, ctx->serverBuffer, sizeof(ctx->serverBuffer), afNone, 1000000, test_udp_sender_address_ipv6_readcb, ctx);
      return;
    }
    ctx->success = true;
  }

  deleteAioObject(ctx->clientSocket);
  deleteAioObject(ctx->serverSocket);
  postQuitOperation(ctx->base);
}

TEST(socket, test_udp_sender_address_ipv6)
{
  TestContext context(gBase);
  HostAddress address;
  ASSERT_EQ(hostAddressFromAscii("::1", &address), 1);
  address.port = gPort;
  socketTy serverSocket = socketCreate(AF_INET6, SOCK_DGRAM, IPPROTO_UDP, 1);
  ASSERT_EQ(socketBind(serverSocket, &address), 0);
  context.serverSocket = newSocketIo(gBase, serverSocket);
  context.clientSocket = newSocketIo(gBase, socketCreate(AF_INET6, SOCK_DGRAM, IPPROTO_UDP, 1));
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);
  aioReadMsg(context.serverSocket, context.serverBuffer, sizeof(context.serverBuffer), afNone, 1000000, test_udp_sender_address_ipv6_readcb, &context);

  aioWriteMsg(context.clientSocket, &address, "ping", 5, afNone, 0, nullptr, nullptr);
  aioWriteMsg(context.clientSocket, &address, "ping", 5, afNone, 0, nullptr, nullptr);
  asyncLoop(gBase);
  ASSERT_TRUE(context.success);
}

// A sendto() error must finish the operation with an error callback.
// A 70 KB datagram cannot fit into a UDP packet (65507-byte payload limit),
// so sendto() fails with EMSGSIZE deterministically - no peer and no network
// are involved. The broken path treats every sendto() error as aosPending and
// re-arms the always-writable UDP socket, so the callback never fires and the
// event loop spins on sendto() at 100% CPU: the test then hangs instead of
// failing.
void test_udp_write_error_writecb(AsyncOpStatus status, aioObject *socket, size_t transferred, void *arg)
{
  __UNUSED(socket);
  __UNUSED(transferred);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_NE(status, aosSuccess);
  ctx->success = (status != aosSuccess);
  postQuitOperation(ctx->base);
}

TEST(socket, test_udp_write_error)
{
  TestContext context(gBase);
  context.clientSocket = initializeUDPClient(gBase);
  ASSERT_NE(context.clientSocket, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  static uint8_t oversized[70 * 1024];
  aioWriteMsg(context.clientSocket, &address, oversized, sizeof(oversized), afNone, 0, test_udp_write_error_writecb, &context);
  asyncLoop(gBase);
  deleteAioObject(context.clientSocket);
  ASSERT_TRUE(context.success);
}

// A datagram that does not fit into the caller's buffer must complete with
// aosBufferTooSmall on every backend. Both delivery paths are exercised the
// same way as in test_udp_sender_address: the first 100-byte datagram
// completes a parked read through the event loop, the second is picked up by
// the re-armed read's synchronous fast path. Broken behavior differs by
// platform. The POSIX backends call recvfrom() without MSG_TRUNC, so the
// kernel silently drops the tail and both reads complete with aosSuccess and
// 10 transferred bytes. On Windows the parked overlapped read is completed
// correctly (WSAEMSGSIZE -> aosBufferTooSmall), but the fast-path recvfrom()
// in aioReadMsg fails with WSAEMSGSIZE - which on Windows consumes and drops
// the datagram - and the error code is never examined: the operation is
// parked as if no data had arrived, so the second read loses its datagram and
// ends in aosTimeout.
void test_udp_read_truncated_readcb(AsyncOpStatus status, aioObject *socket, HostAddress address, size_t transferred, void *arg)
{
  __UNUSED(address);
  __UNUSED(transferred);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosBufferTooSmall);
  if (status == aosBufferTooSmall) {
    ctx->serverState++;
    if (ctx->serverState < 2) {
      aioReadMsg(socket, ctx->serverBuffer, 10, afNone, 1000000, test_udp_read_truncated_readcb, ctx);
      return;
    }
    ctx->success = true;
  }

  deleteAioObject(ctx->clientSocket);
  deleteAioObject(ctx->serverSocket);
  postQuitOperation(ctx->base);
}

TEST(socket, test_udp_read_truncated)
{
  TestContext context(gBase);
  context.serverSocket = startUDPServer(gBase, test_udp_read_truncated_readcb, &context, context.serverBuffer, 10, gPort);
  context.clientSocket = initializeUDPClient(gBase);
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  uint8_t payload[100] = {};
  aioWriteMsg(context.clientSocket, &address, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  aioWriteMsg(context.clientSocket, &address, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  asyncLoop(gBase);
  ASSERT_TRUE(context.success);
}

// afActiveOnce makes the callback a fallback rather than a guarantee: an
// operation that completed inline reports through the return value and its
// callback never fires, -aosPending means the callback will. Which way a
// completed operation is reported is decided by the per-thread budget: at
// most MAX_SYNCHRONOUS_FINISHED_OPERATION consecutive inline reports, then
// one completion goes through the event loop and the window restarts. Only
// afActiveOnce makes both outcomes countable - the coroutine variants hide
// which path delivered the result. Submissions run on freshly created
// threads so the thread-local budget deterministically starts at zero (the
// main thread's counter state depends on previous tests).
constexpr unsigned udpBudgetPeriod = MAX_SYNCHRONOUS_FINISHED_OPERATION + 1;
constexpr unsigned udpBudgetTotal = 3*udpBudgetPeriod;

struct UdpBudgetContext;

struct UdpBudgetSlot {
  UdpBudgetContext *ctx;
  unsigned index;
};

struct UdpBudgetContext {
  asyncBase *base;
  aioObject *serverSocket = nullptr;
  aioObject *clientSocket = nullptr;
  uint32_t writePayload[udpBudgetTotal];
  uint32_t readPayload[udpBudgetTotal];
  ssize_t writeResult[udpBudgetTotal];
  ssize_t readResult[udpBudgetTotal];
  bool writeCallbackFired[udpBudgetTotal] = {};
  bool readCallbackFired[udpBudgetTotal] = {};
  UdpBudgetSlot writeSlot[udpBudgetTotal];
  UdpBudgetSlot readSlot[udpBudgetTotal];
  unsigned expectedCallbacks = 0;
  unsigned deliveredCallbacks = 0;
  UdpBudgetContext(asyncBase *baseArg) : base(baseArg) {}
};

static void udpBudgetCallbackDelivered(UdpBudgetContext *ctx)
{
  if (++ctx->deliveredCallbacks == ctx->expectedCallbacks)
    postQuitOperation(ctx->base);
}

void test_udp_active_once_writecb(AsyncOpStatus status, aioObject*, size_t transferred, void *arg)
{
  UdpBudgetSlot *slot = static_cast<UdpBudgetSlot*>(arg);
  EXPECT_EQ(status, aosSuccess);
  EXPECT_EQ(transferred, sizeof(uint32_t));
  slot->ctx->writeCallbackFired[slot->index] = true;
  udpBudgetCallbackDelivered(slot->ctx);
}

void test_udp_active_once_readcb(AsyncOpStatus status, aioObject*, HostAddress, size_t transferred, void *arg)
{
  UdpBudgetSlot *slot = static_cast<UdpBudgetSlot*>(arg);
  EXPECT_EQ(status, aosSuccess);
  EXPECT_EQ(transferred, sizeof(uint32_t));
  slot->ctx->readCallbackFired[slot->index] = true;
  udpBudgetCallbackDelivered(slot->ctx);
}

TEST(socket, test_udp_active_once_sync_budget)
{
  UdpBudgetContext context(gBase);
  context.serverSocket = startUDPServer(gBase, nullptr, nullptr, nullptr, 0, gPort);
  context.clientSocket = initializeUDPClient(gBase);
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;

  std::thread writer([&context, &address]() {
    for (unsigned i = 0; i < udpBudgetTotal; i++) {
      context.writePayload[i] = i;
      context.writeSlot[i].ctx = &context;
      context.writeSlot[i].index = i;
      context.writeResult[i] = aioWriteMsg(context.clientSocket, &address, &context.writePayload[i], sizeof(uint32_t), afActiveOnce, 1000000, test_udp_active_once_writecb, &context.writeSlot[i]);
    }
  });
  writer.join();

  // sendto always runs inside aioWriteMsg no matter how the result is
  // reported: by now every datagram sits in the server socket buffer and
  // each aioReadMsg below consumes one inline, so only the budget decides
  // between the return value and the callback
  std::thread reader([&context]() {
    for (unsigned i = 0; i < udpBudgetTotal; i++) {
      context.readSlot[i].ctx = &context;
      context.readSlot[i].index = i;
      context.readResult[i] = aioReadMsg(context.serverSocket, &context.readPayload[i], sizeof(uint32_t), afActiveOnce, 1000000, test_udp_active_once_readcb, &context.readSlot[i]);
    }
  });
  reader.join();

  unsigned syncWrites = 0, syncReads = 0;
  for (unsigned i = 0; i < udpBudgetTotal; i++) {
    bool expectInline = ((i + 1) % udpBudgetPeriod) != 0;
    EXPECT_EQ(context.writeResult[i], expectInline ? (ssize_t)sizeof(uint32_t) : -(ssize_t)aosPending) << "write " << i;
    EXPECT_EQ(context.readResult[i], expectInline ? (ssize_t)sizeof(uint32_t) : -(ssize_t)aosPending) << "read " << i;
    syncWrites += context.writeResult[i] >= 0;
    syncReads += context.readResult[i] >= 0;
  }
  EXPECT_EQ(syncWrites, udpBudgetTotal - 3);
  EXPECT_EQ(syncReads, udpBudgetTotal - 3);

  context.expectedCallbacks = 2*udpBudgetTotal - syncWrites - syncReads;
  if (context.expectedCallbacks)
    asyncLoop(gBase);
  deleteAioObject(context.clientSocket);
  deleteAioObject(context.serverSocket);

  for (unsigned i = 0; i < udpBudgetTotal; i++) {
    // exactly one report per operation: inline return or callback, never both
    EXPECT_NE(context.writeResult[i] >= 0, context.writeCallbackFired[i]) << "write " << i;
    EXPECT_NE(context.readResult[i] >= 0, context.readCallbackFired[i]) << "read " << i;
    // loopback keeps datagrams ordered: every payload landed in the buffer
    // of its own read no matter which path reported the completion
    EXPECT_EQ(context.readPayload[i], i) << "read " << i;
  }
}

// With no callback at all the return value is the only channel that exists,
// so an inline completion must be reported inline no matter how many came
// before it: a -aosPending here would throw the result away (the operation
// finishes through the loop, where a null callback is silently dropped) -
// for reads that loses an already consumed datagram
TEST(socket, test_udp_fire_and_forget_sync_result)
{
  TestContext context(gBase);
  context.serverSocket = startUDPServer(gBase, nullptr, nullptr, nullptr, 0, gPort);
  context.clientSocket = initializeUDPClient(gBase);
  ASSERT_NE(context.serverSocket, nullptr);
  ASSERT_NE(context.clientSocket, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;

  std::vector<uint32_t> writePayload(udpBudgetTotal), readPayload(udpBudgetTotal);
  std::vector<ssize_t> writeResult(udpBudgetTotal), readResult(udpBudgetTotal);

  std::thread writer([&]() {
    for (unsigned i = 0; i < udpBudgetTotal; i++) {
      writePayload[i] = i;
      writeResult[i] = aioWriteMsg(context.clientSocket, &address, &writePayload[i], sizeof(uint32_t), afNone, 0, nullptr, nullptr);
    }
  });
  writer.join();

  std::thread reader([&]() {
    for (unsigned i = 0; i < udpBudgetTotal; i++)
      readResult[i] = aioReadMsg(context.serverSocket, &readPayload[i], sizeof(uint32_t), afNone, 1000000, nullptr, nullptr);
  });
  reader.join();

  for (unsigned i = 0; i < udpBudgetTotal; i++) {
    EXPECT_EQ(writeResult[i], (ssize_t)sizeof(uint32_t)) << "write " << i;
    EXPECT_EQ(readResult[i], (ssize_t)sizeof(uint32_t)) << "read " << i;
    EXPECT_EQ(readPayload[i], i) << "read " << i;
  }

  postQuitOperation(gBase);
  asyncLoop(gBase);
  deleteAioObject(context.clientSocket);
  deleteAioObject(context.serverSocket);
}

// The kernel reports some socket failures only through the "exceptional"
// poll state: on Linux EPOLLERR/EPOLLHUP are delivered regardless of the
// interest mask and can arrive with no readable/writable bit at all. Two
// natural sources, one per direction:
// - a connected UDP socket after an ICMP port unreachable for a datagram it
//   sent: the receive queue is empty, so the wakeup carries only the error
//   bit, and the parked read must be driven to pick up ECONNREFUSED;
// - a pipe whose reader closed while the pipe is full: no room to write and
//   no readers left, so again only the error bit.
// kqueue delivers both through the armed filter itself (EVFILT_READ fires
// on so_error, EVFILT_WRITE comes with EV_EOF), IOCP completes the parked
// overlapped operation with WSAECONNRESET. A backend that drops an
// error-only event kills the descriptor: the oneshot registration is
// already consumed, nothing ever rearms it, and the operation dies by its
// own timeout instead of reporting the error - or hangs forever when it has
// none. The exact status differs by backend (aosUnknownError from errno on
// the POSIX paths, aosDisconnected from the EV_EOF/WSAECONNRESET sweeps);
// the invariant is a prompt completion that is neither success nor timeout.

void test_udp_icmp_error_readcb(AsyncOpStatus status, aioObject*, HostAddress, size_t, void *arg)
{
  ErrorWakeupContext *ctx = static_cast<ErrorWakeupContext*>(arg);
  ctx->status = status;
  ctx->callbackFired = true;
  postQuitOperation(ctx->base);
}

TEST(socket, test_udp_icmp_error_wakes_parked_read)
{
  ErrorWakeupContext context(gBase);

  // A guaranteed-closed port: let the kernel assign an ephemeral one, then
  // release it
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = 0;
  socketTy probeSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  ASSERT_EQ(socketBind(probeSocket, &address), 0);
  sockaddr_in closedAddress;
  memset(&closedAddress, 0, sizeof(closedAddress));
#ifdef OS_WINDOWS
  int closedAddressLength = static_cast<int>(sizeof(closedAddress));
#else
  socklen_t closedAddressLength = sizeof(closedAddress);
#endif
  ASSERT_EQ(getsockname(probeSocket, reinterpret_cast<sockaddr*>(&closedAddress), &closedAddressLength), 0);
  socketClose(probeSocket);

  // connect() makes the kernel deliver the ICMP error to this socket
  // (an unconnected one silently drops it)
  socketTy clientSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  ASSERT_EQ(connect(clientSocket, reinterpret_cast<sockaddr*>(&closedAddress), sizeof(closedAddress)), 0);
  aioObject *object = newSocketIo(gBase, clientSocket);

  // Park the read before anything is sent: a datagram sent first would
  // bounce before the submission-time synchronous attempt, which would
  // consume the error inline and never exercise the event-loop wakeup
  uint32_t buffer;
  aioReadMsg(object, &buffer, sizeof(buffer), afNone, 1000000, test_udp_icmp_error_readcb, &context);

  char probe = 'x';
  EXPECT_EQ(send(clientSocket, &probe, 1, 0), 1);

  asyncLoop(gBase);
  EXPECT_TRUE(context.callbackFired);
  EXPECT_NE(context.status, aosSuccess);
  EXPECT_NE(context.status, aosTimeout) << "the ICMP error did not wake the parked read";
  deleteAioObject(object);
}

void test_timeout_readcb(AsyncOpStatus status, aioObject *socket, HostAddress address, size_t transferred, void *arg)
{
  __UNUSED(socket);
  __UNUSED(address);
  __UNUSED(transferred);
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosTimeout);
  if (status == aosTimeout) {
    ctx->serverState++;
    if (ctx->serverState == 1000+1+1+1+1000) {
      ctx->success = true;
      postQuitOperation(ctx->base);
    }
  }
}

TEST(socket, test_timeout)
{
  TestContext context(gBase);
  context.serverSocket = startUDPServer(gBase, nullptr, &context, context.serverBuffer, sizeof(context.serverBuffer), gPort);
  ASSERT_NE(context.serverSocket, nullptr);
  for (unsigned i = 0; i < 1000; i++)
    aioReadMsg(context.serverSocket, context.serverBuffer, sizeof(context.serverBuffer), afRealtime, 77000, test_timeout_readcb, &context);
  aioReadMsg(context.serverSocket, context.serverBuffer, sizeof(context.serverBuffer), afRealtime, 1000, test_timeout_readcb, &context);
  aioReadMsg(context.serverSocket, context.serverBuffer, sizeof(context.serverBuffer), afRealtime, 5000, test_timeout_readcb, &context);
  aioReadMsg(context.serverSocket, context.serverBuffer, sizeof(context.serverBuffer), afRealtime, 10000, test_timeout_readcb, &context);
  for (unsigned i = 0; i < 1000; i++)
    aioReadMsg(context.serverSocket, context.serverBuffer, sizeof(context.serverBuffer), afNone, 100000, test_timeout_readcb, &context);
  asyncLoop(gBase);
  deleteAioObject(context.serverSocket);
  ASSERT_TRUE(context.success);
}
