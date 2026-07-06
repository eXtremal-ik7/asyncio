#include "unittest.h"
#include "asyncio/coroutine.h"
#include "asyncio/device.h"
#include "asyncio/socket.h"
#include "p2putils/HttpRequestParse.h"
#include "asyncioextras/rlpx.h"
#include "atomic.h"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <thread>
#ifdef OS_COMMONUNIX
#include <unistd.h>
#endif

asyncBase *gBase = nullptr;

aioObject *startTCPServer(asyncBase *base, aioAcceptCb callback, void *arg, uint16_t port)
{
  HostAddress address;

  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = port;
  socketTy acceptSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  socketReuseAddr(acceptSocket);
  if (socketBind(acceptSocket, &address) != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (socketBind(acceptSocket, &address) != 0)
      return nullptr;
  }

  if (socketListen(acceptSocket) != 0)
    return nullptr;

  aioObject *object = newSocketIo(base, acceptSocket);
  if (callback)
    aioAccept(object, 333000, callback, arg);
  return object;
}

aioObject *startUDPServer(asyncBase *base, aioReadMsgCb callback, void *arg, void *buffer, size_t size, uint16_t port)
{
  HostAddress address;

  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = port;
  socketTy acceptSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  if (socketBind(acceptSocket, &address) != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (socketBind(acceptSocket, &address) != 0)
      return nullptr;
  }

  aioObject *object = newSocketIo(base, acceptSocket);
  if (callback)
    aioReadMsg(object, buffer, size, afNone, 1000000, callback, arg);
  return object;
}

aioObject *initializeTCPClient(asyncBase *base, aioConnectCb callback, void *arg, uint16_t port)
{
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy connectSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  int bindResult = socketBind(connectSocket, &address);
  EXPECT_EQ(bindResult, 0);
  if (bindResult != 0)
    return nullptr;

  aioObject *object = newSocketIo(base, connectSocket);
  if (callback) {
    address.family = AF_INET;
    address.ipv4 = inet_addr("127.0.0.1");
    address.port = port;
    aioConnect(object, &address, 333000, callback, arg);
  }

  return object;
}

aioObject *initializeUDPClient(asyncBase *base)
{
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  if (socketBind(clientSocket, &address) != 0)
    return nullptr;

  aioObject *object = newSocketIo(base, clientSocket);
  return object;
}

void test_pipe_writecb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status != aosSuccess)
    postQuitOperation(ctx->base);
}

void test_pipe_readcb(AsyncOpStatus status, aioObject*, size_t transferred, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  reqStruct *req = reinterpret_cast<reqStruct*>(ctx->serverBuffer);
  EXPECT_EQ(status, aosSuccess);
  EXPECT_EQ(transferred, sizeof(reqStruct));
  if (status == aosSuccess && transferred == sizeof(reqStruct)) {
    EXPECT_EQ(req->a, 11);
    EXPECT_EQ(req->b, 77);
  }

  postQuitOperation(ctx->base);
}

TEST(basic, test_pipe)
{
  TestContext context(gBase);
  pipeTy unnamedPipe;
  int result = pipeCreate(&unnamedPipe, 1);
  EXPECT_EQ(result, 0);
  if (result == 0) {
    reqStruct req;
    context.pipeRead = newDeviceIo(gBase, unnamedPipe.read);
    context.pipeWrite = newDeviceIo(gBase, unnamedPipe.write);
    req.a = 11;
    req.b = 77;
    aioRead(context.pipeRead, context.serverBuffer, sizeof(req), afWaitAll, 1000000, test_pipe_readcb, &context);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    aioWrite(context.pipeWrite, &req, sizeof(req), afWaitAll, 0, test_pipe_writecb, &context);
    asyncLoop(gBase);
    deleteAioObject(context.pipeRead);
    deleteAioObject(context.pipeWrite);
  }
}

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

TEST(basic, test_connect_accept)
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

TEST(basic, test_tcp_rw)
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

TEST(basic, test_tcp_rw_ipv6)
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

  aioObject *object = newSocketIo(createAsyncBase(amOSDefault), clientSocket);
  const char payload[] = "ping";
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  exit(0);
}

TEST(SocketDeathTest, write_after_peer_reset)
{
  EXPECT_EXIT(writeTwiceAfterPeerReset(), ::testing::ExitedWithCode(0), "");
}

// Same contract for the device path: writing to a pipe whose reader is gone
// must not kill the process. write() has no MSG_NOSIGNAL equivalent, so the
// shield depends on the platform and init flags, and each variant below pins
// one of them:
// - aiNone on Darwin/NetBSD: F_SETNOSIGPIPE set by newDeviceIo (per-fd);
// - aiNone on Linux/FreeBSD: SIGPIPE masked around the write itself
//   (sigpipeGuard, libpq pattern) - there is no per-fd opt-out there;
// - aiIgnoreSigpipe anywhere: process-wide SIG_IGN, masking is skipped.
// Fully deterministic: the kernel tracks pipe readers synchronously, so the
// first write() after the last reader closed raises SIGPIPE, no timing
// involved.
static void writeTwiceAfterPipeReaderClosed(AsyncInitFlags initFlags)
{
  alarm(30);  // if anything blocks, fail the death test instead of hanging it
  initializeAsyncIo(initFlags);

  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);
  close(unnamedPipe.read);

  aioObject *object = newDeviceIo(createAsyncBase(amOSDefault), unnamedPipe.write);
  const char payload[] = "ping";
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  exit(0);
}

TEST(PipeDeathTest, write_after_reader_closed)
{
  EXPECT_EXIT(writeTwiceAfterPipeReaderClosed(aiNone), ::testing::ExitedWithCode(0), "");
}

TEST(PipeDeathTest, ignore_sigpipe_flag)
{
  EXPECT_EXIT(writeTwiceAfterPipeReaderClosed(aiIgnoreSigpipe), ::testing::ExitedWithCode(0), "");
}
#endif

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

TEST(basic, test_udp_rw)
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

TEST(basic, test_udp_sender_address)
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

TEST(basic, test_udp_sender_address_ipv6)
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

TEST(basic, test_udp_write_error)
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

TEST(basic, test_timeout)
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

TEST(basic, test_delete_object)
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

TEST(basic, test_userevent)
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

void coroutine_create_proc(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
}

TEST(coroutine, create)
{
  int x = 0;
  coroutineTy *coro = coroutineNew(coroutine_create_proc, &x, 0x10000);
  while (!coroutineCall(coro))
    continue;
  ASSERT_EQ(x, 1);
}

void coroutine_yield_proc(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
  coroutineYield();
  (*x)++;
}

TEST(coroutine, yield)
{
  int x = 0;
  coroutineTy *coro = coroutineNew(coroutine_yield_proc, &x, 0x10000);
  while (!coroutineCall(coro))
    continue;
  ASSERT_EQ(x, 2);
}

void coroutine_nested_proc2(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
}

void coroutine_nested_proc1(void *arg)
{
  int *x = static_cast<int*>(arg);
  (*x)++;
  coroutineYield();
  coroutineTy *coro = coroutineNew(coroutine_nested_proc2, x, 0x10000);
  coroutineCall(coro);
  coroutineYield();
  (*x)++;
}

TEST(coroutine, nested)
{
  int x = 0;
  coroutineTy *coro = coroutineNew(coroutine_nested_proc1, &x, 0x10000);
  while (!coroutineCall(coro))
    continue;
  ASSERT_EQ(x, 3);
}

static void httpRequestCb1Impl(HttpRequestComponent *component, void *arg)
{
  int *callNum = static_cast<int*>(arg);
  if (*callNum == 0) {
    ASSERT_EQ(component->type, httpRequestDtMethod);
    ASSERT_EQ(component->method, hmGet);
  } else if (*callNum == 1) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    ASSERT_EQ(component->data.size, strlen("path"));
    ASSERT_EQ(memcmp(component->data.data, "path", strlen("path")), 0);
  } else if (*callNum == 2) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    ASSERT_EQ(component->data.size, strlen("to"));
    ASSERT_EQ(memcmp(component->data.data, "to", strlen("to")), 0);
  } else if (*callNum == 3) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    ASSERT_EQ(component->data.size, strlen("page"));
    ASSERT_EQ(memcmp(component->data.data, "page", strlen("page")), 0);
  } else if (*callNum == 4) {
    ASSERT_EQ(component->type, httpRequestDtUriQueryElement);
    ASSERT_EQ(component->data.size, strlen("qname"));
    ASSERT_EQ(memcmp(component->data.data, "qname", strlen("qname")), 0);
    ASSERT_EQ(component->data2.size, strlen("value"));
    ASSERT_EQ(memcmp(component->data2.data, "value", strlen("value")), 0);
  } else if (*callNum == 5) {
    ASSERT_EQ(component->type, httpRequestDtUriFragment);
    ASSERT_EQ(component->data.size, strlen("fragment"));
    ASSERT_EQ(memcmp(component->data.data, "fragment", strlen("fragment")), 0);
  } else if (*callNum == 6) {
    ASSERT_EQ(component->type, httpRequestDtVersion);
    ASSERT_EQ(component->version.majorVersion, 1);
    ASSERT_EQ(component->version.minorVersion, 1);
  } else if (*callNum == 7) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhHost);
    ASSERT_EQ(component->header.stringValue.size, strlen("localhost:8080"));
    ASSERT_EQ(memcmp(component->header.stringValue.data, "localhost:8080", strlen("localhost:8080")), 0);
  } else if (*callNum == 8) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUserAgent);
    ASSERT_EQ(component->header.stringValue.size, strlen("curl/7.58.0"));
    ASSERT_EQ(memcmp(component->header.stringValue.data, "curl/7.58.0", strlen("curl/7.58.0")), 0);
  } else if (*callNum == 9) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhAccept);
    ASSERT_EQ(component->header.stringValue.size, strlen("*/*"));
    ASSERT_EQ(memcmp(component->header.stringValue.data, "*/*", strlen("*/*")), 0);
  } else if (*callNum == 10) {
    ASSERT_EQ(component->type, httpRequestDtDataLast);
    ASSERT_EQ(component->data.size, 0);
  }

  (*callNum)++;
}

static void httpRequestCb2Impl(HttpRequestComponent *component, void *arg)
{
  int *callNum = static_cast<int*>(arg);
  if (*callNum == 0) {
    ASSERT_EQ(component->type, httpRequestDtMethod);
    ASSERT_EQ(component->method, hmPost);
  } else if (*callNum == 1) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    ASSERT_EQ(component->data.size, strlen("api"));
    ASSERT_EQ(memcmp(component->data.data, "api", strlen("api")), 0);
  } else if (*callNum == 2) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    ASSERT_EQ(component->data.size, strlen("usercreate"));
    ASSERT_EQ(memcmp(component->data.data, "usercreate", strlen("usercreate")), 0);
  } else if (*callNum == 3) {
    ASSERT_EQ(component->type, httpRequestDtVersion);
    ASSERT_EQ(component->version.majorVersion, 1);
    ASSERT_EQ(component->version.minorVersion, 1);
  } else if (*callNum == 4) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhHost);
    ASSERT_EQ(component->header.stringValue.size, strlen("localhost:18880"));
    ASSERT_EQ(memcmp(component->header.stringValue.data, "localhost:18880", strlen("localhost:18880")), 0);
  } else if (*callNum == 5) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUserAgent);
    ASSERT_EQ(component->header.stringValue.size, strlen("curl/7.58.0"));
    ASSERT_EQ(memcmp(component->header.stringValue.data, "curl/7.58.0", strlen("curl/7.58.0")), 0);
  } else if (*callNum == 6) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhAccept);
    ASSERT_EQ(component->header.stringValue.size, strlen("*/*"));
    ASSERT_EQ(memcmp(component->header.stringValue.data, "*/*", strlen("*/*")), 0);
  } else if (*callNum == 7) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhContentLength);
    ASSERT_EQ(component->header.sizeValue, 2);
  } else if (*callNum == 8) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryName.size, strlen("Content-Type"));
    ASSERT_EQ(memcmp(component->header.entryName.data, "Content-Type", strlen("Content-Type")), 0);
    ASSERT_EQ(component->header.stringValue.size, strlen("application/x-www-form-urlencoded"));
    ASSERT_EQ(memcmp(component->header.stringValue.data, "application/x-www-form-urlencoded", strlen("application/x-www-form-urlencoded")), 0);
  } else if (*callNum == 9) {
    ASSERT_EQ(component->type, httpRequestDtDataLast);
    ASSERT_EQ(component->data.size, 2);
    ASSERT_EQ(memcmp(component->data.data, "{}", 2), 0);
  }

  (*callNum)++;
}

static int httpRequestCb1(HttpRequestComponent *component, void *arg)
{
  httpRequestCb1Impl(component, arg);
  return 1;
}

TEST(http, http_request_parser)
{
  {
    int callNum = 0;
    const char request1[] = "GET /path/to/page?qname=value#fragment HTTP/1.1\r\nHost: localhost:8080\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n\r\n\r\n";
    HttpRequestParserState state;
    httpRequestParserInit(&state);
    httpRequestSetBuffer(&state, request1, sizeof(request1)-1);
    ParserResultTy result = httpRequestParse(&state, httpRequestCb1, &callNum);
    ASSERT_EQ(result, ParserResultOk);
    ASSERT_EQ(callNum, 11);
  }

  {
    int callNum = 0;
    const char request[] = "POST /api/usercreate HTTP/1.1\r\nHost: localhost:18880\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\nContent-Length: 2\r\nContent-Type: application/x-www-form-urlencoded\r\n\r\n{}";
    HttpRequestParserState state;
    httpRequestParserInit(&state);
    httpRequestSetBuffer(&state, request, sizeof(request)-1);
    ParserResultTy result = httpRequestParse(&state, [](HttpRequestComponent *component, void *arg) -> int {
      httpRequestCb2Impl(component, arg);
      return 1;
    }, &callNum);
    ASSERT_EQ(result, ParserResultOk);
    ASSERT_EQ(callNum, 10);
  }
}

int main(int argc, char **argv)
{
  AsyncMethod method = amOSDefault;
  if (argc >= 2) {
    if (strcmp(argv[1], "default") == 0) {
      method = amOSDefault;
    } else if (strcmp(argv[1], "epoll") == 0) {
      method = amEPoll;
    } else if (strcmp(argv[1], "kqueue") == 0) {
      method = amKQueue;
    } else if (strcmp(argv[1], "iocp") == 0) {
      method = amIOCP;
    } else {
      fprintf(stderr, "ERROR: unknown method %s, default used\n", argv[1]);
    }
  }

  initializeAsyncIo(aiNone);

  gBase = createAsyncBase(method);

  ::testing::InitGoogleTest(&argc, argv);
//  rlpxSocketNew(nullptr, nullptr, nullptr, nullptr);
  return RUN_ALL_TESTS();
}
