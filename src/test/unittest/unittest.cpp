#include "unittest.h"
#include "asyncio/coroutine.h"
#include "asyncio/device.h"
#include "asyncio/http.h"
#include "asyncio/socket.h"
#include "asyncioextras/rlpx.h"
#include "atomic.h"
#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>
#ifdef OS_COMMONUNIX
#include <sys/resource.h>
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

// The server streams a header block larger than the client's fixed 64KB
// input buffer. The request must fail with aosBufferTooSmall instead of
// re-parsing the full buffer in a hot loop that pins the event loop thread
// forever (implRead of the 0 remaining bytes completes synchronously with 0,
// the parser reports NeedMoreData again, repeat). The scenario runs as a
// death test under a watchdog, so the hang turns into a killed child process
// and a test failure instead of a hung test suite.
#if GTEST_HAS_DEATH_TEST
static void armDeathTestWatchdog(unsigned seconds)
{
#ifdef OS_WINDOWS
  // no alarm() on Windows; the death-test child is a fresh process (no fork),
  // so a plain watchdog thread is safe there
  std::thread([seconds]() {
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    TerminateProcess(GetCurrentProcess(), 3);
  }).detach();
#else
  alarm(seconds);
#endif
}

struct HttpOversizedHeaderContext {
  asyncBase *base;
  HTTPClient *client;
  std::string response;
  HTTPParseDefaultContext parseContext;
  AsyncOpStatus requestStatus;
};

static void httpOversizedHeaderRequestCb(AsyncOpStatus status, HTTPClient*, void *arg)
{
  HttpOversizedHeaderContext *ctx = static_cast<HttpOversizedHeaderContext*>(arg);
  ctx->requestStatus = status;
  postQuitOperation(ctx->base);
}

static void httpOversizedHeaderConnectCb(AsyncOpStatus status, HTTPClient *client, void *arg)
{
  HttpOversizedHeaderContext *ctx = static_cast<HttpOversizedHeaderContext*>(arg);
  if (status != aosSuccess)
    exit(2);

  static const char request[] = "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n";
  aioHttpRequest(client, request, sizeof(request)-1, 10000000, httpParseDefault, &ctx->parseContext, httpOversizedHeaderRequestCb, ctx);
}

static void httpOversizedHeaderAcceptCb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  HttpOversizedHeaderContext *ctx = static_cast<HttpOversizedHeaderContext*>(arg);
  if (status != aosSuccess)
    exit(2);

  aioWrite(newSocketIo(ctx->base, acceptSocket), ctx->response.data(), ctx->response.size(), afWaitAll, 0, nullptr, nullptr);
}

static void httpOversizedHeaderScenario()
{
  armDeathTestWatchdog(30);

  HttpOversizedHeaderContext ctx;
  ctx.base = createAsyncBase(amOSDefault);  // own base: gBase must not be touched after fork()
  ctx.client = nullptr;
  ctx.requestStatus = aosUnknown;
  // status line, then one header the client can never buffer entirely
  ctx.response = "HTTP/1.1 200 OK\r\nX-Huge-Header: ";
  ctx.response.append(2*65536, 'a');
  httpParseDefaultInit(&ctx.parseContext);

  if (!startTCPServer(ctx.base, httpOversizedHeaderAcceptCb, &ctx, gPort))
    exit(2);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  if (socketBind(clientSocket, &address) != 0)
    exit(2);

  ctx.client = httpClientNew(ctx.base, newSocketIo(ctx.base, clientSocket));
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioHttpConnect(ctx.client, &address, nullptr, 3000000, httpOversizedHeaderConnectCb, &ctx);

  asyncLoop(ctx.base);
  exit(ctx.requestStatus == aosBufferTooSmall ? 0 : 1);
}

TEST(HttpDeathTest, oversized_header)
{
  EXPECT_EXIT(httpOversizedHeaderScenario(), ::testing::ExitedWithCode(0), "");
}

// A plain-text server answers the ClientHello with garbage. The connect
// must fail with a protocol error: OpenSSL knows the stream is dead (fatal
// SSL error), and treating that as "want more data" hangs the operation (or
// spins when the peer keeps streaming). Same watchdog scheme as above.
static const char gSslGarbage[] = "this is a plain text server, definitely not TLS\r\n";

struct SslGarbageContext {
  asyncBase *base;
  SSLSocket *client;
  aioObject *serverConn;
  uint8_t readBuffer[1024];
  int flood;
};

static void sslGarbageWriteCb(AsyncOpStatus status, aioObject *object, size_t, void *arg)
{
  SslGarbageContext *ctx = static_cast<SslGarbageContext*>(arg);
  if (ctx->flood && status == aosSuccess)
    aioWrite(object, gSslGarbage, sizeof(gSslGarbage)-1, afWaitAll, 0, sslGarbageWriteCb, ctx);
}

static void sslGarbageAcceptCb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  SslGarbageContext *ctx = static_cast<SslGarbageContext*>(arg);
  if (status != aosSuccess)
    exit(2);

  ctx->serverConn = newSocketIo(ctx->base, acceptSocket);
  aioWrite(ctx->serverConn, gSslGarbage, sizeof(gSslGarbage)-1, afWaitAll, 0, sslGarbageWriteCb, ctx);
}

static void sslGarbageConnectCb(AsyncOpStatus status, SSLSocket*, void *arg)
{
  __UNUSED(arg);
  // any error is the expected outcome; success would mean garbage passed as TLS
  exit(status == aosSuccess ? 1 : 0);
}

static void sslConnectGarbageScenario(int flood)
{
  armDeathTestWatchdog(30);

  SslGarbageContext ctx;
  ctx.base = createAsyncBase(amOSDefault);  // own base: gBase must not be touched after fork()
  ctx.client = nullptr;
  ctx.serverConn = nullptr;
  ctx.flood = flood;

  if (!startTCPServer(ctx.base, sslGarbageAcceptCb, &ctx, gPort))
    exit(2);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  if (socketBind(clientSocket, &address) != 0)
    exit(2);

  ctx.client = sslSocketNew(ctx.base, newSocketIo(ctx.base, clientSocket));
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  // no timeout: the garbage must produce an error, not wait for a rescue timer
  aioSslConnect(ctx.client, &address, nullptr, 0, sslGarbageConnectCb, &ctx);

  asyncLoop(ctx.base);
  exit(3);  // event loop ended without the connect callback
}

TEST(SslDeathTest, connect_plain_garbage)
{
  EXPECT_EXIT(sslConnectGarbageScenario(0), ::testing::ExitedWithCode(0), "");
}

TEST(SslDeathTest, connect_plain_garbage_flood)
{
  EXPECT_EXIT(sslConnectGarbageScenario(1), ::testing::ExitedWithCode(0), "");
}

// A peer that accepts the TCP connection and drops it without ever speaking
// TLS. This is NOT connect_plain_garbage: garbage makes SSL_connect() fail
// fatally (a terminal SSL error -> the operation is released), but a bare
// disconnect keeps SSL_connect() in SSL_ERROR_WANT_READ - it never saw a
// ServerHello. The handshake read completes with aosDisconnected and
// resumeParent() sets that terminal status on the parent connect op, so the
// exclusive-slot handler must RELEASE on the terminal status instead of
// re-running the connect state machine. Re-executing connectProc() calls
// SSL_connect() again, gets WANT_READ again, and re-posts the handshake read on
// the dead socket - forever: the connect never completes, its exclusive slot
// stays pinned, the object leaks. There is no rescue timeout by design -
// resumeParent() already set a terminal status, so a timer's opSetStatus()
// loses the CAS and the timer is a no-op; the live-lock cannot be broken from
// outside. The watchdog turns the hang into a verdict.
static void sslConnectPeerDropAcceptCb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  __UNUSED(arg);
  if (status != aosSuccess)
    exit(2);
  socketClose(acceptSocket);  // FIN before a single TLS byte: the client's handshake read fails
}

static void sslConnectPeerDropConnectCb(AsyncOpStatus status, SSLSocket*, void *arg)
{
  __UNUSED(arg);
  // a peer that never completes the handshake must surface as an error, not hang
  exit(status == aosSuccess ? 1 : 0);
}

static void sslConnectPeerDropScenario()
{
  armDeathTestWatchdog(15);

  SslGarbageContext ctx;
  ctx.base = createAsyncBase(amOSDefault);  // own base: gBase must not be touched after fork()
  ctx.client = nullptr;
  ctx.serverConn = nullptr;
  ctx.flood = 0;

  if (!startTCPServer(ctx.base, sslConnectPeerDropAcceptCb, &ctx, gPort))
    exit(2);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  if (socketBind(clientSocket, &address) != 0)
    exit(2);

  ctx.client = sslSocketNew(ctx.base, newSocketIo(ctx.base, clientSocket));
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  // no timeout: releasing on the child's terminal status is the contract (and a
  // rescue timer could not break the live-lock anyway, see above)
  aioSslConnect(ctx.client, &address, nullptr, 0, sslConnectPeerDropConnectCb, &ctx);

  asyncLoop(ctx.base);
  exit(3);  // event loop drained without the connect callback
}

TEST(SslDeathTest, connect_peer_disconnect_during_handshake)
{
  EXPECT_EXIT(sslConnectPeerDropScenario(), ::testing::ExitedWithCode(0), "");
}

// The same garbage after a completed handshake exercises the read path,
// which never calls SSL_get_error. The server does a genuine TLS handshake
// (self-signed cert, blocking OpenSSL in a helper thread), then the wire
// turns into a plain-text stream. SSL_read fails fatally and can never
// recover, yet the code treats any non-positive result as "want more data":
// with a silent peer the operation waits forever, with a streaming peer it
// cycles forever.
static void sslTlsThenGarbageServer(socketTy listenSocket, int flood)
{
  socketTy fd = accept(listenSocket, nullptr, nullptr);
  if (fd == (socketTy)(-1))
    _Exit(4);

  EVP_PKEY *key = EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "P-256");
  if (!key)
    _Exit(4);
  X509 *cert = X509_new();
  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
  X509_gmtime_adj(X509_getm_notBefore(cert), -60);
  X509_gmtime_adj(X509_getm_notAfter(cert), 3600);
  X509_set_pubkey(cert, key);
  X509_NAME *subject = X509_get_subject_name(cert);
  X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC, (const unsigned char*)"localhost", -1, -1, 0);
  X509_set_issuer_name(cert, subject);
  if (X509_sign(cert, key, EVP_sha256()) == 0)
    _Exit(4);

  SSL_CTX *serverContext = SSL_CTX_new(TLS_server_method());
  SSL_CTX_use_certificate(serverContext, cert);
  SSL_CTX_use_PrivateKey(serverContext, key);
  SSL *ssl = SSL_new(serverContext);
  SSL_set_fd(ssl, (int)fd);
  if (SSL_accept(ssl) != 1)
    _Exit(4);

  do {
    if (send(fd, (const char*)gSslGarbage, (int)(sizeof(gSslGarbage)-1), 0) <= 0)
      break;
  } while (flood);
}

static void sslGarbageReadCb(AsyncOpStatus status, SSLSocket*, size_t, void *arg)
{
  __UNUSED(arg);
  // fatal TLS garbage must surface as an error; success would mean it decrypted
  exit(status == aosSuccess ? 1 : 0);
}

static void sslGarbageHandshakeConnectCb(AsyncOpStatus status, SSLSocket *socket, void *arg)
{
  SslGarbageContext *ctx = static_cast<SslGarbageContext*>(arg);
  if (status != aosSuccess)
    exit(4);  // the handshake against a real TLS server must succeed

  aioSslRead(socket, ctx->readBuffer, sizeof(ctx->readBuffer), afNone, 0, sslGarbageReadCb, ctx);
}

static void sslReadGarbageScenario(int flood)
{
  armDeathTestWatchdog(30);

  SslGarbageContext ctx;
  ctx.base = createAsyncBase(amOSDefault);  // own base: gBase must not be touched after fork()
  ctx.client = nullptr;
  ctx.serverConn = nullptr;
  ctx.flood = flood;

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = gPort;
  socketTy listenSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  socketReuseAddr(listenSocket);
  if (socketBind(listenSocket, &address) != 0)
    exit(2);
  if (socketListen(listenSocket) != 0)
    exit(2);
  std::thread(sslTlsThenGarbageServer, listenSocket, flood).detach();

  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  if (socketBind(clientSocket, &address) != 0)
    exit(2);

  ctx.client = sslSocketNew(ctx.base, newSocketIo(ctx.base, clientSocket));
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  // no timeout: the dead stream must produce an error, not wait for a rescue timer
  aioSslConnect(ctx.client, &address, nullptr, 0, sslGarbageHandshakeConnectCb, &ctx);

  asyncLoop(ctx.base);
  exit(3);  // event loop ended without the read callback
}

TEST(SslDeathTest, read_garbage_after_handshake)
{
  EXPECT_EXIT(sslReadGarbageScenario(0), ::testing::ExitedWithCode(0), "");
}

TEST(SslDeathTest, read_garbage_after_handshake_flood)
{
  EXPECT_EXIT(sslReadGarbageScenario(1), ::testing::ExitedWithCode(0), "");
}
#endif

// A write queued while the TLS handshake is still in flight must not report
// success for bytes that never entered the SSL stream. The server accepts the
// TCP connection but never answers the ClientHello, so the connect op fails
// by timeout; the queued write then runs with an incomplete handshake:
// SSL_write() rejects the payload (WANT_READ), but its result is ignored,
// bioOut is empty, and the operation completes with aosSuccess claiming the
// full payload size - the application believes the data was sent.
struct SslSilentServerContext {
  asyncBase *base;
  SSLSocket *client;
  aioObject *serverConn;
  AsyncOpStatus connectStatus;
  AsyncOpStatus writeStatus;
  size_t writeTransferred;
  bool writeFinished;
  SslSilentServerContext(asyncBase *baseArg) :
    base(baseArg), client(nullptr), serverConn(nullptr),
    connectStatus(aosUnknown), writeStatus(aosUnknown),
    writeTransferred(0), writeFinished(false) {}
};

static void sslSilentServerAcceptCb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  SslSilentServerContext *ctx = static_cast<SslSilentServerContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess)
    ctx->serverConn = newSocketIo(ctx->base, acceptSocket);  // keep the connection open, never answer
  else
    postQuitOperation(ctx->base);
}

static void sslWriteBeforeHandshakeConnectCb(AsyncOpStatus status, SSLSocket*, void *arg)
{
  SslSilentServerContext *ctx = static_cast<SslSilentServerContext*>(arg);
  ctx->connectStatus = status;
}

static void sslWriteBeforeHandshakeWriteCb(AsyncOpStatus status, SSLSocket*, size_t transferred, void *arg)
{
  SslSilentServerContext *ctx = static_cast<SslSilentServerContext*>(arg);
  ctx->writeStatus = status;
  ctx->writeTransferred = transferred;
  ctx->writeFinished = true;
  postQuitOperation(ctx->base);
}

TEST(ssl, write_before_handshake)
{
  static const char payload[] = "must not be reported as sent";

  SslSilentServerContext ctx(gBase);
  aioObject *listener = startTCPServer(gBase, sslSilentServerAcceptCb, &ctx, gPort);
  ASSERT_NE(listener, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);

  ctx.client = sslSocketNew(gBase, newSocketIo(gBase, clientSocket));
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioSslConnect(ctx.client, &address, nullptr, 300000, sslWriteBeforeHandshakeConnectCb, &ctx);
  // queued right behind the connect, as an application pipelining its first
  // request would do; the write timeout only bounds the test if a fix leaves
  // the operation waiting for a handshake that can no longer complete
  aioSslWrite(ctx.client, payload, sizeof(payload)-1, afNone, 3000000, sslWriteBeforeHandshakeWriteCb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.connectStatus, aosTimeout);
  ASSERT_TRUE(ctx.writeFinished);
  EXPECT_NE(ctx.writeStatus, aosSuccess)
    << "write reported success for " << ctx.writeTransferred
    << " bytes that never entered the TLS stream";

  sslSocketDelete(ctx.client);
  deleteAioObject(listener);
  if (ctx.serverConn)
    deleteAioObject(ctx.serverConn);
}

// SSL_write on a socket whose handshake was never even started can accept
// nothing: the operation must fail instead of claiming the payload was sent
// (the write path used to ignore the SSL_write result entirely).
struct SslWriteNoConnectContext {
  asyncBase *base;
  AsyncOpStatus status;
  size_t transferred;
  bool finished;
  SslWriteNoConnectContext(asyncBase *baseArg) :
    base(baseArg), status(aosUnknown), transferred(0), finished(false) {}
};

static void sslWriteNoConnectCb(AsyncOpStatus status, SSLSocket*, size_t transferred, void *arg)
{
  SslWriteNoConnectContext *ctx = static_cast<SslWriteNoConnectContext*>(arg);
  ctx->status = status;
  ctx->transferred = transferred;
  ctx->finished = true;
  postQuitOperation(ctx->base);
}

TEST(ssl, write_without_connect)
{
  static const char payload[] = "no handshake was ever started";

  SslWriteNoConnectContext ctx(gBase);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);
  SSLSocket *client = sslSocketNew(gBase, newSocketIo(gBase, clientSocket));

  aioSslWrite(client, payload, sizeof(payload)-1, afNone, 1000000, sslWriteNoConnectCb, &ctx);
  asyncLoop(gBase);

  ASSERT_TRUE(ctx.finished);
  EXPECT_EQ(ctx.status, aosUnknownError);
  EXPECT_EQ(ctx.transferred, 0u);
  sslSocketDelete(client);
}

// The SSL connect claims its object's exclusive slot the same way the plain
// TCP connect does: a second handshake submitted while the first is still in
// flight must be rejected immediately, without touching the shared SSL state
// machine.
struct SslDoubleConnectContext {
  asyncBase *base;
  AsyncOpStatus firstStatus;
  AsyncOpStatus secondStatus;
  int events;
  int firstOrder;
  int secondOrder;
  SslDoubleConnectContext(asyncBase *baseArg) :
    base(baseArg), firstStatus(aosUnknown), secondStatus(aosUnknown),
    events(0), firstOrder(-1), secondOrder(-1) {}
};

static void sslDoubleConnectFirstCb(AsyncOpStatus status, SSLSocket*, void *arg)
{
  SslDoubleConnectContext *ctx = static_cast<SslDoubleConnectContext*>(arg);
  ctx->firstStatus = status;
  ctx->firstOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

static void sslDoubleConnectSecondCb(AsyncOpStatus status, SSLSocket*, void *arg)
{
  SslDoubleConnectContext *ctx = static_cast<SslDoubleConnectContext*>(arg);
  ctx->secondStatus = status;
  ctx->secondOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

TEST(ssl, double_connect_rejected)
{
  SslDoubleConnectContext ctx(gBase);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  ASSERT_EQ(socketBind(clientSocket, &address), 0);
  SSLSocket *client = sslSocketNew(gBase, newSocketIo(gBase, clientSocket));

  address.ipv4 = inet_addr("192.0.2.1");
  address.port = 9;
  aioSslConnect(client, &address, nullptr, 300000, sslDoubleConnectFirstCb, &ctx);
  aioSslConnect(client, &address, nullptr, 300000, sslDoubleConnectSecondCb, &ctx);

  asyncLoop(gBase);
  sslSocketDelete(client);

  if (ctx.firstStatus != aosTimeout)
    GTEST_SKIP() << "blackhole answered (first connect status " << ctx.firstStatus
                 << "), exclusive slot contention cannot be exercised on this network";
  EXPECT_EQ(ctx.secondStatus, aosUnknownError);
  EXPECT_LT(ctx.secondOrder, ctx.firstOrder)
    << "the second SSL connect was not rejected while the first was in flight";
}

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

TEST(basic, test_tcp_read_pipelined_with_connect)
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
  aioConnect(client, &address, 300000, tcpPipelineConnectCb, &ctx);
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

// Connect is an exclusive object operation: it occupies the single
// aioObjectRoot::exclusiveOp slot. A second connect submitted while the
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

TEST(connect, double_connect_rejected)
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
  aioConnect(client, &address, 300000, doubleConnectFirstCb, &ctx);
  aioConnect(client, &address, 300000, doubleConnectSecondCb, &ctx);

  asyncLoop(gBase);
  deleteAioObject(client);

  if (ctx.firstStatus != aosTimeout)
    GTEST_SKIP() << "blackhole answered (first connect status " << ctx.firstStatus
                 << "), exclusive slot contention cannot be exercised on this network";
  EXPECT_EQ(ctx.secondStatus, aosUnknownError);
  EXPECT_LT(ctx.secondOrder, ctx.firstOrder)
    << "the second connect was not rejected while the first was in flight";
}

// deleteAioObject on an object with a connect in flight: the internal cancelIo
// sweep must find the operation in the exclusive slot (it is not in the
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

TEST(connect, delete_object_while_connecting)
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
// packet comes and the operation sits in the exclusive slot forever, holding
// the object and freezing its queues. A hang, not a late error - hence a
// death-test child, where the watchdog turns the hang into a verdict.
// The trigger is a second connect on an already-connected socket (submitted
// from the first connect's callback: the exclusive slot is released before
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
  ctx.base = createAsyncBase(amOSDefault); // own base: gBase must not be touched after fork()
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

TEST(accept, client_reset_in_backlog_does_not_fail_accept)
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

TEST(combiner, write_submission_order)
{
  constexpr uint32_t total = 100000;

  CombinerOrderContext ctx;
  ctx.base = createAsyncBase(amOSDefault);  // own base: MT hammering stays off gBase

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

TEST(basic, test_udp_read_truncated)
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

TEST(basic, test_udp_active_once_sync_budget)
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
TEST(basic, test_udp_fire_and_forget_sync_result)
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
struct ErrorWakeupContext {
  asyncBase *base;
  AsyncOpStatus status = aosPending;
  bool callbackFired = false;
  ErrorWakeupContext(asyncBase *baseArg) : base(baseArg) {}
};

void test_udp_icmp_error_readcb(AsyncOpStatus status, aioObject*, HostAddress, size_t, void *arg)
{
  ErrorWakeupContext *ctx = static_cast<ErrorWakeupContext*>(arg);
  ctx->status = status;
  ctx->callbackFired = true;
  postQuitOperation(ctx->base);
}

TEST(basic, test_udp_icmp_error_wakes_parked_read)
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

#ifdef OS_COMMONUNIX
void test_pipe_reader_close_writecb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  ErrorWakeupContext *ctx = static_cast<ErrorWakeupContext*>(arg);
  ctx->status = status;
  ctx->callbackFired = true;
  postQuitOperation(ctx->base);
}

TEST(basic, test_pipe_reader_close_wakes_parked_write)
{
  ErrorWakeupContext context(gBase);
  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);

  // Fill the pipe so the write below parks instead of completing inline
  char chunk[4096];
  memset(chunk, 0, sizeof(chunk));
  while (write(unnamedPipe.write, chunk, sizeof(chunk)) > 0)
    continue;
  ASSERT_EQ(errno, EAGAIN);

  aioObject *pipeWrite = newDeviceIo(gBase, unnamedPipe.write);
  char payload = 'x';
  aioWrite(pipeWrite, &payload, sizeof(payload), afNone, 1000000, test_pipe_reader_close_writecb, &context);

  close(unnamedPipe.read);

  asyncLoop(gBase);
  EXPECT_TRUE(context.callbackFired);
  EXPECT_NE(context.status, aosSuccess);
  EXPECT_NE(context.status, aosTimeout) << "the reader closing did not wake the parked write";
  deleteAioObject(pipeWrite);
}

// An idle object must not cost CPU, whatever state its fd is in. epoll
// registers the fd with an empty event mask right at object creation
// (epollNewAioObject) and going idle re-arms it with an empty mask, but
// EPOLLERR/EPOLLHUP cannot be masked out: the kernel reports them for as
// long as the condition holds. With no operation parked there is nothing to
// consume the condition, the event translates to an empty mask and is
// swallowed, so every epoll_wait returns immediately and the loop thread
// burns a full core until the object is deleted. Probed on the real kernel
// (scratchpad/acceptprobe/epollstorm.c): an fd registered with events=0
// floods EPOLLERR every call, while an EPOLLONESHOT-disarmed fd stays fully
// silent - so a parked operation does not storm and this is specifically
// about idle objects. A pipe whose reader is gone is the deterministic worst
// case: the error condition is permanent, nothing can consume it. kqueue
// registers filters per operation and iocp has no readiness reporting, so
// the contract holds there as is; the burn is epoll-only.
// The loop runs in its own thread so it can spin while this thread sleeps
// and meters the process CPU clock; the idle baseline over the window is
// microseconds, four orders of magnitude under the threshold.
TEST(basic, test_error_on_idle_object_keeps_loop_asleep)
{
  asyncBase *base = createAsyncBase(amOSDefault);
  std::thread loopThread([base]() { asyncLoop(base); });

  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);
  aioObject *pipeWrite = newDeviceIo(base, unnamedPipe.write);
  close(unnamedPipe.read);  // permanent error condition on an idle fd

  // let the loop thread face the condition before metering starts
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  rusage before, after;
  ASSERT_EQ(getrusage(RUSAGE_SELF, &before), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  ASSERT_EQ(getrusage(RUSAGE_SELF, &after), 0);

  auto cpuMicroseconds = [](const rusage &usage) {
    return static_cast<uint64_t>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000 +
           static_cast<uint64_t>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);
  };
  EXPECT_LT(cpuMicroseconds(after) - cpuMicroseconds(before), 200000u)
    << "the loop thread spins on the error condition of an idle object";

  deleteAioObject(pipeWrite);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  postQuitOperation(base);
  loopThread.join();
}
#endif

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

// coroutineCall on a running coroutine does not switch: it records the wakeup
// in the counter and returns, and the owner consumes the record at the next
// coroutineYield (which then resumes immediately instead of parking). When the
// wakeup lands after the final yield, the coroutine finishes with the record
// still pending, and the caller's re-entry loop (coroutinePosix.c do/while in
// coroutineCall, same shape in coroutineWin32.c) trusts the counter alone: it
// switches back into the finished context, where execution falls off the end
// of fiberEntryPoint - a garbage return address on POSIX, an implicit thread
// exit with Windows fibers. In production the window opens with several loop
// threads: one resumes an operation-owning coroutine that is about to return
// while another delivers a second wakeup for it (a user event, another
// operation on the same second). The handshake below forces that exact
// interleaving on every run, no timing involved. The scenario runs in a death
// test child: today it must die there; with the re-entry loop respecting
// `finished` it exits through the normal path - coroutineCall reports
// completion and the finish callback fires exactly once.
static coroutineTy *coroWakeupRaceCoroutine;
static std::atomic<int> coroWakeupRacePhase(0);
static int coroWakeupRaceFinishCalls = 0;

static void coroWakeupRaceFinishCb(void*)
{
  coroWakeupRaceFinishCalls++;
}

static void coroWakeupRaceProc(void*)
{
  coroWakeupRacePhase.store(1);
  while (coroWakeupRacePhase.load() != 2)
    std::this_thread::yield();
  // return without yielding: the recorded wakeup is still in the counter
}

static void coroWakeupRaceChild()
{
  // On Windows the broken path silently exits the calling thread instead of
  // crashing, which would leave the child hanging; turn a hang into a verdict
  std::thread watchdog([]() {
    std::this_thread::sleep_for(std::chrono::seconds(10));
    std::_Exit(3);
  });
  watchdog.detach();

  coroWakeupRaceCoroutine = coroutineNewWithCb(coroWakeupRaceProc, nullptr, 0x10000, coroWakeupRaceFinishCb, nullptr);
  std::thread wakeup([]() {
    while (coroWakeupRacePhase.load() != 1)
      std::this_thread::yield();
    coroutineCall(coroWakeupRaceCoroutine); // the coroutine is running: recorded, not switched
    coroWakeupRacePhase.store(2);
  });

  int finished = coroutineCall(coroWakeupRaceCoroutine);
  wakeup.join();
  // _Exit keeps the verdict to the scenario itself (no atexit, no leak check)
  std::_Exit(finished == 1 && coroWakeupRaceFinishCalls == 1 ? 42 : 1);
}

TEST(coroutine, wakeup_racing_finish)
{
  testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_EXIT(coroWakeupRaceChild(), testing::ExitedWithCode(42), "");
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
