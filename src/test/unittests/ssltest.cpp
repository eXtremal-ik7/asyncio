#include "unittest.h"

#include "asyncio/coroutine.h"
#include "asyncio/socket.h"
#include "asyncio/socketSSL.h"

#ifdef OS_WINDOWS
#include "asyncio/device.h"
#else
#include <fcntl.h>
#include <sys/socket.h>
#endif

#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>

// Server SSL_CTX with a fresh self-signed certificate for helper-thread
// servers. Returns nullptr on failure; the certificate and key stay
// referenced by the context only.
static SSL_CTX *sslTestMakeServerContext()
{
  EVP_PKEY *key = EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "P-256");
  if (!key)
    return nullptr;
  X509 *cert = X509_new();
  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
  X509_gmtime_adj(X509_getm_notBefore(cert), -60);
  X509_gmtime_adj(X509_getm_notAfter(cert), 3600);
  X509_set_pubkey(cert, key);
  X509_NAME *subject = X509_get_subject_name(cert);
  X509_NAME_add_entry_by_txt(subject, "CN", MBSTRING_ASC, (const unsigned char*)"localhost", -1, -1, 0);
  X509_set_issuer_name(cert, subject);

  SSL_CTX *serverContext = nullptr;
  if (X509_sign(cert, key, EVP_sha256()) != 0) {
    serverContext = SSL_CTX_new(TLS_server_method());
    SSL_CTX_use_certificate(serverContext, cert);
    SSL_CTX_use_PrivateKey(serverContext, key);
  }
  X509_free(cert);
  EVP_PKEY_free(key);
  return serverContext;
}

// Blocking server side of a genuine TLS handshake, for helper threads.
// Returns the connected SSL (the caller frees it and closes *acceptedFd) or
// nullptr; *acceptedFd is valid whenever accept() succeeded. A receive
// timeout is armed before the handshake so a wedged peer fails the helper
// instead of blocking it forever.
static SSL *sslTestServerHandshake(socketTy listenSocket, socketTy *acceptedFd)
{
  *acceptedFd = (socketTy)(-1);
  socketTy fd = accept(listenSocket, nullptr, nullptr);
  if (fd == (socketTy)(-1))
    return nullptr;
  *acceptedFd = fd;

#ifdef OS_WINDOWS
  DWORD readTimeout = 5000;
#else
  timeval readTimeout;
  readTimeout.tv_sec = 5;
  readTimeout.tv_usec = 0;
#endif
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&readTimeout, sizeof(readTimeout));

  SSL_CTX *serverContext = sslTestMakeServerContext();
  if (!serverContext)
    return nullptr;
  SSL *ssl = SSL_new(serverContext);
  SSL_CTX_free(serverContext);  // the SSL keeps its own reference
  SSL_set_fd(ssl, (int)fd);
  if (SSL_accept(ssl) != 1) {
    SSL_free(ssl);
    ssl = nullptr;
  }
  return ssl;
}

#if GTEST_HAS_DEATH_TEST

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
  ctx.base = createAsyncBase(amOSDefault, 1);  // own base: gBase must not be touched after fork()
  ctx.client = nullptr;
  ctx.serverConn = nullptr;
  ctx.flood = flood;

  if (!startTCPServer(ctx.base, sslGarbageAcceptCb, &ctx, gPort))
    exit(2);

  aioObject *clientIo = initializeTCPClient(ctx.base, nullptr, nullptr, 0);
  if (!clientIo)
    exit(2);

  ctx.client = sslSocketNew(ctx.base, clientIo, nullptr);
  HostAddress address;
  address.family = AF_INET;
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
// initialization-slot handler must RELEASE on the terminal status instead of
// re-running the connect state machine. Re-executing connectProc() calls
// SSL_connect() again, gets WANT_READ again, and re-posts the handshake read on
// the dead socket - forever: the connect never completes, its initialization slot
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
  ctx.base = createAsyncBase(amOSDefault, 1);  // own base: gBase must not be touched after fork()
  ctx.client = nullptr;
  ctx.serverConn = nullptr;
  ctx.flood = 0;

  if (!startTCPServer(ctx.base, sslConnectPeerDropAcceptCb, &ctx, gPort))
    exit(2);

  aioObject *clientIo = initializeTCPClient(ctx.base, nullptr, nullptr, 0);
  if (!clientIo)
    exit(2);

  ctx.client = sslSocketNew(ctx.base, clientIo, nullptr);
  HostAddress address;
  address.family = AF_INET;
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
  socketTy fd;
  SSL *ssl = sslTestServerHandshake(listenSocket, &fd);
  if (!ssl)
    _Exit(4);

  do {
    if (send(fd, (const char*)gSslGarbage, (int)(sizeof(gSslGarbage)-1), 0) <= 0)
      break;
  } while (flood);
  SSL_free(ssl);
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
  ctx.base = createAsyncBase(amOSDefault, 1);  // own base: gBase must not be touched after fork()
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

  aioObject *clientIo = initializeTCPClient(ctx.base, nullptr, nullptr, 0);
  if (!clientIo)
    exit(2);

  ctx.client = sslSocketNew(ctx.base, clientIo, nullptr);
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

  aioObject *clientIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  ASSERT_NE(clientIo, nullptr);

  ctx.client = sslSocketNew(gBase, clientIo, nullptr);
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioSslConnect(ctx.client, &address, nullptr, 150000, sslWriteBeforeHandshakeConnectCb, &ctx);
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

  aioObject *clientIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  ASSERT_NE(clientIo, nullptr);
  SSLSocket *client = sslSocketNew(gBase, clientIo, nullptr);

  aioSslWrite(client, payload, sizeof(payload)-1, afNone, 1000000, sslWriteNoConnectCb, &ctx);
  asyncLoop(gBase);

  ASSERT_TRUE(ctx.finished);
  EXPECT_EQ(ctx.status, aosUnknownError);
  EXPECT_EQ(ctx.transferred, 0u);
  sslSocketDelete(client);
}

// The combiner installs the SSL initialization operation just as for plain TCP.
// A second handshake submitted while the first is still in flight must fail
// without touching the shared SSL state machine.
TEST(ssl, double_connect_rejected)
{
  DoubleConnectRecorder ctx(gBase);

  aioObject *clientIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  ASSERT_NE(clientIo, nullptr);
  SSLSocket *client = sslSocketNew(gBase, clientIo, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("192.0.2.1");
  address.port = 9;
  aioSslConnect(client, &address, nullptr, 150000, doubleConnectFirstCb<SSLSocket>, &ctx);
  aioSslConnect(client, &address, nullptr, 150000, doubleConnectSecondCb<SSLSocket>, &ctx);

  asyncLoop(gBase);
  sslSocketDelete(client);

  if (ctx.firstStatus != aosTimeout)
    GTEST_SKIP() << "blackhole answered (first connect status " << ctx.firstStatus
                 << "), initialization slot contention cannot be exercised on this network";
  EXPECT_EQ(ctx.secondStatus, aosUnknownError);
  EXPECT_LT(ctx.secondOrder, ctx.firstOrder)
    << "the second SSL connect was not rejected while the first was in flight";
}

// A caller-supplied SSL_CTX is shared, not consumed: the socket must take its
// own reference, so the caller may drop theirs right away and several sockets
// may share one context. ASAN turns a missing up_ref into a double free here.
TEST(ssl, shared_user_context)
{
  SSL_CTX *userContext = SSL_CTX_new(TLS_client_method());
  ASSERT_NE(userContext, nullptr);

  aioObject *firstIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  aioObject *secondIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  ASSERT_NE(firstIo, nullptr);
  ASSERT_NE(secondIo, nullptr);

  SSLSocket *first = sslSocketNew(gBase, firstIo, userContext);
  SSLSocket *second = sslSocketNew(gBase, secondIo, userContext);
  ASSERT_NE(first, nullptr);
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(first->sslContext, userContext);
  EXPECT_EQ(second->sslContext, userContext);

  SSL_CTX_free(userContext);  // the sockets keep the context alive on their own
  sslSocketDelete(first);
  sslSocketDelete(second);
}

// SSL_MODE_ENABLE_PARTIAL_WRITE in a caller-supplied SSL_CTX makes SSL_write
// accept as little as one TLS record (16K) per call. A positive result is
// not "the whole plaintext was taken": the write path must keep feeding the
// remainder, or the tail of the payload silently vanishes while the
// operation still reports the full size as sent. A real TLS server receives
// into a sink and the test compares byte counts on both sides; a receive
// timeout on the sink turns the lost tail into a countable shortfall
// instead of a hang. Two flavors: the write submitted after the handshake
// (synchronous fast path) and pipelined right behind the connect (queued
// operation path).
struct SslPartialWriteContext {
  asyncBase *base;
  std::vector<uint8_t> payload;
  bool writeFromConnectCb;
  AsyncOpStatus connectStatus;
  AsyncOpStatus writeStatus;
  size_t writeTransferred;
  bool writeFinished;
  ssize_t serverReceived;
  bool serverMismatch;
  SslPartialWriteContext(asyncBase *baseArg) :
    base(baseArg), writeFromConnectCb(false),
    connectStatus(aosUnknown), writeStatus(aosUnknown),
    writeTransferred(0), writeFinished(false),
    serverReceived(0), serverMismatch(false) {}
};

static void sslPartialWriteSink(socketTy listenSocket, SslPartialWriteContext *ctx)
{
  socketTy fd;
  SSL *ssl = sslTestServerHandshake(listenSocket, &fd);
  if (!ssl) {
    if (fd != (socketTy)(-1))
      socketClose(fd);
    ctx->serverReceived = -1;
    return;
  }

  size_t received = 0;
  uint8_t chunk[16384];
  while (received < ctx->payload.size()) {
    int R = SSL_read(ssl, chunk, (int)sizeof(chunk));
    if (R <= 0)
      break;
    if (received + (size_t)R > ctx->payload.size() ||
        memcmp(chunk, ctx->payload.data() + received, (size_t)R) != 0) {
      ctx->serverMismatch = true;
      break;
    }
    received += (size_t)R;
  }
  ctx->serverReceived = (ssize_t)received;
  SSL_free(ssl);
  socketClose(fd);
}

static void sslPartialWriteWriteCb(AsyncOpStatus status, SSLSocket*, size_t transferred, void *arg)
{
  SslPartialWriteContext *ctx = static_cast<SslPartialWriteContext*>(arg);
  ctx->writeStatus = status;
  ctx->writeTransferred = transferred;
  ctx->writeFinished = true;
  postQuitOperation(ctx->base);
}

static void sslPartialWriteConnectCb(AsyncOpStatus status, SSLSocket *socket, void *arg)
{
  SslPartialWriteContext *ctx = static_cast<SslPartialWriteContext*>(arg);
  ctx->connectStatus = status;
  if (status != aosSuccess) {
    postQuitOperation(ctx->base);
    return;
  }
  if (ctx->writeFromConnectCb)
    aioSslWrite(socket, ctx->payload.data(), ctx->payload.size(), afNone, 10000000, sslPartialWriteWriteCb, ctx);
}

static void sslPartialWriteScenario(bool writeFromConnectCb)
{
  SslPartialWriteContext ctx(gBase);
  ctx.writeFromConnectCb = writeFromConnectCb;
  // several records plus a partial one: a single SSL_write in partial-write
  // mode cannot take it whole
  ctx.payload.resize(100000);
  for (size_t i = 0; i < ctx.payload.size(); i++)
    ctx.payload[i] = (uint8_t)(i ^ (i >> 8) ^ (i >> 16));

  aioObject *clientIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  ASSERT_NE(clientIo, nullptr);

  SSL_CTX *userContext = SSL_CTX_new(TLS_client_method());
  ASSERT_NE(userContext, nullptr);
  SSL_CTX_set_verify(userContext, SSL_VERIFY_NONE, nullptr);
  SSL_CTX_set_mode(userContext, SSL_MODE_ENABLE_PARTIAL_WRITE);
  SSLSocket *client = sslSocketNew(gBase, clientIo, userContext);
  SSL_CTX_free(userContext);
  ASSERT_NE(client, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = gPort;
  socketTy listenSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  socketReuseAddr(listenSocket);
  ASSERT_EQ(socketBind(listenSocket, &address), 0);
  ASSERT_EQ(socketListen(listenSocket), 0);
  std::thread sink(sslPartialWriteSink, listenSocket, &ctx);

  address.ipv4 = inet_addr("127.0.0.1");
  aioSslConnect(client, &address, nullptr, 5000000, sslPartialWriteConnectCb, &ctx);
  if (!writeFromConnectCb)
    aioSslWrite(client, ctx.payload.data(), ctx.payload.size(), afNone, 10000000, sslPartialWriteWriteCb, &ctx);

  asyncLoop(gBase);
  sink.join();

  EXPECT_EQ(ctx.connectStatus, aosSuccess);
  EXPECT_TRUE(ctx.writeFinished);
  EXPECT_EQ(ctx.writeStatus, aosSuccess);
  EXPECT_EQ(ctx.writeTransferred, ctx.payload.size());
  EXPECT_EQ(ctx.serverReceived, (ssize_t)ctx.payload.size())
    << "the plaintext tail never entered the TLS stream";
  EXPECT_FALSE(ctx.serverMismatch);

  sslSocketDelete(client);
  socketClose(listenSocket);
}

TEST(ssl, partial_write_mode)
{
  sslPartialWriteScenario(true);
}

TEST(ssl, partial_write_mode_pipelined)
{
  sslPartialWriteScenario(false);
}

// implSslRead/implSslWrite hand a continuation operation back to the caller
// while the transport child was already pushed to the plain socket's combiner
// inside the impl call. This is the calling pattern of the HTTP/SMTP clients:
// they run under their own client-object combiner (not the SSL object's one)
// and push the returned continuation afterwards. The child's completion
// signal must wake the continuation even when the continuation is queued
// after that completion already happened: here a TLS read parks its transport
// child, the loop consumes the child's completion while the continuation is
// still unqueued, and only then is the continuation pushed - it must still
// complete with the decrypted payload instead of parking forever.
struct SslContinuationWakeupContext {
  asyncBase *base = nullptr;
  AsyncOpStatus connectStatus = aosUnknown;
  std::atomic<bool> readDone{false};
  AsyncOpStatus readStatus = aosUnknown;
  size_t readTransferred = 0;
  std::atomic<bool> serverReady{false};
  std::atomic<bool> serverSend{false};
  std::atomic<bool> serverExit{false};
  std::atomic<bool> serverFailed{false};
  uint8_t buffer[64] = {};
};

static const char gSslContinuationPayload[] = "wakeup";

static void sslContinuationWakeupSink(socketTy listenSocket, SslContinuationWakeupContext *ctx)
{
  socketTy fd;
  SSL *ssl = sslTestServerHandshake(listenSocket, &fd);
  if (!ssl) {
    ctx->serverFailed = true;
    ctx->serverReady = true;
    if (fd != (socketTy)(-1))
      socketClose(fd);
    return;
  }
  ctx->serverReady = true;
  while (!ctx->serverSend)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  if (SSL_write(ssl, gSslContinuationPayload, (int)strlen(gSslContinuationPayload)) <= 0)
    ctx->serverFailed = true;
  // keep the connection open: an EOF-driven read-queue sweep must not stand in
  // for the wakeup under test
  while (!ctx->serverExit)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  SSL_free(ssl);
  socketClose(fd);
}

static void sslContinuationWakeupConnectCb(AsyncOpStatus status, SSLSocket*, void *arg)
{
  SslContinuationWakeupContext *ctx = static_cast<SslContinuationWakeupContext*>(arg);
  ctx->connectStatus = status;
  postQuitOperation(ctx->base);
}

static void sslContinuationWakeupReadCb(AsyncOpStatus status, SSLSocket*, size_t transferred, void *arg)
{
  SslContinuationWakeupContext *ctx = static_cast<SslContinuationWakeupContext*>(arg);
  ctx->readStatus = status;
  ctx->readTransferred = transferred;
  ctx->readDone = true;
  postQuitOperation(ctx->base);
}

static void sslContinuationWakeupQuitCb(aioUserEvent*, void *arg)
{
  postQuitOperation(static_cast<asyncBase*>(arg));
}

TEST(ssl, read_continuation_pushed_after_child_completion)
{
  SslContinuationWakeupContext ctx;
  ctx.base = gBase;

  aioObject *clientIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  ASSERT_NE(clientIo, nullptr);
  SSLSocket *client = sslSocketNew(gBase, clientIo, nullptr);
  ASSERT_NE(client, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = gPort;
  socketTy listenSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  socketReuseAddr(listenSocket);
  ASSERT_EQ(socketBind(listenSocket, &address), 0);
  ASSERT_EQ(socketListen(listenSocket), 0);
  std::thread sink(sslContinuationWakeupSink, listenSocket, &ctx);
  // an early ASSERT return must release the helper, or its thread destructor
  // terminates the whole binary
  struct SinkGuard {
    SslContinuationWakeupContext *ctx;
    std::thread *sink;
    ~SinkGuard() { ctx->serverSend = true; ctx->serverExit = true; sink->join(); }
  } sinkGuard{&ctx, &sink};

  aioUserEvent *watchdog = newUserEvent(gBase, 0, sslContinuationWakeupQuitCb, gBase);

  address.ipv4 = inet_addr("127.0.0.1");
  aioSslConnect(client, &address, nullptr, 5000000, sslContinuationWakeupConnectCb, &ctx);
  asyncLoop(gBase);
  ASSERT_EQ(ctx.connectStatus, aosSuccess);
  auto readyDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!ctx.serverReady && std::chrono::steady_clock::now() < readyDeadline)
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  ASSERT_TRUE(ctx.serverReady.load());
  ASSERT_FALSE(ctx.serverFailed.load());

  // the caller's half of the client pattern: park a TLS read. The transport
  // child is pushed inside implSslRead; the continuation op is returned and
  // deliberately not pushed yet
  size_t bytes = 0;
  asyncOpRoot *continuationOp = implSslRead(client, ctx.buffer, sizeof(ctx.buffer), afNone, 0,
                                            sslContinuationWakeupReadCb, &ctx, &bytes);
  ASSERT_NE(continuationOp, nullptr);  // no transport data buffered: the read must park

  // let the transport child run to completion while the continuation stays
  // unqueued: the server sends one TLS record, the loop consumes the child's
  // completion (and with it the continuation's wakeup signal), the watchdog
  // then stops the loop
  resetQuitOperation(gBase);
  ctx.serverSend = true;
  userEventStartTimer(watchdog, 700000, 1);
  asyncLoop(gBase);
  userEventStopTimer(watchdog);
  ASSERT_FALSE(ctx.serverFailed.load());
  // the loop is stopped, no concurrent BIO access: ciphertext in the client
  // input BIO proves the transport child completed and signalled the (not yet
  // queued) continuation
  ASSERT_GT(BIO_ctrl_pending(client->bioIn), 0u);
  ASSERT_FALSE(ctx.readDone.load());

  // push the continuation exactly as the HTTP/SMTP clients do, then wait for
  // its completion with a rescue deadline
  resetQuitOperation(gBase);
  combinerPushOperation(continuationOp);
  userEventStartTimer(watchdog, 2500000, 1);
  asyncLoop(gBase);
  userEventStopTimer(watchdog);

  bool wedged = !ctx.readDone.load();
  EXPECT_FALSE(wedged) << "continuation op never completed: its child's wakeup was consumed before the op was queued";
  if (!wedged) {
    EXPECT_EQ(ctx.readStatus, aosSuccess);
    EXPECT_EQ(ctx.readTransferred, strlen(gSslContinuationPayload));
    EXPECT_EQ(memcmp(ctx.buffer, gSslContinuationPayload, strlen(gSslContinuationPayload)), 0);
  }

  // teardown: deleting the socket sweeps a wedged continuation; the cancel
  // callback both proves the op was parked in the queue and lets the object
  // release cleanly
  resetQuitOperation(gBase);
  sslSocketDelete(client);
  if (wedged) {
    userEventStartTimer(watchdog, 2000000, 1);
    asyncLoop(gBase);
    userEventStopTimer(watchdog);
    EXPECT_TRUE(ctx.readDone.load());
    EXPECT_EQ(ctx.readStatus, aosCanceled);
  }
  deleteUserEvent(watchdog);
  socketClose(listenSocket);
}

// address == NULL means "the transport is already connected, run only the
// TLS handshake" - the async flavor honors that, and the coroutine flavor
// must not dereference the null address instead. The transport is connected
// with ioConnect() first, then ioSslConnect(NULL) drives the handshake over
// it; a verified write proves the stream is genuinely established.
struct SslIoPreconnectedContext {
  SslPartialWriteContext *sink;
  aioObject *transport;
  SSLSocket *client;
  int tcpResult;
  int sslResult;
  ssize_t writeResult;
  bool finished;
  SslIoPreconnectedContext() :
    sink(nullptr), transport(nullptr), client(nullptr),
    tcpResult(-1), sslResult(-1), writeResult(-1), finished(false) {}
};

static void sslIoPreconnectedProc(void *arg)
{
  SslIoPreconnectedContext *ctx = static_cast<SslIoPreconnectedContext*>(arg);
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  ctx->tcpResult = ioConnect(ctx->transport, &address, 5000000);
  if (ctx->tcpResult == 0) {
    ctx->sslResult = ioSslConnect(ctx->client, nullptr, nullptr, 5000000);
    if (ctx->sslResult == 0)
      ctx->writeResult = ioSslWrite(ctx->client, ctx->sink->payload.data(), ctx->sink->payload.size(), afWaitAll, 5000000);
  }
  ctx->finished = true;
  postQuitOperation(gBase);
}

// The client-side TLS 1.3 handshake ends with SSL_connect() == 1 while the
// final flight (Finished) is still only queued to bioOut: the flush is a
// real transport write and the connect must not report success before it
// lands. Making that write fail deterministically needs no TCP timing (an
// RST races the data it discards, and IOCP accepts whole overlapped sends
// long before any reset): the transport is a duplex local channel with FIFO
// close semantics - everything written before the peer end closes is still
// delivered, anything the client writes afterwards dies with a broken-pipe
// error. The transport write queue is wedged with a parked bulk write, and
// the peer holds the server flight until it has SEEN bulk bytes arrive, so
// the Finished provably queues behind the wedge; then the peer closes.
// Both writes die and the connect must fail instead of reporting a session
// whose Finished never left the machine.
#ifdef OS_WINDOWS
struct SslPeerChannel {
  iodevTy transportEnd;
  iodevTy peerEnd;
};

static bool sslPeerChannelCreate(SslPeerChannel *channel)
{
  pipeTy pipe;
  if (pipeCreate(&pipe, 1) != 0)
    return false;
  // pipeCreate builds a duplex named pipe: both ends carry both directions
  channel->transportEnd = pipe.read;
  channel->peerEnd = pipe.write;
  return true;
}

static aioObject *sslPeerChannelOpen(SslPeerChannel *channel)
{
  return newDeviceIo(gBase, channel->transportEnd);
}

// event-based synchronous I/O on the overlapped peer end, bounded so a
// broken scenario fails the test instead of hanging it
static bool sslPeerIo(bool isRead, iodevTy handle, void *buffer, size_t size, size_t *transferred)
{
  OVERLAPPED overlapped;
  memset(&overlapped, 0, sizeof(overlapped));
  overlapped.hEvent = CreateEvent(nullptr, TRUE, FALSE, nullptr);
  BOOL started = isRead ? ReadFile(handle, buffer, (DWORD)size, nullptr, &overlapped)
                        : WriteFile(handle, buffer, (DWORD)size, nullptr, &overlapped);
  bool ok = false;
  if (started || GetLastError() == ERROR_IO_PENDING) {
    DWORD bytes = 0;
    if (WaitForSingleObject(overlapped.hEvent, 5000) == WAIT_OBJECT_0) {
      ok = GetOverlappedResult(handle, &overlapped, &bytes, FALSE) && bytes > 0;
      if (ok && transferred)
        *transferred = bytes;
    } else {
      CancelIoEx(handle, &overlapped);
      GetOverlappedResult(handle, &overlapped, &bytes, TRUE);
    }
  }
  CloseHandle(overlapped.hEvent);
  return ok;
}

static bool sslPeerRead(SslPeerChannel *channel, void *buffer, size_t size, size_t *transferred)
{
  return sslPeerIo(true, channel->peerEnd, buffer, size, transferred);
}

static bool sslPeerWrite(SslPeerChannel *channel, const void *buffer, size_t size)
{
  size_t sent = 0;
  while (sent < size) {
    size_t chunk = 0;
    if (!sslPeerIo(false, channel->peerEnd, (uint8_t*)(uintptr_t)buffer + sent, size - sent, &chunk))
      return false;
    sent += chunk;
  }
  return true;
}

static void sslPeerClose(SslPeerChannel *channel)
{
  CloseHandle(channel->peerEnd);
}
#else
struct SslPeerChannel {
  socketTy transportFd;
  socketTy peerFd;
};

static bool sslPeerChannelCreate(SslPeerChannel *channel)
{
  int fds[2];
  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
    return false;
  // newSocketIo expects what socketCreate produces: a non-blocking socket
  // with SIGPIPE suppressed per-descriptor where send() has no MSG_NOSIGNAL
  fcntl(fds[0], F_SETFL, O_NONBLOCK | fcntl(fds[0], F_GETFL));
#ifdef SO_NOSIGPIPE
  int optval = 1;
  setsockopt(fds[0], SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof(optval));
  setsockopt(fds[1], SOL_SOCKET, SO_NOSIGPIPE, &optval, sizeof(optval));
#endif
  // bound the peer reads so a broken scenario fails the test, not hangs it
  timeval readTimeout;
  readTimeout.tv_sec = 5;
  readTimeout.tv_usec = 0;
  setsockopt(fds[1], SOL_SOCKET, SO_RCVTIMEO, (const char*)&readTimeout, sizeof(readTimeout));
  channel->transportFd = fds[0];
  channel->peerFd = fds[1];
  return true;
}

static aioObject *sslPeerChannelOpen(SslPeerChannel *channel)
{
  return newSocketIo(gBase, channel->transportFd);
}

static bool sslPeerRead(SslPeerChannel *channel, void *buffer, size_t size, size_t *transferred)
{
  ssize_t result = recv(channel->peerFd, buffer, size, 0);
  if (result <= 0)
    return false;
  *transferred = (size_t)result;
  return true;
}

static bool sslPeerWrite(SslPeerChannel *channel, const void *buffer, size_t size)
{
  size_t sent = 0;
  while (sent < size) {
    ssize_t result = send(channel->peerFd, (const uint8_t*)buffer + sent, size - sent, 0);
    if (result <= 0)
      return false;
    sent += (size_t)result;
  }
  return true;
}

static void sslPeerClose(SslPeerChannel *channel)
{
  socketClose(channel->peerFd);
}
#endif

struct SslFinalFlightContext {
  asyncBase *base;
  aioObject *transport;
  SSLSocket *client;
  std::vector<uint8_t> bulk;
  aioUserEvent *bulkTrigger;
  AsyncOpStatus connectStatus;
  AsyncOpStatus bulkStatus;
  int events;
  bool serverFlightSent;
  SslFinalFlightContext(asyncBase *baseArg) :
    base(baseArg), transport(nullptr), client(nullptr), bulkTrigger(nullptr),
    connectStatus(aosUnknown), bulkStatus(aosUnknown),
    events(0), serverFlightSent(false) {}
};

// Server side of the handshake over memory BIOs: pump channel bytes into
// SSL_accept until the server flight is ready, prove the wedge is in place,
// deliver the flight, break the channel. No sleeps anywhere: every step
// waits for an observable state of the client.
static void sslFinalFlightPeerProc(SslPeerChannel *channel, SslFinalFlightContext *ctx)
{
  SSL_CTX *serverContext = sslTestMakeServerContext();
  if (!serverContext) {
    sslPeerClose(channel);
    return;
  }
  SSL *ssl = SSL_new(serverContext);
  SSL_CTX_free(serverContext);
  // the final-flight-after-SSL_connect()==1 shape exists only in TLS 1.3;
  // pin the version instead of trusting library/config defaults
  SSL_set_min_proto_version(ssl, TLS1_3_VERSION);
  BIO *bioIn = BIO_new(BIO_s_mem());
  BIO *bioOut = BIO_new(BIO_s_mem());
  SSL_set_bio(ssl, bioIn, bioOut);
  SSL_set_accept_state(ssl);

  uint8_t buffer[4096];
  bool flightReady = false;
  for (;;) {
    int acceptResult = SSL_accept(ssl);
    if (acceptResult == 1)
      break;  // cannot happen before the flight went out; tolerated by the pump
    if (BIO_ctrl_pending(bioOut) > 0) {
      flightReady = true;
      break;
    }
    if (SSL_get_error(ssl, acceptResult) != SSL_ERROR_WANT_READ)
      break;
    size_t got = 0;
    if (!sslPeerRead(channel, buffer, sizeof(buffer), &got))
      break;
    BIO_write(bioIn, buffer, (int)got);
  }

  if (flightReady) {
    // the bytes after the ClientHello are the client's bulk write: seeing
    // them proves the transport write queue is wedged, so the Finished that
    // follows the flight below cannot overtake it
    size_t got = 0;
    if (sslPeerRead(channel, buffer, sizeof(buffer), &got)) {
      size_t flightSize = BIO_ctrl_pending(bioOut);
      std::vector<uint8_t> flight(flightSize);
      BIO_read(bioOut, flight.data(), (int)flightSize);
      ctx->serverFlightSent = sslPeerWrite(channel, flight.data(), flight.size());
    }
  }
  // FIFO close: the flight above still reaches the client, while the parked
  // bulk write and the queued Finished die with a broken-pipe error
  sslPeerClose(channel);
  SSL_free(ssl);
}

static void sslFinalFlightQuitIfDone(SslFinalFlightContext *ctx)
{
  if (++ctx->events == 2)
    postQuitOperation(ctx->base);
}

static void sslFinalFlightConnectCb(AsyncOpStatus status, SSLSocket*, void *arg)
{
  SslFinalFlightContext *ctx = static_cast<SslFinalFlightContext*>(arg);
  ctx->connectStatus = status;
  sslFinalFlightQuitIfDone(ctx);
}

static void sslFinalFlightBulkCb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  SslFinalFlightContext *ctx = static_cast<SslFinalFlightContext*>(arg);
  ctx->bulkStatus = status;
  sslFinalFlightQuitIfDone(ctx);
}

static void sslFinalFlightTriggerCb(aioUserEvent*, void *arg)
{
  // fires after the ClientHello went out (the connect was submitted first on
  // this same thread); the peer will not release the server flight until
  // these bytes are seen on its side
  SslFinalFlightContext *ctx = static_cast<SslFinalFlightContext*>(arg);
  aioWrite(ctx->transport, ctx->bulk.data(), ctx->bulk.size(), afWaitAll, 10000000, sslFinalFlightBulkCb, ctx);
}

TEST(ssl, connect_fails_when_final_flight_write_fails)
{
  SslFinalFlightContext ctx(gBase);
  ctx.bulk.assign(1 << 20, 0x41);  // far beyond any channel buffering: parks and stays parked

  SslPeerChannel channel;
  ASSERT_TRUE(sslPeerChannelCreate(&channel));
  ctx.transport = sslPeerChannelOpen(&channel);
  ASSERT_NE(ctx.transport, nullptr);
  // both sides pin TLS 1.3: the scenario is built around its final-flight
  // handshake shape
  SSL_CTX *userContext = SSL_CTX_new(TLS_client_method());
  ASSERT_NE(userContext, nullptr);
  SSL_CTX_set_verify(userContext, SSL_VERIFY_NONE, nullptr);
  SSL_CTX_set_min_proto_version(userContext, TLS1_3_VERSION);
  ctx.client = sslSocketNew(gBase, ctx.transport, userContext);
  SSL_CTX_free(userContext);
  ASSERT_NE(ctx.client, nullptr);

  std::thread peer(sslFinalFlightPeerProc, &channel, &ctx);

  // preconnected transport: the channel needs no TCP connect stage
  aioSslConnect(ctx.client, nullptr, nullptr, 10000000, sslFinalFlightConnectCb, &ctx);
  ctx.bulkTrigger = newUserEvent(gBase, 0, sslFinalFlightTriggerCb, &ctx);
  ASSERT_NE(ctx.bulkTrigger, nullptr);
  userEventStartTimer(ctx.bulkTrigger, 100000, 1);

  asyncLoop(gBase);
  peer.join();

  EXPECT_TRUE(ctx.serverFlightSent);
  EXPECT_NE(ctx.bulkStatus, aosSuccess);
  EXPECT_NE(ctx.connectStatus, aosSuccess)
    << "connect reported success although the final handshake flight never reached the transport";

  deleteUserEvent(ctx.bulkTrigger);
  sslSocketDelete(ctx.client);
}

TEST(ssl, io_connect_preconnected_transport)
{
  SslPartialWriteContext sinkCtx(gBase);
  sinkCtx.payload.resize(1000);
  for (size_t i = 0; i < sinkCtx.payload.size(); i++)
    sinkCtx.payload[i] = (uint8_t)(i ^ (i >> 8));

  aioObject *clientIo = initializeTCPClient(gBase, nullptr, nullptr, 0);
  ASSERT_NE(clientIo, nullptr);
  SSLSocket *client = sslSocketNew(gBase, clientIo, nullptr);
  ASSERT_NE(client, nullptr);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = gPort;
  socketTy listenSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0);
  socketReuseAddr(listenSocket);
  ASSERT_EQ(socketBind(listenSocket, &address), 0);
  ASSERT_EQ(socketListen(listenSocket), 0);
  std::thread sink(sslPartialWriteSink, listenSocket, &sinkCtx);

  SslIoPreconnectedContext ctx;
  ctx.sink = &sinkCtx;
  ctx.transport = clientIo;
  ctx.client = client;
  coroutineTy *coroutine = coroutineNew(sslIoPreconnectedProc, &ctx, 0x10000);
  ASSERT_NE(coroutine, nullptr);
  coroutineCall(coroutine);

  asyncLoop(gBase);
  sink.join();

  EXPECT_TRUE(ctx.finished);
  EXPECT_EQ(ctx.tcpResult, 0);
  EXPECT_EQ(ctx.sslResult, 0);
  EXPECT_EQ(ctx.writeResult, (ssize_t)sinkCtx.payload.size());
  EXPECT_EQ(sinkCtx.serverReceived, (ssize_t)sinkCtx.payload.size());
  EXPECT_FALSE(sinkCtx.serverMismatch);

  sslSocketDelete(client);
  socketClose(listenSocket);
}
