#include "unittest.h"

#include "asyncio/socket.h"
#include "asyncio/socketSSL.h"

#include <openssl/evp.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>

#include <cstdlib>
#include <thread>

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

// The SSL connect claims its object's initialization slot the same way the plain
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
                 << "), initialization slot contention cannot be exercised on this network";
  EXPECT_EQ(ctx.secondStatus, aosUnknownError);
  EXPECT_LT(ctx.secondOrder, ctx.firstOrder)
    << "the second SSL connect was not rejected while the first was in flight";
}
