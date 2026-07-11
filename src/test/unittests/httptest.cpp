#include "unittest.h"

#include "asyncio/http.h"
#include "asyncio/socket.h"

#include <cstdlib>
#include <string>

// The server streams a header block larger than the client's fixed 64KB
// input buffer. The request must fail with aosBufferTooSmall instead of
// re-parsing the full buffer in a hot loop that pins the event loop thread
// forever (implRead of the 0 remaining bytes completes synchronously with 0,
// the parser reports NeedMoreData again, repeat). The scenario runs as a
// death test under a watchdog, so the hang turns into a killed child process
// and a test failure instead of a hung test suite.
#if GTEST_HAS_DEATH_TEST

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
  ctx.base = createAsyncBase(amOSDefault, 1);  // own base: gBase must not be touched after fork()
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

#endif
