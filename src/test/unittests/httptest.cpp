#include "unittest.h"

#include "asyncio/http.h"
#include "asyncio/socket.h"
#include "p2putils/HttpHeaderTable.h"

#include <cstdlib>
#include <cstring>
#include <string>

namespace {

// Standalone httpParseDefault driver with the same component envelope the
// client machinery uses: Initialize, one whole-buffer httpParse, Finalize
void parseDefaultResponse(HTTPParseDefaultContext &context, const char *response)
{
  HttpComponent component;
  component.type = httpDtInitialize;
  httpParseDefault(&component, &context);

  HttpParserState state;
  httpInit(&state);
  httpSetBuffer(&state, response, strlen(response));
  ASSERT_EQ(httpParse(&state, &httpParseDefaultTable, httpParseDefault, &context),
            ParserResultOk);

  component.type = httpDtFinalize;
  httpParseDefault(&component, &context);
}

}

// Content-Type is the first thing stored into the accumulation buffer, so it
// legitimately lands at offset 0; the absent-field marker must not swallow it
TEST(http, parse_default_reports_content_type)
{
  HTTPParseDefaultContext context;
  httpParseDefaultInit(&context, nullptr);
  parseDefaultResponse(context,
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: 5\r\n"
    "\r\n"
    "hello");

  EXPECT_EQ(context.resultCode, 200u);
  ASSERT_EQ(context.contentType.size, strlen("text/html"));
  ASSERT_NE(context.contentType.data, nullptr);
  EXPECT_EQ(memcmp(context.contentType.data, "text/html", context.contentType.size), 0);
  ASSERT_EQ(context.body.size, strlen("hello"));
  EXPECT_EQ(memcmp(context.body.data, "hello", context.body.size), 0);
  dynamicBufferFree(&context.buffer);
}

// Without Content-Type the body starts at offset 0 too; the second chunk must
// not be mistaken for the body start
TEST(http, parse_default_chunked_body_keeps_first_fragment)
{
  HTTPParseDefaultContext context;
  httpParseDefaultInit(&context, nullptr);
  parseDefaultResponse(context,
    "HTTP/1.1 200 OK\r\n"
    "Transfer-Encoding: chunked\r\n"
    "\r\n"
    "5\r\nhello\r\n"
    "6\r\n world\r\n"
    "0\r\n\r\n");

  ASSERT_EQ(context.body.size, strlen("hello world"));
  EXPECT_EQ(memcmp(context.body.data, "hello world", context.body.size), 0);
  dynamicBufferFree(&context.buffer);
}

TEST(http, parse_default_empty_body_with_content_type)
{
  HTTPParseDefaultContext context;
  httpParseDefaultInit(&context, nullptr);
  parseDefaultResponse(context,
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: text/html\r\n"
    "Content-Length: 0\r\n"
    "\r\n");

  ASSERT_EQ(context.contentType.size, strlen("text/html"));
  ASSERT_NE(context.contentType.data, nullptr);
  EXPECT_EQ(memcmp(context.contentType.data, "text/html", context.contentType.size), 0);
  EXPECT_EQ(context.body.size, 0u);
  ASSERT_NE(context.body.data, nullptr);
  EXPECT_EQ(context.body.data[0], 0);
  dynamicBufferFree(&context.buffer);
}

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

  if (!startTCPServer(ctx.base, httpOversizedHeaderAcceptCb, &ctx, gPort))
    exit(2);

  aioObject *clientIo = initializeTCPClient(ctx.base, nullptr, nullptr, 0);
  if (!clientIo)
    exit(2);

  ctx.client = httpClientNew(ctx.base, clientIo);
  httpParseDefaultInit(&ctx.parseContext, ctx.client);
  HostAddress address;
  address.family = AF_INET;
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
