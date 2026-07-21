// HTTP request/response parser tests, no networking involved

#include "p2putils/HttpParse.h"
#include "p2putils/HttpRequestParse.h"
#include "unittest.h"
#include "gtest/gtest.h"
#include <algorithm>
#include <cstdlib>
#include <memory>
#include <string.h>
#include <string>
#include <vector>

namespace {

// Asserts that a parsed fragment equals the expected string; the size check
// stays first as the guard of the memcmp against out-of-bounds reads.
void expectData(const Raw &data, const char *expected)
{
  ASSERT_EQ(data.size, strlen(expected));
  ASSERT_EQ(memcmp(data.data, expected, strlen(expected)), 0);
}

// Single-buffer request parse driver: init, hand over the whole buffer,
// return the parser verdict. No table = the reserved names only.
ParserResultTy parseRequest(const void *request, size_t size, httpRequestParseCb *callback, void *arg,
                            const HttpHeaderTable *table = nullptr)
{
  HttpRequestParserState state;
  httpRequestParserInit(&state);
  httpRequestSetBuffer(&state, request, size);
  return httpRequestParse(&state, table, callback, arg);
}

struct RequestCompletionContext {
  bool done = false;
};

int requestCompletionCb(HttpRequestComponent *component, void *arg)
{
  RequestCompletionContext *context = static_cast<RequestCompletionContext*>(arg);
  if (component->type == httpRequestDtDataLast)
    context->done = true;
  return 1;
}

#if GTEST_HAS_DEATH_TEST

void parseRequestWithLeadingQueryByte(const char *request, size_t size,
                                      ParserResultTy expected)
{
  armDeathTestWatchdog(3);
  RequestCompletionContext context;
  ParserResultTy result = parseRequest(request, size, requestCompletionCb, &context);
  std::exit(result == expected && context.done ? 0 : 1);
}

TEST(UriParseDeathTest, empty_query_before_space_returns)
{
  const char request[] = "GET /? HTTP/1.1\r\n\r\n";
  EXPECT_EXIT(parseRequestWithLeadingQueryByte(request, sizeof(request)-1, ParserResultOk),
              ::testing::ExitedWithCode(0), "");
}

TEST(UriParseDeathTest, empty_query_before_fragment_returns)
{
  const char request[] = "GET /?#fragment HTTP/1.1\r\n\r\n";
  EXPECT_EXIT(parseRequestWithLeadingQueryByte(request, sizeof(request)-1, ParserResultOk),
              ::testing::ExitedWithCode(0), "");
}

#endif

void expectIncrementalRequestCompletes(const char *name, const char *first,
                                       const char *continuation)
{
  SCOPED_TRACE(name);

  const size_t firstSize = strlen(first);
  std::unique_ptr<char[]> exactBuffer(new char[firstSize]);
  memcpy(exactBuffer.get(), first, firstSize);

  HttpRequestParserState state;
  httpRequestParserInit(&state);
  httpRequestSetBuffer(&state, exactBuffer.get(), firstSize);
  RequestCompletionContext context;
  ASSERT_EQ(httpRequestParse(&state, nullptr, requestCompletionCb, &context),
            ParserResultNeedMoreData);
  ASSERT_FALSE(context.done);

  const char *remaining = static_cast<const char*>(httpRequestDataPtr(&state));
  const size_t remainingSize = httpRequestDataRemaining(&state);
  std::string resumed(remaining, remainingSize);
  resumed += continuation;
  exactBuffer.reset();

  httpRequestSetBuffer(&state, resumed.data(), resumed.size());
  ASSERT_EQ(httpRequestParse(&state, nullptr, requestCompletionCb, &context),
            ParserResultOk);
  EXPECT_TRUE(context.done);
}

TEST(http, request_line_incremental_resume_from_retained_tail)
{
  struct ResumeCase {
    const char *name;
    const char *first;
    const char *continuation;
  };
  static const ResumeCase cases[] = {
    {"path starts in next buffer", "GET ", "/x HTTP/1.1\r\n\r\n"},
    {"multi-space method separator is split", "GET ", " /x HTTP/1.1\r\n\r\n"},
    {"path ends at buffer boundary", "GET /abc", " HTTP/1.1\r\n\r\n"},
    {"path percent escape is split", "GET /%A", "B HTTP/1.1\r\n\r\n"},
    {"query starts in next buffer", "GET /?", "q=x HTTP/1.1\r\n\r\n"},
    {"query percent escape is split", "GET /?q=%A", "B HTTP/1.1\r\n\r\n"},
    {"fragment starts in next buffer", "GET /#", "frag HTTP/1.1\r\n\r\n"},
    {"fragment percent escape is split", "GET /#%A", "B HTTP/1.1\r\n\r\n"},
    {"version starts in next buffer", "GET /x ", "HTTP/1.1\r\n\r\n"},
    {"multi-space version separator is split", "GET /x ", " HTTP/1.1\r\n\r\n"},
    {"version token is split", "GET /x HTT", "P/1.1\r\n\r\n"}
  };

  for (const ResumeCase &test : cases)
    expectIncrementalRequestCompletes(test.name, test.first, test.continuation);
}

void httpRequestCb1Impl(HttpRequestComponent *component, void *arg)
{
  int *callNum = static_cast<int*>(arg);
  if (*callNum == 0) {
    ASSERT_EQ(component->type, httpRequestDtMethod);
    ASSERT_EQ(component->method, hmGet);
  } else if (*callNum == 1) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    expectData(component->data, "path");
  } else if (*callNum == 2) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    expectData(component->data, "to");
  } else if (*callNum == 3) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    expectData(component->data, "page");
  } else if (*callNum == 4) {
    ASSERT_EQ(component->type, httpRequestDtUriQueryElement);
    expectData(component->data, "qname");
    expectData(component->data2, "value");
  } else if (*callNum == 5) {
    ASSERT_EQ(component->type, httpRequestDtUriFragment);
    expectData(component->data, "fragment");
  } else if (*callNum == 6) {
    ASSERT_EQ(component->type, httpRequestDtVersion);
    ASSERT_EQ(component->version.majorVersion, 1u);
    ASSERT_EQ(component->version.minorVersion, 1u);
  } else if (*callNum == 7) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhHost);
    expectData(component->header.stringValue, "localhost:8080");
  } else if (*callNum == 8) {
    // not a reserved name: delivered untyped, the name and value are there
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUnknown);
    expectData(component->header.entryName, "User-Agent");
    expectData(component->header.stringValue, "curl/7.58.0");
  } else if (*callNum == 9) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUnknown);
    expectData(component->header.entryName, "Accept");
    expectData(component->header.stringValue, "*/*");
  } else if (*callNum == 10) {
    ASSERT_EQ(component->type, httpRequestDtDataLast);
    ASSERT_EQ(component->data.size, 0u);
  }

  (*callNum)++;
}

void httpRequestCb2Impl(HttpRequestComponent *component, void *arg)
{
  int *callNum = static_cast<int*>(arg);
  if (*callNum == 0) {
    ASSERT_EQ(component->type, httpRequestDtMethod);
    ASSERT_EQ(component->method, hmPost);
  } else if (*callNum == 1) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    expectData(component->data, "api");
  } else if (*callNum == 2) {
    ASSERT_EQ(component->type, httpRequestDtUriPathElement);
    expectData(component->data, "usercreate");
  } else if (*callNum == 3) {
    ASSERT_EQ(component->type, httpRequestDtVersion);
    ASSERT_EQ(component->version.majorVersion, 1u);
    ASSERT_EQ(component->version.minorVersion, 1u);
  } else if (*callNum == 4) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhHost);
    expectData(component->header.stringValue, "localhost:18880");
  } else if (*callNum == 5) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUnknown);
    expectData(component->header.entryName, "User-Agent");
    expectData(component->header.stringValue, "curl/7.58.0");
  } else if (*callNum == 6) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUnknown);
    expectData(component->header.entryName, "Accept");
    expectData(component->header.stringValue, "*/*");
  } else if (*callNum == 7) {
    // Content-Length delivers both the parsed number and the raw value
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhContentLength);
    ASSERT_EQ(component->header.sizeValue, 2u);
    expectData(component->header.stringValue, "2");
  } else if (*callNum == 8) {
    // Content-Type is not reserved: untyped without a table listing it
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUnknown);
    expectData(component->header.entryName, "Content-Type");
    expectData(component->header.stringValue, "application/x-www-form-urlencoded");
  } else if (*callNum == 9) {
    ASSERT_EQ(component->type, httpRequestDtDataLast);
    expectData(component->data, "{}");
  }

  (*callNum)++;
}

int httpRequestCb1(HttpRequestComponent *component, void *arg)
{
  httpRequestCb1Impl(component, arg);
  return 1;
}

TEST(http, http_request_parser)
{
  {
    int callNum = 0;
    const char request1[] = "GET /path/to/page?qname=value#fragment HTTP/1.1\r\nHost: localhost:8080\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\n\r\n\r\n";
    ParserResultTy result = parseRequest(request1, sizeof(request1)-1, httpRequestCb1, &callNum);
    ASSERT_EQ(result, ParserResultOk);
    ASSERT_EQ(callNum, 11);
  }

  {
    int callNum = 0;
    const char request[] = "POST /api/usercreate HTTP/1.1\r\nHost: localhost:18880\r\nUser-Agent: curl/7.58.0\r\nAccept: */*\r\nContent-Length: 2\r\nContent-Type: application/x-www-form-urlencoded\r\n\r\n{}";
    ParserResultTy result = parseRequest(request, sizeof(request)-1, [](HttpRequestComponent *component, void *arg) -> int {
      httpRequestCb2Impl(component, arg);
      return 1;
    }, &callNum);
    ASSERT_EQ(result, ParserResultOk);
    ASSERT_EQ(callNum, 10);
  }
}

struct HttpRequestTestContext {
  int method = -1;
  std::vector<int> headerTypes;
  std::vector<std::string> headerNames;
  std::vector<std::string> headerValues;
  size_t contentLength = 0;
  std::string body;
  bool done = false;
};

int httpRequestTestCb(HttpRequestComponent *component, void *arg)
{
  HttpRequestTestContext *ctx = static_cast<HttpRequestTestContext*>(arg);
  switch (component->type) {
    case httpRequestDtMethod :
      ctx->method = component->method;
      break;
    case httpRequestDtHeaderEntry :
      ctx->headerTypes.push_back(component->header.entryType);
      ctx->headerNames.emplace_back(component->header.entryName.data, component->header.entryName.size);
      ctx->headerValues.emplace_back(component->header.stringValue.data, component->header.stringValue.size);
      if (component->header.entryType == hhContentLength)
        ctx->contentLength = component->header.sizeValue;
      break;
    case httpRequestDtData :
      ctx->body.append(component->data.data, component->data.size);
      break;
    case httpRequestDtDataLast :
      if (component->data.size)
        ctx->body.append(component->data.data, component->data.size);
      ctx->done = true;
      break;
    default :
      break;
  }
  return 1;
}

ParserResultTy parseRequest(const void *request, size_t size, HttpRequestTestContext &context,
                            const HttpHeaderTable *table = nullptr)
{
  return parseRequest(request, size, httpRequestTestCb, &context, table);
}

TEST(http, chunked_body_incremental_resume_from_every_retained_tail)
{
  const std::string request =
    "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
    "3\r\nabc\r\n"
    "4;ext=v\r\ndefg\r\n"
    "0\r\n\r\n";

  for (size_t cut = 0; cut < request.size(); cut++) {
    SCOPED_TRACE(cut);

    std::unique_ptr<char[]> first(new char[cut ? cut : 1]);
    if (cut)
      memcpy(first.get(), request.data(), cut);

    HttpRequestParserState state;
    httpRequestParserInit(&state);
    httpRequestSetBuffer(&state, first.get(), cut);
    HttpRequestTestContext context;
    ParserResultTy result = httpRequestParse(&state, nullptr, httpRequestTestCb,
                                             &context);
    if (result != ParserResultNeedMoreData) {
      ADD_FAILURE() << "proper prefix returned " << result;
      continue;
    }
    ASSERT_FALSE(context.done);

    const char *remaining = static_cast<const char*>(httpRequestDataPtr(&state));
    const size_t remainingSize = httpRequestDataRemaining(&state);
    std::string resumed(remaining, remainingSize);
    resumed.append(request.data() + cut, request.size() - cut);
    first.reset();

    httpRequestSetBuffer(&state, resumed.data(), resumed.size());
    result = httpRequestParse(&state, nullptr, httpRequestTestCb, &context);
    EXPECT_EQ(result, ParserResultOk);
    EXPECT_TRUE(context.done);
    EXPECT_EQ(context.body, "abcdefg");
  }
}

struct HttpResponseTestContext {
  unsigned code = 0;
  std::string description;
  std::vector<int> headerTypes;
  std::vector<std::string> headerValues;
  size_t contentLength = 0;
  std::string body;
  int finalDataEvents = 0;
};

void httpResponseTestCb(HttpComponent *component, void *arg)
{
  HttpResponseTestContext *ctx = static_cast<HttpResponseTestContext*>(arg);
  switch (component->type) {
    case httpDtStartLine :
      ctx->code = component->startLine.code;
      ctx->description.assign(component->startLine.description.data, component->startLine.description.size);
      break;
    case httpDtHeaderEntry :
      ctx->headerTypes.push_back(component->header.entryType);
      ctx->headerValues.emplace_back(component->header.stringValue.data, component->header.stringValue.size);
      if (component->header.entryType == hhContentLength)
        ctx->contentLength = component->header.sizeValue;
      break;
    case httpDtData :
    case httpDtDataFragment :
      ctx->body.append(component->data.data, component->data.size);
      if (component->type == httpDtData)
        ctx->finalDataEvents++;
      break;
  }
}

// Response-side driver, symmetric to parseRequest.
ParserResultTy parseResponse(const void *response, size_t size, HttpResponseTestContext &context,
                             const HttpHeaderTable *table = nullptr)
{
  HttpParserState state;
  httpInit(&state);
  httpSetBuffer(&state, response, size);
  return httpParse(&state, table, httpResponseTestCb, &context);
}

void expectIncrementalRequestAtEveryCut(const std::string &request,
                                        const char *expectedBody)
{
  for (size_t cut = 0; cut < request.size(); cut++) {
    SCOPED_TRACE(cut);

    std::unique_ptr<char[]> first(new char[cut ? cut : 1]);
    if (cut)
      memcpy(first.get(), request.data(), cut);

    HttpRequestParserState state;
    httpRequestParserInit(&state);
    httpRequestSetBuffer(&state, first.get(), cut);
    HttpRequestTestContext context;
    ParserResultTy result = httpRequestParse(&state, nullptr, httpRequestTestCb,
                                             &context);
    ASSERT_EQ(result, ParserResultNeedMoreData);
    ASSERT_FALSE(context.done);

    const char *remaining = static_cast<const char*>(httpRequestDataPtr(&state));
    const size_t remainingSize = httpRequestDataRemaining(&state);
    std::string resumed(remaining, remainingSize);
    resumed.append(request.data() + cut, request.size() - cut);
    first.reset();

    httpRequestSetBuffer(&state, resumed.data(), resumed.size());
    result = httpRequestParse(&state, nullptr, httpRequestTestCb, &context);
    EXPECT_EQ(result, ParserResultOk);
    EXPECT_TRUE(context.done);
    EXPECT_EQ(context.body, expectedBody);
  }
}

void expectIncrementalResponseAtEveryCut(const std::string &response,
                                         const char *expectedBody,
                                         int expectedFinalDataEvents = -1)
{
  for (size_t cut = 0; cut < response.size(); cut++) {
    SCOPED_TRACE(cut);

    std::unique_ptr<char[]> first(new char[cut ? cut : 1]);
    if (cut)
      memcpy(first.get(), response.data(), cut);

    HttpParserState state;
    httpInit(&state);
    httpSetBuffer(&state, first.get(), cut);
    HttpResponseTestContext context;
    ParserResultTy result = httpParse(&state, nullptr, httpResponseTestCb,
                                      &context);
    ASSERT_EQ(result, ParserResultNeedMoreData);

    const char *remaining = static_cast<const char*>(httpDataPtr(&state));
    const size_t remainingSize = httpDataRemaining(&state);
    std::string resumed(remaining, remainingSize);
    resumed.append(response.data() + cut, response.size() - cut);
    first.reset();

    httpSetBuffer(&state, resumed.data(), resumed.size());
    result = httpParse(&state, nullptr, httpResponseTestCb, &context);
    EXPECT_EQ(result, ParserResultOk);
    EXPECT_EQ(context.body, expectedBody);
    if (expectedFinalDataEvents >= 0)
      EXPECT_EQ(context.finalDataEvents, expectedFinalDataEvents);
  }
}

TEST(http, fixed_length_request_incremental_resume_from_every_retained_tail)
{
  const std::string request =
    "POST /fixed HTTP/1.1\r\n"
    "X-Test: yes\r\n"
    "Content-Length: 11\r\n"
    "\r\n"
    "hello world";
  expectIncrementalRequestAtEveryCut(request, "hello world");
}

TEST(http, fixed_length_response_incremental_resume_from_every_retained_tail)
{
  const std::string response =
    "HTTP/1.1 200 OK\r\n"
    "X-Test: yes\r\n"
    "Content-Length: 11\r\n"
    "\r\n"
    "hello world";
  expectIncrementalResponseAtEveryCut(response, "hello world");
}

TEST(http, chunked_request_trailers_incremental_resume_from_every_retained_tail)
{
  const std::string request =
    "POST /trailers HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
    "3\r\nabc\r\n"
    "0\r\n"
    "First-Trailer: one\r\n"
    "Second-Trailer: two\r\n"
    "\r\n";
  expectIncrementalRequestAtEveryCut(request, "abc");
}

TEST(http, chunked_response_trailers_incremental_resume_from_every_retained_tail)
{
  const std::string response =
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
    "3\r\nabc\r\n"
    "0\r\n"
    "First-Trailer: one\r\n"
    "Second-Trailer: two\r\n"
    "\r\n";
  expectIncrementalResponseAtEveryCut(response, "abc", 1);
}

TEST(http, chunked_trailers_can_exceed_the_refill_buffer)
{
  const std::string trailers =
    "A: 12345\r\n"
    "B: 67890\r\n"
    "C: abcde\r\n"
    "\r\n";
  const size_t refillCapacity = 14;
  ASSERT_GT(trailers.size(), refillCapacity);

  const std::string requestPrefix =
    "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
    "1\r\nx\r\n0\r\n";
  HttpRequestParserState requestState;
  httpRequestParserInit(&requestState);
  httpRequestSetBuffer(&requestState, requestPrefix.data(), requestPrefix.size());
  HttpRequestTestContext requestContext;
  ASSERT_EQ(httpRequestParse(&requestState, nullptr, httpRequestTestCb,
                             &requestContext), ParserResultNeedMoreData);
  ASSERT_EQ(requestState.state, httpRequestTrailer);

  size_t requestOffset = 0;
  std::string requestRefill;
  while (requestOffset != trailers.size()) {
    const char *retained = static_cast<const char*>(httpRequestDataPtr(&requestState));
    const size_t retainedSize = httpRequestDataRemaining(&requestState);
    ASSERT_LT(retainedSize, refillCapacity);
    const size_t appended = std::min(refillCapacity - retainedSize,
                                     trailers.size() - requestOffset);
    std::string nextRefill(retained, retainedSize);
    nextRefill.append(trailers.data() + requestOffset, appended);
    requestRefill.swap(nextRefill);
    requestOffset += appended;

    httpRequestSetBuffer(&requestState, requestRefill.data(), requestRefill.size());
    ParserResultTy result = httpRequestParse(&requestState, nullptr,
                                             httpRequestTestCb, &requestContext);
    EXPECT_EQ(result, requestOffset == trailers.size()
                      ? ParserResultOk : ParserResultNeedMoreData);
  }
  EXPECT_TRUE(requestContext.done);
  EXPECT_EQ(requestContext.body, "x");

  const std::string responsePrefix =
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
    "1\r\nx\r\n0\r\n";
  HttpParserState responseState;
  httpInit(&responseState);
  httpSetBuffer(&responseState, responsePrefix.data(), responsePrefix.size());
  HttpResponseTestContext responseContext;
  ASSERT_EQ(httpParse(&responseState, nullptr, httpResponseTestCb,
                      &responseContext), ParserResultNeedMoreData);
  ASSERT_EQ(responseState.state, httpStTrailer);

  size_t responseOffset = 0;
  std::string responseRefill;
  while (responseOffset != trailers.size()) {
    const char *retained = static_cast<const char*>(httpDataPtr(&responseState));
    const size_t retainedSize = httpDataRemaining(&responseState);
    ASSERT_LT(retainedSize, refillCapacity);
    const size_t appended = std::min(refillCapacity - retainedSize,
                                     trailers.size() - responseOffset);
    std::string nextRefill(retained, retainedSize);
    nextRefill.append(trailers.data() + responseOffset, appended);
    responseRefill.swap(nextRefill);
    responseOffset += appended;

    httpSetBuffer(&responseState, responseRefill.data(), responseRefill.size());
    ParserResultTy result = httpParse(&responseState, nullptr,
                                      httpResponseTestCb, &responseContext);
    EXPECT_EQ(result, responseOffset == trailers.size()
                      ? ParserResultOk : ParserResultNeedMoreData);
  }
  EXPECT_EQ(responseContext.body, "x");
  EXPECT_EQ(responseContext.finalDataEvents, 1);
}

struct HeaderTokenPair {
  const char *name;
  int token;
};

// the complete reserved set: every name the library recognizes with any
// table; a stale committed table, an edited enum or a hash disagreement
// between the generator and the runtime lookup shows up here
const HeaderTokenPair gReservedHeaders[] = {
  {"Content-Length", hhContentLength},
  {"Transfer-Encoding", hhTransferEncoding},
  {"Content-Encoding", hhContentEncoding},
  {"Connection", hhConnection},
  {"Keep-Alive", hhKeepAlive},
  {"Host", hhHost},
  {"Expect", hhExpect},
  {"Upgrade", hhUpgrade},
  {"Location", hhLocation}
};

TEST(http, reserved_tokens)
{
  // Content-Length and Transfer-Encoding may not share a message, so the
  // names ride two requests
  const size_t count = sizeof(gReservedHeaders)/sizeof(gReservedHeaders[0]);
  std::string request = "GET /x HTTP/1.1\r\n";
  std::vector<int> expectedTypes;
  std::vector<std::string> expectedNames;
  for (size_t i = 0; i < count; i++) {
    if (gReservedHeaders[i].token == hhTransferEncoding)
      continue;
    request += gReservedHeaders[i].name;
    request += gReservedHeaders[i].token == hhContentLength ? ": 0\r\n" : ": x\r\n";
    expectedTypes.push_back(gReservedHeaders[i].token);
    expectedNames.emplace_back(gReservedHeaders[i].name);
  }
  request += "\r\n";

  HttpRequestTestContext context;
  ASSERT_EQ(parseRequest(request.data(), request.size(), context), ParserResultOk);
  ASSERT_TRUE(context.done);
  EXPECT_EQ(context.headerTypes, expectedTypes);
  EXPECT_EQ(context.headerNames, expectedNames);

  const char chunkedRequest[] = "GET /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n";
  HttpRequestTestContext teContext;
  ASSERT_EQ(parseRequest(chunkedRequest, sizeof(chunkedRequest)-1, teContext), ParserResultOk);
  ASSERT_TRUE(teContext.done);
  EXPECT_EQ(teContext.headerTypes, (std::vector<int>{hhTransferEncoding}));
  EXPECT_EQ(teContext.headerNames, (std::vector<std::string>{"Transfer-Encoding"}));

  static const HeaderTokenPair methods[] = {
    {"GET", hmGet},
    {"HEAD", hmHead},
    {"POST", hmPost},
    {"PUT", hmPut},
    {"DELETE", hmDelete},
    {"CONNECT", hmConnect},
    {"OPTIONS", hmOptions},
    {"TRACE", hmTrace},
    {"PATCH", hmPatch}
  };
  for (const HeaderTokenPair &method : methods) {
    std::string methodRequest = std::string(method.name) + " /x HTTP/1.1\r\n\r\n";
    HttpRequestTestContext methodContext;
    ASSERT_EQ(parseRequest(methodRequest.data(), methodRequest.size(), methodContext), ParserResultOk);
    EXPECT_EQ(methodContext.method, method.token) << method.name;
  }
}

TEST(http, parser_user_table)
{
  enum { myXCustom = 1, myETag = 2 };
  const HttpHeaderTableEntry entries[] = {{"X-Custom", myXCustom}, {"ETag", myETag}};
  HttpHeaderTable table;
  ASSERT_TRUE(httpHeaderTablePrepare(&table, entries, 2));

  // request side: user ids, the riding reserved names and untyped strangers
  const char request[] = "POST /x HTTP/1.1\r\n"
                         "X-Custom: a\r\n"
                         "etag: \"v1\"\r\n"
                         "Content-Length: 2\r\n"
                         "Server: s\r\n"
                         "\r\nab";
  HttpRequestTestContext context;
  ASSERT_EQ(parseRequest(request, sizeof(request)-1, context, &table), ParserResultOk);
  ASSERT_TRUE(context.done);
  EXPECT_EQ(context.headerTypes,
            (std::vector<int>{myXCustom, myETag, hhContentLength, hhUnknown}));
  EXPECT_EQ(context.headerValues,
            (std::vector<std::string>{"a", "\"v1\"", "2", "s"}));
  EXPECT_EQ(context.contentLength, 2u);
  EXPECT_EQ(context.body, "ab");

  // response side shares the lookup
  const char response[] = "HTTP/1.1 200 OK\r\nETAG: \"v2\"\r\nContent-Length: 0\r\n\r\n";
  HttpResponseTestContext responseContext;
  ASSERT_EQ(parseResponse(response, sizeof(response)-1, responseContext, &table), ParserResultOk);
  EXPECT_EQ(responseContext.headerTypes, (std::vector<int>{myETag, hhContentLength}));

  httpHeaderTableFree(&table);
}

TEST(http, header_incremental_resume)
{
  const HttpHeaderTableEntry entries[] = {{"X-Custom", 7}};
  HttpHeaderTable table;
  ASSERT_TRUE(httpHeaderTablePrepare(&table, entries, 1));

  // interrupted mid-name: nothing of the header line is consumed, the caller
  // re-feeds it from httpRequestDataPtr with more bytes appended
  HttpRequestTestContext context;
  HttpRequestParserState state;
  httpRequestParserInit(&state);
  const char firstPart[] = "GET /x HTTP/1.1\r\nX-Cus";
  httpRequestSetBuffer(&state, firstPart, sizeof(firstPart)-1);
  ASSERT_EQ(httpRequestParse(&state, &table, httpRequestTestCb, &context), ParserResultNeedMoreData);
  const size_t consumed = sizeof(firstPart)-1 - httpRequestDataRemaining(&state);
  EXPECT_EQ(std::string(firstPart + consumed), "X-Cus");

  const char resumed[] = "X-Custom: value\r\n\r\n";
  httpRequestSetBuffer(&state, resumed, sizeof(resumed)-1);
  ASSERT_EQ(httpRequestParse(&state, &table, httpRequestTestCb, &context), ParserResultOk);
  ASSERT_TRUE(context.done);
  EXPECT_EQ(context.headerTypes, (std::vector<int>{7}));
  EXPECT_EQ(context.headerValues, (std::vector<std::string>{"value"}));

  httpHeaderTableFree(&table);
}

TEST(http, header_name_near_miss)
{
  // names one edit away from Content-Length must stay unknown and must not
  // affect body framing. A colliding full 64-bit hash is not constructible
  // in a test, so the reject branch of the key verification is unreachable
  // from outside; the accept branch is pinned by header_name_case_insensitive
  // (a folded name reaches the Content-Length slot with an equal hash)
  static const char *names[] = {
    "Content-Lengt",     // truncated
    "Content-Lengths",   // extended
    "Content-Lengtx",    // same length, last byte differs
    "Kontent-Length"     // same length, first byte differs
  };
  for (const char *name : names) {
    std::string request = std::string("GET /x HTTP/1.1\r\n") + name + ": 999\r\n\r\n";
    HttpRequestTestContext context;
    ASSERT_EQ(parseRequest(request.data(), request.size(), context), ParserResultOk) << name;
    ASSERT_TRUE(context.done) << name;
    ASSERT_EQ(context.headerTypes.size(), 1u) << name;
    EXPECT_EQ(context.headerTypes[0], static_cast<int>(hhUnknown)) << name;
    EXPECT_EQ(context.headerNames[0], name);
    EXPECT_EQ(context.headerValues[0], "999") << name;
    EXPECT_EQ(context.body.size(), 0u) << name;
  }
}

TEST(http, header_name_case_insensitive)
{
  const char request[] = "POST /x HTTP/1.1\r\ncontent-length: 2\r\nX-CUSTOM: y\r\n\r\nab";
  HttpRequestTestContext context;
  ASSERT_EQ(parseRequest(request, sizeof(request)-1, context), ParserResultOk);
  ASSERT_TRUE(context.done);
  ASSERT_EQ(context.headerTypes.size(), 2u);
  EXPECT_EQ(context.headerTypes[0], static_cast<int>(hhContentLength));
  EXPECT_EQ(context.contentLength, 2u);
  EXPECT_EQ(context.headerTypes[1], static_cast<int>(hhUnknown));
  EXPECT_EQ(context.body, "ab");
}

TEST(http, request_chunked_body)
{
  // mixed-case chunked as the final coding, a chunk extension and a trailer
  const char request[] = "POST /x HTTP/1.1\r\n"
                         "TRANSFER-ENCODING: gzip, Chunked\r\n"
                         "\r\n"
                         "3\r\nabc\r\n"
                         "8;ext=val\r\n01234567\r\n"
                         "0\r\n"
                         "Trailer-Header: v\r\n"
                         "\r\n";
  HttpRequestTestContext context;
  ASSERT_EQ(parseRequest(request, sizeof(request)-1, context), ParserResultOk);
  ASSERT_TRUE(context.done);
  ASSERT_EQ(context.headerTypes.size(), 1u);
  EXPECT_EQ(context.headerTypes[0], static_cast<int>(hhTransferEncoding));
  EXPECT_EQ(context.body, "abc01234567");
}

TEST(http, malformed_header_names)
{
  static const char *requests[] = {
    "GET /x HTTP/1.1\r\nBad Header: x\r\n\r\n",         // space inside the name
    "GET /x HTTP/1.1\r\nContent-Length : 5\r\n\r\n",    // space before the colon
    "GET /x HTTP/1.1\r\n: x\r\n\r\n"                    // empty name
  };
  for (const char *request : requests) {
    HttpRequestTestContext context;
    EXPECT_EQ(parseRequest(request, strlen(request), context), ParserResultError) << request;
  }
}

TEST(http, bare_cr_lf_rejected_in_header_values)
{
  static const char controls[] = {'\r', '\n'};
  for (char control : controls) {
    SCOPED_TRACE(control == '\r' ? "bare CR" : "bare LF");

    std::string request = "GET /x HTTP/1.1\r\nX-Test: alpha";
    request.push_back(control);
    request += "beta\r\n\r\n";
    HttpRequestTestContext requestContext;
    EXPECT_EQ(parseRequest(request.data(), request.size(), requestContext),
              ParserResultError);
    EXPECT_FALSE(requestContext.done);

    std::string response = "HTTP/1.1 200 OK\r\nX-Test: alpha";
    response.push_back(control);
    response += "beta\r\nContent-Length: 0\r\n\r\n";
    HttpResponseTestContext responseContext;
    EXPECT_EQ(parseResponse(response.data(), response.size(), responseContext),
              ParserResultError);
    EXPECT_EQ(responseContext.finalDataEvents, 0);
  }
}

TEST(http, bare_cr_lf_rejected_in_response_reason_phrase)
{
  static const char controls[] = {'\r', '\n'};
  for (char control : controls) {
    SCOPED_TRACE(control == '\r' ? "bare CR" : "bare LF");

    std::string response = "HTTP/1.1 200 alpha";
    response.push_back(control);
    response += "beta\r\nContent-Length: 0\r\n\r\n";
    HttpResponseTestContext context;
    EXPECT_EQ(parseResponse(response.data(), response.size(), context),
              ParserResultError);
    EXPECT_EQ(context.code, 0u);
    EXPECT_EQ(context.finalDataEvents, 0);
  }
}

TEST(http, bare_cr_lf_rejected_in_chunked_trailers)
{
  static const char controls[] = {'\r', '\n'};
  for (char control : controls) {
    SCOPED_TRACE(control == '\r' ? "bare CR" : "bare LF");

    std::string request =
      "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n"
      "3\r\nabc\r\n0\r\nX-Trailer: alpha";
    request.push_back(control);
    request += "beta\r\n\r\n";
    HttpRequestTestContext requestContext;
    EXPECT_EQ(parseRequest(request.data(), request.size(), requestContext),
              ParserResultError);
    EXPECT_EQ(requestContext.body, "abc");
    EXPECT_FALSE(requestContext.done);

    std::string response =
      "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
      "3\r\nabc\r\n0\r\nX-Trailer: alpha";
    response.push_back(control);
    response += "beta\r\n\r\n";
    HttpResponseTestContext responseContext;
    EXPECT_EQ(parseResponse(response.data(), response.size(), responseContext),
              ParserResultError);
    EXPECT_EQ(responseContext.body, "abc");
    EXPECT_EQ(responseContext.finalDataEvents, 0);
  }
}

TEST(http, content_length_invalid)
{
  static const char *requests[] = {
    "GET /x HTTP/1.1\r\nContent-Length: 18446744073709551616\r\n\r\n",  // 2^64
    "GET /x HTTP/1.1\r\nContent-Length:\r\n\r\n",
    "GET /x HTTP/1.1\r\nContent-Length: 12x\r\n\r\n"
  };
  for (const char *request : requests) {
    HttpRequestTestContext context;
    EXPECT_EQ(parseRequest(request, strlen(request), context), ParserResultError) << request;
  }
}

TEST(http, request_framing_conflicts)
{
  // conflicting Content-Length / Transfer-Encoding sets let two HTTP nodes
  // disagree on the request boundary (request smuggling) - all are rejected
  static const char *requests[] = {
    "POST /x HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 0\r\n\r\nHELLO",                        // differing repeat
    "POST /x HTTP/1.1\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",           // CL, then TE
    "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n0\r\n\r\n",           // TE, then CL
    "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: identity\r\n\r\n0\r\n\r\n", // codings after chunked
    "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked, gzip\r\n\r\n0\r\n\r\n",                          // chunked not final
    "POST /x HTTP/1.1\r\nTransfer-Encoding: gzip chunked\r\n\r\n0\r\n\r\n",                          // missing list comma
    "POST /x HTTP/1.1\r\nTransfer-Encoding: identity\r\n\r\n"                                         // no chunked at all
  };
  for (const char *request : requests) {
    HttpRequestTestContext context;
    EXPECT_EQ(parseRequest(request, strlen(request), context), ParserResultError) << request;
  }

  // an identical repeated Content-Length is recoverable (RFC 9110 8.6)
  const char identical[] = "POST /x HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nHELLO";
  HttpRequestTestContext identicalContext;
  ASSERT_EQ(parseRequest(identical, sizeof(identical)-1, identicalContext), ParserResultOk);
  ASSERT_TRUE(identicalContext.done);
  EXPECT_EQ(identicalContext.body, "HELLO");

  // a list joined over two lines with chunked final still frames as chunked
  const char joined[] = "POST /x HTTP/1.1\r\n"
                        "Transfer-Encoding: gzip\r\nTransfer-Encoding: chunked\r\n"
                        "\r\n3\r\nabc\r\n0\r\n\r\n";
  HttpRequestTestContext joinedContext;
  ASSERT_EQ(parseRequest(joined, sizeof(joined)-1, joinedContext), ParserResultOk);
  ASSERT_TRUE(joinedContext.done);
  EXPECT_EQ(joinedContext.body, "abc");
}

TEST(http, response_framing_conflicts)
{
  // same rejections on the response side; the last case would need
  // read-until-close framing the parser does not have
  static const char *responses[] = {
    "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nContent-Length: 0\r\n\r\nHELLO",
    "HTTP/1.1 200 OK\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Length: 5\r\n\r\n0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip chunked\r\n\r\n0\r\n\r\n",
    "HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n"
  };
  for (const char *response : responses) {
    HttpResponseTestContext context;
    EXPECT_EQ(parseResponse(response, strlen(response), context), ParserResultError) << response;
  }
}

TEST(http, malformed_chunked_framing)
{
  static const char *bodies[] = {
    "4\r\nWikiXX0\r\n\r\n",       // payload must be followed by CRLF
    "4garbage\r\nWiki\r\n0\r\n\r\n" // chunk extensions require a semicolon
  };

  for (const char *body : bodies) {
    std::string request = "POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
    request += body;
    HttpRequestTestContext requestContext;
    EXPECT_EQ(parseRequest(request.data(), request.size(), requestContext), ParserResultError)
      << body;

    std::string response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
    response += body;
    HttpResponseTestContext responseContext;
    EXPECT_EQ(parseResponse(response.data(), response.size(), responseContext), ParserResultError)
      << body;
  }
}

TEST(http, method_case_sensitive)
{
  // RFC 9110: methods are case-sensitive, "get" is an unknown extension method
  const char request[] = "get /x HTTP/1.1\r\n\r\n";
  HttpRequestTestContext context;
  ASSERT_EQ(parseRequest(request, sizeof(request)-1, context), ParserResultOk);
  EXPECT_EQ(context.method, static_cast<int>(hmUnknown));
}

TEST(http, incremental_method_resume)
{
  HttpRequestTestContext context;
  HttpRequestParserState state;
  httpRequestParserInit(&state);

  // the incomplete method must not be consumed: the caller keeps the tail
  // starting at httpRequestDataPtr and appends freshly received bytes to it
  const char firstPart[] = "GE";
  httpRequestSetBuffer(&state, firstPart, sizeof(firstPart)-1);
  ASSERT_EQ(httpRequestParse(&state, nullptr, httpRequestTestCb, &context), ParserResultNeedMoreData);
  ASSERT_EQ(httpRequestDataPtr(&state), static_cast<const void*>(firstPart));
  ASSERT_EQ(httpRequestDataRemaining(&state), sizeof(firstPart)-1);

  const char request[] = "GET /x HTTP/1.1\r\n\r\n";
  httpRequestSetBuffer(&state, request, sizeof(request)-1);
  ASSERT_EQ(httpRequestParse(&state, nullptr, httpRequestTestCb, &context), ParserResultOk);
  EXPECT_EQ(context.method, static_cast<int>(hmGet));
  EXPECT_TRUE(context.done);
}

TEST(http, header_value_ows)
{
  const char request[] = "GET /x HTTP/1.1\r\nHost:\tlocalhost \r\nUser-Agent:no-space\r\n\r\n";
  HttpRequestTestContext context;
  ASSERT_EQ(parseRequest(request, sizeof(request)-1, context), ParserResultOk);
  ASSERT_EQ(context.headerValues.size(), 2u);
  EXPECT_EQ(context.headerValues[0], "localhost");
  EXPECT_EQ(context.headerValues[1], "no-space");
}

TEST(http, response_parser)
{
  {
    const char response[] = "HTTP/1.1 404 Not Found\r\n"
                            "Content-Type: text/html\r\n"
                            "ETag: \"abc\"\r\n"
                            "Content-Length: 5\r\n"
                            "\r\n"
                            "Hello";
    HttpResponseTestContext context;
    ASSERT_EQ(parseResponse(response, sizeof(response)-1, context), ParserResultOk);
    EXPECT_EQ(context.code, 404u);
    EXPECT_EQ(context.description, "Not Found");
    EXPECT_EQ(context.headerTypes, (std::vector<int>{hhUnknown, hhUnknown, hhContentLength}));
    EXPECT_EQ(context.headerValues, (std::vector<std::string>{"text/html", "\"abc\"", "5"}));
    EXPECT_EQ(context.contentLength, 5u);
    EXPECT_EQ(context.body, "Hello");
    EXPECT_EQ(context.finalDataEvents, 1);
  }

  {
    // reason phrase is optional
    const char response[] = "HTTP/1.1 200\r\n\r\n";
    HttpResponseTestContext context;
    ASSERT_EQ(parseResponse(response, sizeof(response)-1, context), ParserResultOk);
    EXPECT_EQ(context.code, 200u);
    EXPECT_EQ(context.description.size(), 0u);
    EXPECT_EQ(context.body.size(), 0u);
  }
}

TEST(http, response_chunked)
{
  const char response[] = "HTTP/1.1 200 OK\r\n"
                          "Transfer-Encoding: chunked\r\n"
                          "\r\n"
                          "4\r\nWiki\r\n"
                          "5\r\npedia\r\n"
                          "0\r\n"
                          "Expires: never\r\n"
                          "\r\n";
  HttpResponseTestContext context;
  ASSERT_EQ(parseResponse(response, sizeof(response)-1, context), ParserResultOk);
  EXPECT_EQ(context.code, 200u);
  EXPECT_EQ(context.headerTypes, (std::vector<int>{hhTransferEncoding}));
  EXPECT_EQ(context.body, "Wikipedia");
  EXPECT_EQ(context.finalDataEvents, 1);
}

}
