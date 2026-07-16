// HTTP request/response parser tests, no networking involved

#include "p2putils/HttpParse.h"
#include "p2putils/HttpRequestParse.h"
#include "gtest/gtest.h"
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
// return the parser verdict.
ParserResultTy parseRequest(const void *request, size_t size, httpRequestParseCb *callback, void *arg)
{
  HttpRequestParserState state;
  httpRequestParserInit(&state);
  httpRequestSetBuffer(&state, request, size);
  return httpRequestParse(&state, callback, arg);
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
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhUserAgent);
    expectData(component->header.stringValue, "curl/7.58.0");
  } else if (*callNum == 9) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhAccept);
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
    ASSERT_EQ(component->header.entryType, hhUserAgent);
    expectData(component->header.stringValue, "curl/7.58.0");
  } else if (*callNum == 6) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhAccept);
    expectData(component->header.stringValue, "*/*");
  } else if (*callNum == 7) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhContentLength);
    ASSERT_EQ(component->header.sizeValue, 2u);
  } else if (*callNum == 8) {
    ASSERT_EQ(component->type, httpRequestDtHeaderEntry);
    ASSERT_EQ(component->header.entryType, hhContentType);
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
      if (component->header.entryType == hhContentLength) {
        ctx->contentLength = component->header.sizeValue;
        ctx->headerValues.emplace_back();
      } else {
        ctx->headerValues.emplace_back(component->header.stringValue.data, component->header.stringValue.size);
      }
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

ParserResultTy parseRequest(const void *request, size_t size, HttpRequestTestContext &context)
{
  return parseRequest(request, size, httpRequestTestCb, &context);
}

struct HttpResponseTestContext {
  unsigned code = 0;
  std::string description;
  std::vector<int> headerTypes;
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
ParserResultTy parseResponse(const void *response, size_t size, HttpResponseTestContext &context)
{
  HttpParserState state;
  httpInit(&state);
  httpSetBuffer(&state, response, size);
  return httpParse(&state, httpResponseTestCb, &context);
}

struct HeaderTokenPair {
  const char *name;
  int token;
};

// spot-check of the most common tokens from the generated table; a stale
// committed table, an edited enum or a hash disagreement between the
// generator and the runtime lookup shows up here
const HeaderTokenPair gCommonHeaders[] = {
  {"Accept", hhAccept},
  {"Accept-Encoding", hhAcceptEncoding},
  {"Authorization", hhAuthorization},
  {"Cache-Control", hhCacheControl},
  {"Connection", hhConnection},
  {"Content-Length", hhContentLength},
  {"Content-Type", hhContentType},
  {"Cookie", hhCookie},
  {"Date", hhDate},
  {"ETag", hhETag},
  {"Host", hhHost},
  {"Location", hhLocation},
  {"Server", hhServer},
  {"Set-Cookie", hhSetCookie},
  {"Transfer-Encoding", hhTransferEncoding},
  {"Upgrade", hhUpgrade},
  {"User-Agent", hhUserAgent},
  {"X-Forwarded-For", hhXForwardedFor}
};

TEST(http, common_tokens)
{
  const size_t count = sizeof(gCommonHeaders)/sizeof(gCommonHeaders[0]);
  std::string request = "GET /x HTTP/1.1\r\n";
  for (size_t i = 0; i < count; i++) {
    request += gCommonHeaders[i].name;
    if (gCommonHeaders[i].token == hhContentLength)
      request += ": 0\r\n";
    else if (gCommonHeaders[i].token == hhTransferEncoding)
      request += ": identity\r\n";
    else
      request += ": x\r\n";
  }
  request += "\r\n";

  HttpRequestTestContext context;
  ASSERT_EQ(parseRequest(request.data(), request.size(), context), ParserResultOk);
  ASSERT_TRUE(context.done);
  ASSERT_EQ(context.headerTypes.size(), count);
  for (size_t i = 0; i < count; i++) {
    EXPECT_EQ(context.headerTypes[i], gCommonHeaders[i].token) << gCommonHeaders[i].name;
    EXPECT_EQ(context.headerNames[i], gCommonHeaders[i].name);
  }

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
  ASSERT_EQ(httpRequestParse(&state, httpRequestTestCb, &context), ParserResultNeedMoreData);
  ASSERT_EQ(httpRequestDataPtr(&state), static_cast<const void*>(firstPart));
  ASSERT_EQ(httpRequestDataRemaining(&state), sizeof(firstPart)-1);

  const char request[] = "GET /x HTTP/1.1\r\n\r\n";
  httpRequestSetBuffer(&state, request, sizeof(request)-1);
  ASSERT_EQ(httpRequestParse(&state, httpRequestTestCb, &context), ParserResultOk);
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
    EXPECT_EQ(context.headerTypes, (std::vector<int>{hhContentType, hhETag, hhContentLength}));
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
