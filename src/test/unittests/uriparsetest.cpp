#include "p2putils/uriParse.h"
#include "gtest/gtest.h"
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

namespace {

typedef ParserResultTy UriRangeParser(const char **ptr, const char *end,
                                      bool uriOnly, uriParseCb callback, void *arg);

// Expected URI::ipv6 layout: groups in the order they are written in the
// literal, each group in host byte order.
void expectIpv6Host(const URI &uri, const uint16_t (&groups)[8])
{
  ASSERT_EQ(uri.hostType, static_cast<int>(URI::HostTypeIPv6));
  for (int i = 0; i < 8; i++)
    EXPECT_EQ(uri.ipv6[i], groups[i]) << "group " << i;
}

void expectTruncatedEscapeNeedsMore(UriRangeParser *parser,
                                    const char *source, size_t size)
{
  // Keep the allocation exact: there is deliberately no readable NUL byte
  // after end, so ASan catches either byte of percent-escape lookahead.
  char *buffer = new char[size];
  for (size_t i = 0; i < size; i++)
    buffer[i] = source[i];

  const char *ptr = buffer;
  int callbackCount = 0;
  ParserResultTy result = parser(&ptr, buffer + size, false,
    [](URIComponent*, void *arg) -> int {
      ++*static_cast<int*>(arg);
      return 1;
    }, &callbackCount);

  EXPECT_EQ(result, ParserResultNeedMoreData);
  EXPECT_EQ(ptr, buffer);
  EXPECT_EQ(callbackCount, 0);
  delete[] buffer;
}

struct QueryCapture {
  std::vector<std::pair<std::string, std::string> > elements;
  std::string raw;
};

int captureQuery(URIComponent *component, void *arg)
{
  QueryCapture *capture = static_cast<QueryCapture*>(arg);
  if (component->type == uriCtQueryElement) {
    capture->elements.push_back(std::make_pair(
      std::string(component->raw.data, component->raw.size),
      std::string(component->raw2.data, component->raw2.size)));
  } else if (component->type == uriCtQuery) {
    capture->raw.assign(component->raw.data, component->raw.size);
  }
  return 1;
}

struct RawComponentCapture {
  int type;
  std::vector<std::string> values;
};

int captureRawComponent(URIComponent *component, void *arg)
{
  RawComponentCapture *capture = static_cast<RawComponentCapture*>(arg);
  if (component->type == capture->type)
    capture->values.push_back(std::string(component->raw.data, component->raw.size));
  return 1;
}

}

TEST(uriparse, test_path_pct_escape_missing_both_hex_digits)
{
  const char path[] = {'/', '%'};
  expectTruncatedEscapeNeedsMore(uriParsePath, path, sizeof(path));
}

TEST(uriparse, test_path_pct_escape_missing_last_hex_digit)
{
  const char path[] = {'/', '%', 'A'};
  expectTruncatedEscapeNeedsMore(uriParsePath, path, sizeof(path));
}

TEST(uriparse, test_query_pct_escape_truncated_at_either_hex_digit)
{
  const char missingBoth[] = {'%'};
  expectTruncatedEscapeNeedsMore(uriParseQuery, missingBoth, sizeof(missingBoth));

  const char missingLast[] = {'%', 'A'};
  expectTruncatedEscapeNeedsMore(uriParseQuery, missingLast, sizeof(missingLast));
}

TEST(uriparse, test_fragment_pct_escape_truncated_at_either_hex_digit)
{
  const char missingBoth[] = {'%'};
  expectTruncatedEscapeNeedsMore(uriParseFragment, missingBoth, sizeof(missingBoth));

  const char missingLast[] = {'%', 'A'};
  expectTruncatedEscapeNeedsMore(uriParseFragment, missingLast, sizeof(missingLast));
}

TEST(uriparse, valid_pct_escape_at_exact_range_end)
{
  struct TestCase {
    UriRangeParser *parser;
    int componentType;
  } cases[] = {
    {uriParsePath, uriCtPath},
    {uriParseQuery, uriCtQuery},
    {uriParseFragment, uriCtFragment}
  };

  for (const TestCase &test : cases) {
    const char source[] = {'%', '4', '1'};
    const char *ptr = source;
    RawComponentCapture capture = {test.componentType, {}};

    ASSERT_EQ(test.parser(&ptr, source + sizeof(source), true,
                          captureRawComponent, &capture), ParserResultOk);
    EXPECT_EQ(ptr, source + sizeof(source));
    ASSERT_EQ(capture.values.size(), 1u);
    EXPECT_EQ(capture.values[0], "%41");
  }
}

TEST(uriparse, path_accepts_every_pchar_class)
{
  const char source[] = "AZaz09-._~!$&'()*+,;=:@%41 ";
  const char *ptr = source;
  RawComponentCapture path = {uriCtPath, {}};

  ASSERT_EQ(uriParsePath(&ptr, source + sizeof(source) - 1, false,
                         captureRawComponent, &path), ParserResultOk);
  EXPECT_EQ(ptr, source + sizeof(source) - 2);
  ASSERT_EQ(path.values.size(), 1u);
  EXPECT_EQ(path.values[0], "AZaz09-._~!$&'()*+,;=:@%41");
}

TEST(uriparse, malformed_pct_escape_must_fail_the_whole_uri)
{
  static const char *uris[] = {
    "http://%",
    "http://%A",
    "http://%zz/",
    "http://example.com/%",
    "http://example.com/%A",
    "http://example.com/%zz",
    "http://example.com/?q=%",
    "http://example.com/?q=%A",
    "http://example.com/?q=%zz",
    "http://example.com/#%",
    "http://example.com/#%A",
    "http://example.com/#%zz"
  };

  for (const char *source : uris) {
    URI uri;
    EXPECT_EQ(uriParse(source, &uri), 0) << source;
  }
}

TEST(uriparse, test_scheme_and_dns_host)
{
  URI uri;
  ASSERT_EQ(uriParse("http://example.com", &uri), 1);
  EXPECT_EQ(uri.schema, "http");
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, -1);
  EXPECT_TRUE(uri.userInfo.empty());
  EXPECT_TRUE(uri.path.empty());
  EXPECT_TRUE(uri.query.empty());
  EXPECT_TRUE(uri.fragment.empty());
}

TEST(uriparse, test_dns_host_with_port_and_fragment)
{
  URI uri;
  ASSERT_EQ(uriParse("https://example.com:8080#frag", &uri), 1);
  EXPECT_EQ(uri.schema, "https");
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, 8080);
  EXPECT_EQ(uri.fragment, "frag");
}

TEST(uriparse, test_ipv4_host)
{
  URI uri;
  ASSERT_EQ(uriParse("http://192.168.0.1:8080", &uri), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeIPv4));
  EXPECT_EQ(uri.ipv4, 192u | (168u << 8) | (0u << 16) | (1u << 24));
  EXPECT_EQ(uri.port, 8080);
}

TEST(uriparse, test_userinfo_with_ipv4_host)
{
  URI uri;
  ASSERT_EQ(uriParse("ftp://us%40er@10.0.0.1:21", &uri), 1);
  EXPECT_EQ(uri.userInfo, "us@er");
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeIPv4));
  EXPECT_EQ(uri.ipv4, 10u | (1u << 24));
  EXPECT_EQ(uri.port, 21);
}

TEST(uriparse, test_query_single_pair)
{
  URI uri;
  ASSERT_EQ(uriParse("http://example.com/p?x=1", &uri), 1);
  EXPECT_EQ(uri.query, "x=1");
}

TEST(uriparse, test_path_full_component)
{
  URI uri;
  ASSERT_EQ(uriParse("http://example.com/a/b/c", &uri), 1);
  EXPECT_EQ(uri.path, "/a/b/c");

  ASSERT_EQ(uriParse("http://example.com/index.html", &uri), 1);
  EXPECT_EQ(uri.path, "/index.html");
}

TEST(uriparse, test_query_full_component)
{
  URI uri;
  ASSERT_EQ(uriParse("http://example.com/p?x=1&y=2", &uri), 1);
  EXPECT_EQ(uri.query, "x=1&y=2");
}

TEST(uriparse, query_elements_continue_after_ampersand)
{
  const char source[] = "a=b&c=d ";
  const char *ptr = source;
  QueryCapture capture;

  ASSERT_EQ(uriParseQuery(&ptr, source + sizeof(source) - 1, false,
                          captureQuery, &capture), ParserResultOk);
  EXPECT_EQ(ptr, source + sizeof(source) - 2);
  ASSERT_EQ(capture.elements.size(), 2u);
  EXPECT_EQ(capture.elements[0], std::make_pair(std::string("a"), std::string("b")));
  EXPECT_EQ(capture.elements[1], std::make_pair(std::string("c"), std::string("d")));
  EXPECT_EQ(capture.raw, "a=b&c=d");
}

TEST(uriparse, dangling_query_name_after_ampersand_is_not_a_pair)
{
  const char source[] = "a=b&c ";
  const char *ptr = source;
  QueryCapture capture;

  ASSERT_EQ(uriParseQuery(&ptr, source + sizeof(source) - 1, false,
                          captureQuery, &capture), ParserResultOk);
  EXPECT_EQ(ptr, source + sizeof(source) - 2);
  ASSERT_EQ(capture.elements.size(), 1u);
  EXPECT_EQ(capture.elements[0], std::make_pair(std::string("a"), std::string("b")));
  EXPECT_EQ(capture.raw, "a=b&c");
}

TEST(uriparse, pct_decode_compacts_adjacent_escapes_and_embedded_nul)
{
  URI uri;
  ASSERT_EQ(uriParse("http://%41%42x%00@example.com/%43%44x%00?q=%45%46#%47%48",
                     &uri), 1);

  const char expectedUserInfo[] = {'A', 'B', 'x', '\0'};
  const char expectedPath[] = {'/', 'C', 'D', 'x', '\0'};
  EXPECT_EQ(uri.userInfo,
            std::string(expectedUserInfo, sizeof(expectedUserInfo)));
  EXPECT_EQ(uri.path, std::string(expectedPath, sizeof(expectedPath)));
  EXPECT_EQ(uri.query, "q=EF");
  EXPECT_EQ(uri.fragment, "GH");
}

// A regname after userinfo ("user@host") must leave the StWaitRegname loop;
// otherwise these calls hang the test run.
TEST(uriparse, test_dns_host_with_userinfo)
{
  URI uri;
  ASSERT_EQ(uriParse("http://user@example.com/index.html", &uri), 1);
  EXPECT_EQ(uri.userInfo, "user");
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, -1);

  ASSERT_EQ(uriParse("http://user@example.com", &uri), 1);
  EXPECT_EQ(uri.userInfo, "user");
  EXPECT_EQ(uri.domain, "example.com");

  ASSERT_EQ(uriParse("http://user:pass@example.com:8080", &uri), 1);
  EXPECT_EQ(uri.userInfo, "user:pass");
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, 8080);
}

TEST(uriparse, test_ipv6_loopback)
{
  URI uri;
  ASSERT_EQ(uriParse("http://[::1]/", &uri), 1);
  expectIpv6Host(uri, {0, 0, 0, 0, 0, 0, 0, 1});
}

TEST(uriparse, test_ipv6_full_form)
{
  URI uri;
  ASSERT_EQ(uriParse("http://[2001:0db8:0001:0002:0003:0004:0005:0006]", &uri), 1);
  expectIpv6Host(uri, {0x2001, 0x0db8, 1, 2, 3, 4, 5, 6});
}

TEST(uriparse, test_ipv6_compressed)
{
  URI uri;
  ASSERT_EQ(uriParse("http://[2001:db8::1]", &uri), 1);
  expectIpv6Host(uri, {0x2001, 0x0db8, 0, 0, 0, 0, 0, 1});

  ASSERT_EQ(uriParse("http://[::]", &uri), 1);
  expectIpv6Host(uri, {0, 0, 0, 0, 0, 0, 0, 0});

  ASSERT_EQ(uriParse("http://[fe80::]", &uri), 1);
  expectIpv6Host(uri, {0xfe80, 0, 0, 0, 0, 0, 0, 0});
}

TEST(uriparse, test_ipv6_with_port)
{
  URI uri;
  ASSERT_EQ(uriParse("http://[::1]:8080", &uri), 1);
  expectIpv6Host(uri, {0, 0, 0, 0, 0, 0, 0, 1});
  EXPECT_EQ(uri.port, 8080);
}

TEST(uriparse, test_ipv6_embedded_ipv4)
{
  URI uri;
  ASSERT_EQ(uriParse("http://[::ffff:192.168.0.1]", &uri), 1);
  expectIpv6Host(uri, {0, 0, 0, 0, 0, 0xffff, 0xc0a8, 0x0001});
}

TEST(uriparse, test_ipv6_malformed_must_fail)
{
  URI uri;
  EXPECT_EQ(uriParse("http://[:::1]/", &uri), 0);
  EXPECT_EQ(uriParse("http://[1::2::3]/", &uri), 0);
  EXPECT_EQ(uriParse("http://[12345::1]/", &uri), 0);
  EXPECT_EQ(uriParse("http://[gggg::1]/", &uri), 0);
  EXPECT_EQ(uriParse("http://[1:2:3:4:5:6:7]/", &uri), 0);
  EXPECT_EQ(uriParse("http://[1:2:3:4:5:6:7:8:9]/", &uri), 0);
  EXPECT_EQ(uriParse("http://[::1", &uri), 0);
  EXPECT_EQ(uriParse("http://[]", &uri), 0);
}

// Malformed input may be rejected or leniently accepted, but the parser must
// not crash or hang on it.
TEST(uriparse, test_malformed_inputs)
{
  const char *inputs[] = {
    "",
    "http:",
    "http://",
    "http://:",
    "http://:8080",
    "http://@",
    "http://user@",
    "http://user@:8080",
    "http://%",
    "http://%zz",
    "http://]",
    "a://b:c/d",
    "http://example.com:99999999",
  };

  for (const char *input : inputs) {
    URI uri;
    uriParse(input, &uri);
  }
}

// F22: a host starting with a digit is a reg-name unless the whole host is a
// valid dotted quad (RFC 3986)
TEST(uriparse, test_digit_first_dns_host)
{
  URI uri;
  ASSERT_EQ(uriParse("http://9gag.com/", &uri), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "9gag.com");

  ASSERT_EQ(uriParse("http://1.fi:8080/", &uri), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "1.fi");
  EXPECT_EQ(uri.port, 8080);

  ASSERT_EQ(uriParse("http://1.2.3.4.com/", &uri), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "1.2.3.4.com");

  ASSERT_EQ(uriParse("http://user@9gag.com:8080/", &uri), 1);
  EXPECT_EQ(uri.userInfo, "user");
  EXPECT_EQ(uri.domain, "9gag.com");
  EXPECT_EQ(uri.port, 8080);

  ASSERT_EQ(uriParse("http://192.168.0.1/", &uri), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeIPv4));
}

TEST(uriparse, test_hostport_dns)
{
  URI uri;
  ASSERT_EQ(uriParseHostPort("example.com:8080", &uri, 0), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, 8080);

  ASSERT_EQ(uriParseHostPort("example.com", &uri, 8333), 1);
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, 8333);

  ASSERT_EQ(uriParseHostPort("9gag.com:80", &uri, 0), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "9gag.com");
  EXPECT_EQ(uri.port, 80);
}

TEST(uriparse, test_hostport_ipv4)
{
  URI uri;
  ASSERT_EQ(uriParseHostPort("192.168.0.1:3357", &uri, 0), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeIPv4));
  EXPECT_EQ(uri.ipv4, 192u | (168u << 8) | (0u << 16) | (1u << 24));
  EXPECT_EQ(uri.port, 3357);

  ASSERT_EQ(uriParseHostPort("10.0.0.1", &uri, 8333), 1);
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeIPv4));
  EXPECT_EQ(uri.port, 8333);
}

TEST(uriparse, test_hostport_ipv6)
{
  URI uri;
  ASSERT_EQ(uriParseHostPort("[::1]:8080", &uri, 0), 1);
  expectIpv6Host(uri, {0, 0, 0, 0, 0, 0, 0, 1});
  EXPECT_EQ(uri.port, 8080);

  ASSERT_EQ(uriParseHostPort("[2001:db8::1]", &uri, 30303), 1);
  expectIpv6Host(uri, {0x2001, 0x0db8, 0, 0, 0, 0, 0, 1});
  EXPECT_EQ(uri.port, 30303);
}

TEST(uriparse, test_hostport_port_rules)
{
  URI uri;
  ASSERT_EQ(uriParseHostPort("example.com:65535", &uri, 0), 1);
  EXPECT_EQ(uri.port, 65535);

  // explicit port always wins over defaultPort, ":0" included
  ASSERT_EQ(uriParseHostPort("example.com:0", &uri, 8333), 1);
  EXPECT_EQ(uri.port, 0);

  EXPECT_EQ(uriParseHostPort("example.com:65536", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("example.com:99999999", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("example.com:", &uri, 0), 0);
}

// The authority port of the full uriParse must follow the same rules as the
// uriParseHostPort pair: values beyond 65535 are rejected (never accumulated
// into signed overflow), and an empty port keeps the "-1 = no explicit port"
// sentinel so build() cannot turn "host:/" into "host:0/". An explicit ":0"
// stays a real port, matching the hostport parser.
TEST(uriparse, test_authority_port_rules)
{
  URI uri;
  EXPECT_EQ(uriParse("http://example.com:65536/", &uri), 0);
  EXPECT_EQ(uriParse("http://example.com:9999999999/", &uri), 0);

  ASSERT_EQ(uriParse("http://example.com:/", &uri), 1);
  EXPECT_EQ(uri.port, -1);

  ASSERT_EQ(uriParse("http://example.com:0/", &uri), 1);
  EXPECT_EQ(uri.port, 0);
}

TEST(uriparse, test_hostport_must_reject)
{
  URI uri;
  EXPECT_EQ(uriParseHostPort("", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort(":8080", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("example.com:80x", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("example.com:8080/path", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("user@example.com:8080", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("http://example.com", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("[::1", &uri, 0), 0);
  EXPECT_EQ(uriParseHostPort("[::1]x", &uri, 0), 0);
}

namespace {

void expectUriFieldsEqual(const URI &expected, const URI &actual)
{
  EXPECT_EQ(actual.schema, expected.schema);
  EXPECT_EQ(actual.userInfo, expected.userInfo);
  ASSERT_EQ(actual.hostType, expected.hostType);
  if (expected.hostType == URI::HostTypeIPv4) {
    EXPECT_EQ(actual.ipv4, expected.ipv4);
  } else if (expected.hostType == URI::HostTypeIPv6) {
    for (int i = 0; i < 8; i++)
      EXPECT_EQ(actual.ipv6[i], expected.ipv6[i]) << "group " << i;
  } else if (expected.hostType == URI::HostTypeDNS) {
    EXPECT_EQ(actual.domain, expected.domain);
  }
  EXPECT_EQ(actual.port, expected.port);
  EXPECT_EQ(actual.path, expected.path);
  EXPECT_EQ(actual.query, expected.query);
  EXPECT_EQ(actual.fragment, expected.fragment);
}

// parse source, build, compare with the canonical form, then re-parse the
// built string and require component-level equality with the first parse
void expectRoundTrip(const char *source, const char *canonical)
{
  URI uri;
  ASSERT_EQ(uriParse(source, &uri), 1) << source;

  std::string built;
  uri.build(built);
  EXPECT_EQ(built, canonical) << source;

  URI reparsed;
  ASSERT_EQ(uriParse(built.c_str(), &reparsed), 1)
    << source << " built as " << built;
  expectUriFieldsEqual(uri, reparsed);
}

}

// RFC 5952 canonical form: lowercase hex, no leading zeros, the longest run
// of two or more zero groups compressed to "::", leftmost run on a tie
TEST(uriparse, build_roundtrip_ipv6_hosts)
{
  expectRoundTrip("http://[::1]/", "http://[::1]/");
  expectRoundTrip("http://[::]", "http://[::]");
  expectRoundTrip("http://[fe80::]", "http://[fe80::]");
  expectRoundTrip("http://[2001:db8::1]", "http://[2001:db8::1]");
  expectRoundTrip("http://[2001:0db8:0001:0002:0003:0004:0005:0006]",
                  "http://[2001:db8:1:2:3:4:5:6]");
  expectRoundTrip("http://[::ffff:192.168.0.1]", "http://[::ffff:c0a8:1]");
  expectRoundTrip("http://[1:0:2:3:4:5:6:7]", "http://[1:0:2:3:4:5:6:7]");
  expectRoundTrip("http://[1:0:0:2:0:0:3:4]", "http://[1::2:0:0:3:4]");
  expectRoundTrip("http://[1:0:0:2:0:0:0:3]", "http://[1:0:0:2::3]");
  expectRoundTrip("http://[::1]:8080/", "http://[::1]:8080/");
}

TEST(uriparse, build_omits_absent_port)
{
  URI uri;
  ASSERT_EQ(uriParse("http://example.com/", &uri), 1);
  EXPECT_EQ(uri.port, -1);

  expectRoundTrip("http://example.com/", "http://example.com/");
  expectRoundTrip("http://example.com", "http://example.com");
  expectRoundTrip("http://user@example.com/a/b?x=1#f",
                  "http://user@example.com/a/b?x=1#f");
}

TEST(uriparse, build_keeps_explicit_port)
{
  expectRoundTrip("http://example.com:8080/x", "http://example.com:8080/x");
  expectRoundTrip("http://example.com:0/", "http://example.com:0/");
  expectRoundTrip("http://192.168.0.1:3357", "http://192.168.0.1:3357");
}

// high bytes must be encoded from their unsigned value: 0xD0 is "%D0", not
// the sign-extended garbage "%40"
TEST(uriparse, build_pct_encodes_high_bytes)
{
  expectRoundTrip("http://example.com/%D0%BF?q=%D0%96#%D1%8F",
                  "http://example.com/%D0%BF?q=%D0%96#%D1%8F");
  expectRoundTrip("http://us%D0%AFer@example.com/",
                  "http://us%D0%AFer@example.com/");
}

TEST(uriparse, build_pct_encodes_embedded_nul)
{
  URI uri;
  ASSERT_EQ(uriParse("http://us%00er@example.com/a%00b", &uri), 1);

  std::string built;
  uri.build(built);
  EXPECT_EQ(built.find('\0'), std::string::npos);
  EXPECT_EQ(built, "http://us%00er@example.com/a%00b");
}
