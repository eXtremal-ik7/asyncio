#include "p2putils/uriParse.h"
#include "gtest/gtest.h"
#include <stdint.h>

namespace {

// Expected URI::ipv6 layout: groups in the order they are written in the
// literal, each group in host byte order.
void expectIpv6Host(const URI &uri, const uint16_t (&groups)[8])
{
  ASSERT_EQ(uri.hostType, static_cast<int>(URI::HostTypeIPv6));
  for (int i = 0; i < 8; i++)
    EXPECT_EQ(uri.ipv6[i], groups[i]) << "group " << i;
}

}

TEST(uriparse, test_scheme_and_dns_host)
{
  URI uri;
  ASSERT_EQ(uriParse("http://example.com", &uri), 1);
  EXPECT_EQ(uri.schema, "http");
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, 0);
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

// B2: regname after userinfo ("user@host") never leaves the StWaitRegname
// loop; until the fix these calls hang the test run.
TEST(uriparse, test_dns_host_with_userinfo)
{
  URI uri;
  ASSERT_EQ(uriParse("http://user@example.com/index.html", &uri), 1);
  EXPECT_EQ(uri.userInfo, "user");
  EXPECT_EQ(uri.hostType, static_cast<int>(URI::HostTypeDNS));
  EXPECT_EQ(uri.domain, "example.com");
  EXPECT_EQ(uri.port, 0);

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
