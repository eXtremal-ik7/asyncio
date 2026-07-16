// Runtime-prepared header recognition tables: httpHeaderTablePrepare
// validation rules and the lookup roundtrip; no parser or networking involved

#include "p2putils/HttpHeaderTable.h"
#include "p2putils/HttpParseCommon.h"
#include "HttpTokens.h"
#include "gtest/gtest.h"
#include <string.h>
#include <string>
#include <vector>

namespace {

// Feeds "<name>:" to the table lookup and returns the delivered id;
// -1 reports a verdict other than ParserResultOk
int lookupId(const HttpHeaderTable &table, const std::string &name)
{
  std::string line = name + ":";
  const char *p = line.data();
  int token = -1;
  if (httpHeaderTableLookup(&table, &p, line.data() + line.size(), &token) != ParserResultOk)
    return -1;
  return token;
}

TEST(http_header_table, prepare_roundtrip)
{
  const HttpHeaderTableEntry entries[] = {
    {"X-Alpha", 1},
    {"X-Beta", 2},
    {"Session-Token", 777},
    {"Content-Md5", 0x3FFFFFFF}
  };

  HttpHeaderTable table;
  ASSERT_TRUE(httpHeaderTablePrepare(&table, entries, 4));
  EXPECT_EQ(lookupId(table, "X-Alpha"), 1);
  EXPECT_EQ(lookupId(table, "x-alpha"), 1);
  EXPECT_EQ(lookupId(table, "X-ALPHA"), 1);
  EXPECT_EQ(lookupId(table, "X-Beta"), 2);
  EXPECT_EQ(lookupId(table, "session-token"), 777);
  EXPECT_EQ(lookupId(table, "Content-Md5"), 0x3FFFFFFF);
  // a valid name missing from the table is delivered as 0, not an error
  EXPECT_EQ(lookupId(table, "X-Gamma"), 0);
  httpHeaderTableFree(&table);
}

TEST(http_header_table, reserved_names_always_recognized)
{
  // count == 0 is legal and yields the reserved-only table
  HttpHeaderTable table;
  ASSERT_TRUE(httpHeaderTablePrepare(&table, nullptr, 0));
  EXPECT_EQ(lookupId(table, "Content-Length"), hhContentLength);
  EXPECT_EQ(lookupId(table, "transfer-encoding"), hhTransferEncoding);
  EXPECT_EQ(lookupId(table, "HOST"), hhHost);
  EXPECT_EQ(lookupId(table, "Location"), hhLocation);
  EXPECT_EQ(lookupId(table, "X-Anything"), 0);
  httpHeaderTableFree(&table);

  // the reserved names ride along with any user list
  const HttpHeaderTableEntry entries[] = {{"X-Alpha", 5}};
  ASSERT_TRUE(httpHeaderTablePrepare(&table, entries, 1));
  EXPECT_EQ(lookupId(table, "X-Alpha"), 5);
  EXPECT_EQ(lookupId(table, "content-encoding"), hhContentEncoding);
  EXPECT_EQ(lookupId(table, "Connection"), hhConnection);
  httpHeaderTableFree(&table);

  // Content-Type is not reserved: a user list may claim it
  const HttpHeaderTableEntry contentType[] = {{"Content-Type", 5}};
  ASSERT_TRUE(httpHeaderTablePrepare(&table, contentType, 1));
  EXPECT_EQ(lookupId(table, "content-type"), 5);
  httpHeaderTableFree(&table);
}

TEST(http_header_table, parse_default_table)
{
  // the table of the httpParseDefault callback: its own names plus the
  // riding reserved ones
  EXPECT_EQ(lookupId(httpParseDefaultTable, "Content-Type"), hpdContentType);
  EXPECT_EQ(lookupId(httpParseDefaultTable, "content-length"), hhContentLength);
  EXPECT_EQ(lookupId(httpParseDefaultTable, "X-Anything"), 0);
}

TEST(http_header_table, prepare_rejections)
{
  HttpHeaderTable table;

  const HttpHeaderTableEntry idZero[] = {{"X-Alpha", 0}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, idZero, 1));
  // a failed prepare leaves the table empty and safe to free
  httpHeaderTableFree(&table);

  const HttpHeaderTableEntry idNegative[] = {{"X-Alpha", -3}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, idNegative, 1));

  const HttpHeaderTableEntry idReservedRange[] = {{"X-Alpha", hhReservedBase}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, idReservedRange, 1));

  const HttpHeaderTableEntry duplicate[] = {{"X-Token", 1}, {"X-Token", 2}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, duplicate, 2));

  const HttpHeaderTableEntry duplicateFolded[] = {{"X-Token", 1}, {"x-tOKEN", 2}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, duplicateFolded, 2));

  const HttpHeaderTableEntry badCharset[] = {{"Bad Header", 1}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, badCharset, 1));

  const HttpHeaderTableEntry emptyName[] = {{"", 1}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, emptyName, 1));

  const HttpHeaderTableEntry nullName[] = {{nullptr, 1}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, nullName, 1));

  // reserved names can not be reassigned, in any case spelling
  const HttpHeaderTableEntry reservedName[] = {{"Content-Length", 1}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, reservedName, 1));

  const HttpHeaderTableEntry reservedFolded[] = {{"content-length", 1}};
  EXPECT_FALSE(httpHeaderTablePrepare(&table, reservedFolded, 1));
}

TEST(http_header_table, lookup_verdicts)
{
  HttpHeaderTable table;
  ASSERT_TRUE(httpHeaderTablePrepare(&table, nullptr, 0));

  // no ':' terminator reached: needs more data, the cursor stays untouched
  const char partial[] = "Content-Le";
  const char *p = partial;
  int token = -1;
  EXPECT_EQ(httpHeaderTableLookup(&table, &p, partial + strlen(partial), &token),
            ParserResultNeedMoreData);
  EXPECT_EQ(p, partial);

  const char badCharset[] = "Bad Header:";
  p = badCharset;
  EXPECT_EQ(httpHeaderTableLookup(&table, &p, badCharset + strlen(badCharset), &token),
            ParserResultError);

  const char emptyName[] = ":";
  p = emptyName;
  EXPECT_EQ(httpHeaderTableLookup(&table, &p, emptyName + strlen(emptyName), &token),
            ParserResultError);

  httpHeaderTableFree(&table);
}

TEST(http_header_table, big_table_roundtrip)
{
  std::vector<std::string> names;
  for (int i = 0; i < 300; i++)
    names.push_back("X-Test-" + std::to_string(i));

  std::vector<HttpHeaderTableEntry> entries;
  for (int i = 0; i < 300; i++)
    entries.push_back(HttpHeaderTableEntry{names[i].c_str(), i + 1});

  HttpHeaderTable table;
  ASSERT_TRUE(httpHeaderTablePrepare(&table, entries.data(), entries.size()));
  for (int i = 0; i < 300; i++)
    ASSERT_EQ(lookupId(table, names[i]), i + 1);
  EXPECT_EQ(lookupId(table, "Content-Length"), hhContentLength);
  httpHeaderTableFree(&table);
}

TEST(http_header_table, reuse_after_free)
{
  HttpHeaderTable table;
  const HttpHeaderTableEntry first[] = {{"X-Alpha", 1}};
  ASSERT_TRUE(httpHeaderTablePrepare(&table, first, 1));
  EXPECT_EQ(lookupId(table, "X-Alpha"), 1);
  httpHeaderTableFree(&table);

  // names are copied inside: the source list of the next prepare can differ
  const HttpHeaderTableEntry second[] = {{"X-Beta", 9}};
  ASSERT_TRUE(httpHeaderTablePrepare(&table, second, 1));
  EXPECT_EQ(lookupId(table, "X-Beta"), 9);
  EXPECT_EQ(lookupId(table, "X-Alpha"), 0);
  httpHeaderTableFree(&table);
}

}
