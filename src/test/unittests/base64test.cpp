#include "unittest.h"

#include "asyncio/base64.h"

#include <array>
#include <cstring>
#include <limits>
#include <vector>

namespace {

TEST(base64, length_helpers_include_terminator)
{
  EXPECT_EQ(base64getEncodeLength(0), 1u);
  EXPECT_EQ(base64getEncodeLength(1), 5u);
  EXPECT_EQ(base64getEncodeLength(2), 5u);
  EXPECT_EQ(base64getEncodeLength(3), 5u);
  EXPECT_EQ(base64getEncodeLength(4), 9u);

  EXPECT_EQ(base64GetDecodeLength(""), 1u);
  EXPECT_EQ(base64GetDecodeLength("Zg=="), 2u);
  EXPECT_EQ(base64GetDecodeLength("Zm8="), 3u);
  EXPECT_EQ(base64GetDecodeLength("Zm9v"), 4u);
}

TEST(base64, encode_exact_capacity_preserves_canaries)
{
  struct EncodeCase {
    const char *plain;
    const char *encoded;
  };
  const EncodeCase cases[] = {
    {"", ""},
    {"f", "Zg=="},
    {"fo", "Zm8="},
    {"foo", "Zm9v"},
    {"foob", "Zm9vYg=="},
  };

  for (const EncodeCase &test : cases) {
    size_t inputSize = strlen(test.plain);
    size_t capacity = base64getEncodeLength(inputSize);
    ASSERT_NE(capacity, 0u);
    std::vector<unsigned char> storage(capacity + 2, 0xa5);
    char *out = reinterpret_cast<char*>(storage.data() + 1);

    size_t encodedSize = base64Encode(out,
                                      reinterpret_cast<const uint8_t*>(test.plain),
                                      inputSize);

    EXPECT_EQ(encodedSize + 1, capacity);
    EXPECT_STREQ(out, test.encoded);
    EXPECT_EQ(storage.front(), 0xa5);
    EXPECT_EQ(storage.back(), 0xa5);
  }
}

TEST(base64, decode_exact_capacity_handles_padding_and_malformed_suffixes)
{
  struct DecodeCase {
    const char *encoded;
    std::array<uint8_t, 3> plain;
    size_t plainSize;
  };
  const DecodeCase cases[] = {
    {"",       {{0, 0, 0}},             0},
    {"Zg==",   {{'f', 0, 0}},           1},
    {"Zm8=",   {{'f', 'o', 0}},         2},
    {"Zm9v",   {{'f', 'o', 'o'}},       3},
    {"=",      {{0, 0, 0}},             0},
    {"====",   {{0, 0, 0}},             0},
    {"A===",   {{0, 0, 0}},             0},
    {"AA=A",   {{0, 0, 0}},             1},
  };

  for (const DecodeCase &test : cases) {
    size_t capacity = base64GetDecodeLength(test.encoded);
    std::vector<unsigned char> storage(capacity + 2, 0xa5);
    uint8_t *out = storage.data() + 1;

    size_t decodedSize = base64Decode(out, test.encoded);

    EXPECT_EQ(decodedSize, test.plainSize);
    EXPECT_EQ(decodedSize + 1, capacity);
    EXPECT_EQ(memcmp(out, test.plain.data(), decodedSize), 0);
    EXPECT_EQ(out[decodedSize], 0);
    EXPECT_EQ(storage.front(), 0xa5);
    EXPECT_EQ(storage.back(), 0xa5);
  }
}

TEST(base64, encode_length_rejects_unrepresentable_capacity)
{
  const size_t sizeMax = (std::numeric_limits<size_t>::max)();
  const size_t maxGroups = (sizeMax - 1)/4;
  const size_t maxRepresentableInput = maxGroups*3;

  EXPECT_EQ(base64getEncodeLength(maxRepresentableInput), maxGroups*4 + 1);
  EXPECT_EQ(base64getEncodeLength(maxRepresentableInput + 1), 0u);
  EXPECT_EQ(base64getEncodeLength(sizeMax), 0u);
}

} // namespace
