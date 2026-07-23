#include "unittest.h"

#include "p2putils/xmstream.h"

#include <cstddef>
#include <limits>

namespace {

void expectStreamStillUsable(xmstream &stream, void *originalData,
                             size_t originalSize, size_t originalCapacity)
{
  EXPECT_EQ(stream.data(), originalData);
  EXPECT_EQ(stream.offsetOf(), originalSize);
  EXPECT_EQ(stream.sizeOf(), originalSize);
  EXPECT_EQ(stream.capacity(), originalCapacity);

  uint8_t *next = static_cast<uint8_t*>(stream.reserve(1));
  ASSERT_NE(next, nullptr);
  *next = 0xa5;
  EXPECT_EQ(stream.sizeOf(), originalSize + 1);
}

TEST(xmstream, reserve_rejects_size_max_without_changing_stream)
{
  xmstream stream;
  uint8_t *first = static_cast<uint8_t*>(stream.reserve(1));
  ASSERT_NE(first, nullptr);
  *first = 0x5a;

  void *originalData = stream.data();
  size_t originalSize = stream.sizeOf();
  size_t originalCapacity = stream.capacity();

  EXPECT_EQ(stream.reserve((std::numeric_limits<size_t>::max)()), nullptr);
  EXPECT_EQ(*static_cast<uint8_t*>(stream.data()), 0x5a);
  expectStreamStillUsable(stream, originalData, originalSize, originalCapacity);
}

TEST(xmstream, reserve_rejects_unrepresentable_size_larger_than_one_tib)
{
  if (sizeof(size_t) <= sizeof(uint32_t))
    return;

  xmstream stream;
  void *originalData = stream.data();
  size_t originalCapacity = stream.capacity();
  const size_t hugeSize =
    static_cast<size_t>((std::numeric_limits<ptrdiff_t>::max)()) + 1;

  EXPECT_GT(hugeSize, static_cast<size_t>(UINT64_C(1) << 40));
  EXPECT_EQ(stream.reserve(hugeSize), nullptr);
  expectStreamStillUsable(stream, originalData, 0, originalCapacity);
}

} // namespace
