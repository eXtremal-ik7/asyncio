#include "asyncioextras/zmtpProto.h"
#include "gtest/gtest.h"
#include <string.h>

// Streams here carry the READY command payload exactly as it reaches
// readReadyCmd: the command name length byte first, frame flags and
// length already stripped.

TEST(zmtpProto, ready_with_socket_type)
{
  zmtpStream w;
  w.reset();
  ASSERT_TRUE(w.writeReadyCmd("PUSH", "peer"));

  zmtpStream rd(w.data(), w.offsetOf());
  RawData socketType;
  RawData identity;
  ASSERT_TRUE(rd.readReadyCmd(&socketType, &identity));
  ASSERT_EQ(socketType.size, 4u);
  EXPECT_EQ(memcmp(socketType.data, "PUSH", 4), 0);
  ASSERT_EQ(identity.size, 4u);
  EXPECT_EQ(memcmp(identity.data, "peer", 4), 0);
}

// metadata property names are case-insensitive per ZMTP 3.0
TEST(zmtpProto, ready_metadata_keys_case_insensitive)
{
  zmtpStream w;
  w.reset();
  ASSERT_TRUE(w.writeCommandName("READY"));
  ASSERT_TRUE(w.write<uint8_t>(11));
  ASSERT_TRUE(w.write("SOCKET-TYPE", 11));
  ASSERT_TRUE(w.writebe<uint32_t>(4));
  ASSERT_TRUE(w.write("PUSH", 4));

  zmtpStream rd(w.data(), w.offsetOf());
  RawData socketType;
  RawData identity;
  ASSERT_TRUE(rd.readReadyCmd(&socketType, &identity));
  ASSERT_EQ(socketType.size, 4u);
  EXPECT_EQ(memcmp(socketType.data, "PUSH", 4), 0);
}

TEST(zmtpProto, ready_requires_socket_type)
{
  zmtpStream w;
  w.reset();
  ASSERT_TRUE(w.writeCommandName("READY"));
  ASSERT_TRUE(w.write<uint8_t>(8));
  ASSERT_TRUE(w.write("Identity", 8));
  ASSERT_TRUE(w.writebe<uint32_t>(4));
  ASSERT_TRUE(w.write("peer", 4));

  zmtpStream rd(w.data(), w.offsetOf());
  RawData socketType;
  RawData identity;
  EXPECT_FALSE(rd.readReadyCmd(&socketType, &identity));
}
