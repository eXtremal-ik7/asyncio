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
  w.writeReadyCmd("PUSH", "peer");

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
  w.writeCommandName("READY");
  w.write<uint8_t>(11);
  w.write("SOCKET-TYPE", 11);
  w.writebe<uint32_t>(4);
  w.write("PUSH", 4);

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
  w.writeCommandName("READY");
  w.write<uint8_t>(8);
  w.write("Identity", 8);
  w.writebe<uint32_t>(4);
  w.write("peer", 4);

  zmtpStream rd(w.data(), w.offsetOf());
  RawData socketType;
  RawData identity;
  EXPECT_FALSE(rd.readReadyCmd(&socketType, &identity));
}
