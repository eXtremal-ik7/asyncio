// BTC transport contract: the receive completion callback's command argument
// must name the message that was actually received.

#include "unittest.h"

#include "asyncioextras/btc.h"
#include "p2putils/xmstream.h"

#include <string.h>

#ifndef OS_WINDOWS

#include <fcntl.h>
#include <sys/socket.h>

namespace {

struct BtcRecvProbe {
  asyncBase *base;
  xmstream stream;
  char userCommand[12];
  char callbackCommand[12];
  AsyncOpStatus recvStatus = aosPending;

  explicit BtcRecvProbe(asyncBase *baseArg) : base(baseArg) {
    memset(userCommand, 0xAA, sizeof(userCommand));
    memset(callbackCommand, 0xBB, sizeof(callbackCommand));
  }
};

void btcRecvNameCb(AsyncOpStatus status, BTCSocket*, char *command, xmstream*, void *arg)
{
  BtcRecvProbe *probe = static_cast<BtcRecvProbe*>(arg);
  probe->recvStatus = status;
  if (command)
    memcpy(probe->callbackCommand, command, sizeof(probe->callbackCommand));
  postQuitOperation(probe->base);
}

void btcSendNameCb(AsyncOpStatus status, BTCSocket*, void*)
{
  EXPECT_EQ(status, aosSuccess);
}

// newSocketIo expects what socketCreate produces: a non-blocking descriptor
socketTy preparePairEnd(int fd)
{
  fcntl(fd, F_SETFL, O_NONBLOCK | fcntl(fd, F_GETFL));
  return fd;
}

} // namespace

TEST(btc, recv_callback_reports_received_command_name)
{
  int fds[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
  BTCSocket *sender = btcSocketNew(gBase, newSocketIo(gBase, preparePairEnd(fds[0])));
  BTCSocket *receiver = btcSocketNew(gBase, newSocketIo(gBase, preparePairEnd(fds[1])));
  ASSERT_NE(sender, nullptr);
  ASSERT_NE(receiver, nullptr);

  BtcRecvProbe probe(gBase);

  // Post the receive first: no bytes have arrived yet, so the completion is
  // deferred and must travel through the operation's finish path.
  aioBtcRecv(receiver, probe.userCommand, probe.stream, 1024, afNone, 5000000, btcRecvNameCb, &probe);

  uint8_t payload[4] = {1, 2, 3, 4};
  aioBtcSend(sender, "ping", payload, sizeof(payload), afNone, 5000000, btcSendNameCb, nullptr);

  asyncLoop(gBase);

  const char expected[12] = {'p', 'i', 'n', 'g', 0, 0, 0, 0, 0, 0, 0, 0};
  ASSERT_EQ(probe.recvStatus, aosSuccess);
  ASSERT_EQ(probe.stream.sizeOf(), sizeof(payload));
  EXPECT_EQ(memcmp(probe.stream.data(), payload, sizeof(payload)), 0);
  // The user-supplied array receives the wire command name...
  EXPECT_EQ(memcmp(probe.userCommand, expected, sizeof(expected)), 0);
  // ...and the callback's own command argument must carry the same name.
  EXPECT_EQ(memcmp(probe.callbackCommand, expected, sizeof(expected)), 0)
      << "the recv callback's command argument does not name the received message";

  btcSocketDelete(sender);
  btcSocketDelete(receiver);
}

#endif // !OS_WINDOWS
