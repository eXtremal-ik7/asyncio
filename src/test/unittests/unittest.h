#include <asyncio/asyncio.h>
#include <gtest/gtest.h>
#include "macro.h"

constexpr unsigned gPort = 65333;
extern asyncBase *gBase;

__NO_PADDING_BEGIN
struct TestContext {
  aioObject *serverSocket;
  aioObject *clientSocket;
  aioObject *pipeRead;
  aioObject *pipeWrite;
  uint8_t clientBuffer[65536];
  uint8_t serverBuffer[128];
  asyncBase *base;
  int serverState;
  int clientState;
  bool success;
  TestContext(asyncBase *baseArg) : base(baseArg), serverState(0), clientState(0), success(false) {}
};

struct reqStruct {
  uint64_t a;
  uint64_t b;
};

struct repStruct {
  uint64_t c;
};

struct ErrorWakeupContext {
  asyncBase *base;
  AsyncOpStatus status = aosPending;
  bool callbackFired = false;

  explicit ErrorWakeupContext(asyncBase *baseArg) : base(baseArg) {}
};

__NO_PADDING_END


aioObject *startTCPServer(asyncBase *base, aioAcceptCb callback, void *arg, uint16_t port);
aioObject *startUDPServer(asyncBase *base, aioReadMsgCb callback, void *arg, void *buffer, size_t size, uint16_t port);
aioObject *initializeTCPClient(asyncBase *base, aioConnectCb callback, void *arg, uint16_t port);
aioObject *initializeUDPClient(asyncBase *base);
void armDeathTestWatchdog(unsigned seconds);
