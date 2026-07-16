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

// Connect is an object's one-shot initialization: a second connect submitted
// while the first is still in flight must be rejected immediately. Shared
// recorder for that contract across the transports; instantiating the
// callback templates with the socket type produces the exact C callback
// signature of each connect flavor (aioObject, SSLSocket, zmtpSocket).
struct DoubleConnectRecorder {
  asyncBase *base;
  AsyncOpStatus firstStatus;
  AsyncOpStatus secondStatus;
  int events;
  int firstOrder;
  int secondOrder;
  DoubleConnectRecorder(asyncBase *baseArg) :
    base(baseArg), firstStatus(aosUnknown), secondStatus(aosUnknown),
    events(0), firstOrder(-1), secondOrder(-1) {}
};

template<typename SocketTy>
void doubleConnectFirstCb(AsyncOpStatus status, SocketTy*, void *arg)
{
  DoubleConnectRecorder *ctx = static_cast<DoubleConnectRecorder*>(arg);
  ctx->firstStatus = status;
  ctx->firstOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

template<typename SocketTy>
void doubleConnectSecondCb(AsyncOpStatus status, SocketTy*, void *arg)
{
  DoubleConnectRecorder *ctx = static_cast<DoubleConnectRecorder*>(arg);
  ctx->secondStatus = status;
  ctx->secondOrder = ctx->events++;
  if (ctx->events == 2)
    postQuitOperation(ctx->base);
}

aioObject *startTCPServer(asyncBase *base, aioAcceptCb callback, void *arg, uint16_t port);
aioObject *startUDPServer(asyncBase *base, aioReadMsgCb callback, void *arg, void *buffer, size_t size, uint16_t port);
aioObject *initializeTCPClient(asyncBase *base, aioConnectCb callback, void *arg, uint16_t port);
aioObject *initializeUDPClient(asyncBase *base);
void armDeathTestWatchdog(unsigned seconds);
