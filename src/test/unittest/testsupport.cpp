#include "unittest.h"

#include "asyncio/socket.h"

#include <chrono>
#include <thread>

#ifdef OS_COMMONUNIX
#include <unistd.h>
#endif

aioObject *startTCPServer(asyncBase *base, aioAcceptCb callback, void *arg, uint16_t port)
{
  HostAddress address;

  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = port;
  socketTy acceptSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  socketReuseAddr(acceptSocket);
  if (socketBind(acceptSocket, &address) != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (socketBind(acceptSocket, &address) != 0)
      return nullptr;
  }

  if (socketListen(acceptSocket) != 0)
    return nullptr;

  aioObject *object = newSocketIo(base, acceptSocket);
  if (callback)
    aioAccept(object, 333000, callback, arg);
  return object;
}

aioObject *startUDPServer(asyncBase *base, aioReadMsgCb callback, void *arg, void *buffer, size_t size, uint16_t port)
{
  HostAddress address;

  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = port;
  socketTy acceptSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  if (socketBind(acceptSocket, &address) != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (socketBind(acceptSocket, &address) != 0)
      return nullptr;
  }

  aioObject *object = newSocketIo(base, acceptSocket);
  if (callback)
    aioReadMsg(object, buffer, size, afNone, 1000000, callback, arg);
  return object;
}

aioObject *initializeTCPClient(asyncBase *base, aioConnectCb callback, void *arg, uint16_t port)
{
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy connectSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  int bindResult = socketBind(connectSocket, &address);
  EXPECT_EQ(bindResult, 0);
  if (bindResult != 0)
    return nullptr;

  aioObject *object = newSocketIo(base, connectSocket);
  if (callback) {
    address.family = AF_INET;
    address.ipv4 = inet_addr("127.0.0.1");
    address.port = port;
    aioConnect(object, &address, 333000, callback, arg);
  }

  return object;
}

aioObject *initializeUDPClient(asyncBase *base)
{
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy clientSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 1);
  if (socketBind(clientSocket, &address) != 0)
    return nullptr;

  aioObject *object = newSocketIo(base, clientSocket);
  return object;
}
void armDeathTestWatchdog(unsigned seconds)
{
#ifdef OS_WINDOWS
  // no alarm() on Windows; the death-test child is a fresh process (no fork),
  // so a plain watchdog thread is safe there
  std::thread([seconds]() {
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    TerminateProcess(GetCurrentProcess(), 3);
  }).detach();
#else
  alarm(seconds);
#endif
}
