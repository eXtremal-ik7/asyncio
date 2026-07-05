#include "asyncio/asyncio.h"
#include "asyncio/socket.h"
#include "p2putils/uriParse.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#if !defined(OS_WINDOWS)
#include <netdb.h>
#endif


const size_t clientBufferSize = 1024;

__NO_PADDING_BEGIN
struct ClientData {
  asyncBase *base;
  aioObject *socket;
  bool isConnected;
  uint8_t buffer[clientBufferSize];
};
__NO_PADDING_END

void writeCb(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  __UNUSED(transferred);
  ClientData *data = static_cast<ClientData*>(arg);
  if (status != aosSuccess) {
    printf("write error\n");
    postQuitOperation(data->base);
  }
}

void readCb(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{
  __UNUSED(object);
  ClientData *data = static_cast<ClientData*>(arg);
  if (status == aosSuccess) {
    for (uint8_t *p = data->buffer, *pe = p+transferred; p < pe; p++)
      printf("%02X", static_cast<unsigned>(*p));
    printf("\n");
  } else if (status == aosDisconnected) {
    fprintf(stderr, "connection lost!\n");
    postQuitOperation(data->base);
  } else {
    fprintf(stderr, "receive error!\n");
  }
}


void pingTimerCb(aioUserEvent *event, void *arg)
{
  __UNUSED(event);
  ClientData *data = static_cast<ClientData*>(arg);
  if (data->isConnected) {
    char symbol = 32 + rand() % 96;
    printf("%02X:", static_cast<int>(symbol));
    fflush(stdout);
    aioWrite(data->socket, &symbol, 1, afNone, 1000000, writeCb, data);
    aioRead(data->socket, data->buffer, clientBufferSize, afNone, 1000000, readCb, data);
  }
}


void connectCb(AsyncOpStatus status, aioObject *object, void *arg)
{
  __UNUSED(object);
  ClientData *data = static_cast<ClientData*>(arg);
  if (status == aosSuccess) {
    fprintf(stderr, "connected\n");
    data->isConnected = true;
  } else {
    fprintf(stderr, "connection error\n");
    exit(1);
  }
}


int main(int argc, char **argv)
{
  if (argc != 3) {
    fprintf(stderr, "usage: %s <method> <host:port>\n", argv[0]);
    return 1;
  }
  
  AsyncMethod method;
  if (strcmp(argv[1], "default") == 0) {
    method = amOSDefault;
  } else if (strcmp(argv[1], "epoll") == 0) {
    method = amEPoll;
  } else if (strcmp(argv[1], "kqueue") == 0) {
    method = amKQueue;
  } else if (strcmp(argv[1], "iocp") == 0) {
    method = amIOCP;
  } else {
    fprintf(stderr, "ERROR: unknown method %s, default used\n", argv[1]);
    method = amOSDefault;
  }  
  
  HostAddress address;
  initializeAsyncIo(aiNone);
  srand(static_cast<unsigned>(time(nullptr)));

  URI uri;
  if (!uriParseHostPort(argv[2], &uri, 0) || uri.port == 0) {
    fprintf(stderr, "Invalid address %s\nIt must have host:port format\n", argv[2]);
    return 1;
  }

  uint32_t remoteIPv4 = 0;
  if (uri.hostType == URI::HostTypeIPv4) {
    remoteIPv4 = uri.ipv4;
  } else if (uri.hostType == URI::HostTypeDNS) {
    hostent *host = gethostbyname(uri.domain.c_str());
    if (!host || !host->h_addr) {
      fprintf(stderr, " * cannot retrieve address of %s (gethostbyname failed)\n", uri.domain.c_str());
      return 1;
    }
    memcpy(&remoteIPv4, host->h_addr, sizeof(remoteIPv4));
  } else {
    fprintf(stderr, "IPv6 address is not supported by this example\n");
    return 1;
  }

  asyncBase *base = createAsyncBase(method);
  
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  socketTy hSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  if (socketBind(hSocket, &address) != 0) {
    exit(1);
  }

  ClientData data;  
  aioObject *socketOp = newSocketIo(base, hSocket);
  aioUserEvent *stdInputOp = newUserEvent(base, 0, pingTimerCb, &data);

  address.family = AF_INET;
  address.ipv4 = remoteIPv4;
  address.port = static_cast<uint16_t>(uri.port);
  data.base = base;
  data.socket = socketOp;
  data.isConnected = false;    
  aioConnect(socketOp, &address, 3000000, connectCb, &data);
  userEventStartTimer(stdInputOp, 1000000, -1);
  asyncLoop(base);
  return 0;
}
