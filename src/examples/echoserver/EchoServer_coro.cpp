#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include "asyncio/socket.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct listenerContext {
  asyncBase *base;
  aioObject *socket;
};

struct readerContext {
  asyncBase *base;
  aioObject *socket;
};

void printhex(uint8_t *data, size_t size)
{
  for (size_t i = 0; i < size; i++) {
    fprintf(stderr, "%02X", (unsigned)data[i]);
  }
  fprintf(stderr, "\n");   
}

void readerProc(void *arg)
{
  readerContext *reader = (readerContext*)arg;
  uint8_t echoBuffer[1024];
  while (true) {
    ssize_t bytesRead = ioRead(reader->socket, echoBuffer, sizeof(echoBuffer), afNone, 0);
    if (bytesRead > 0) {
      ioWrite(reader->socket, echoBuffer, bytesRead, afNone, 0);
    } else {
      fprintf(stderr, " * asyncRead error, exiting..\n");
      deleteAioObject(reader->socket);
      break;
    }
  }
}

void listenerProc(void *arg)
{
  listenerContext *ctx = (listenerContext*)arg;
  while (true) {
    socketTy acceptSocket = ioAccept(ctx->socket, 0);
    if (acceptSocket > 0) {
      fprintf(stderr, "new connection accepted\n");
      readerContext *reader = new readerContext;
      reader->base = ctx->base;
      reader->socket = newSocketIo(ctx->base, acceptSocket);      
      coroutineTy *echoProc = coroutineNew(readerProc, reader, 0x10000);
      coroutineCall(echoProc);      
    } else {
      fprintf(stderr, "accept error %i\n", -static_cast<int>(acceptSocket));
    }
  }
}

int main(int argc, char **argv)
{
  if (argc != 3) {
    fprintf(stderr, "usage: %s <method> <port>\n", argv[0]);
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
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = static_cast<uint16_t>(atoi(argv[2]));
  
  initializeAsyncIo(aiNone);
  socketTy hSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  socketReuseAddr(hSocket);
  if (socketBind(hSocket, &address) != 0) {
    fprintf(stderr, "cannot bind\n");
    exit(1);
  }

  if (socketListen(hSocket) != 0) {
    fprintf(stderr, "listen error\n");
    exit(1);
  }

  asyncBase *base = createAsyncBase(method, 1);
  aioObject *socketOp = newSocketIo(base, hSocket);
  
  listenerContext ctx;
  ctx.base = base;
  ctx.socket = socketOp;

  coroutineCall(coroutineNew(listenerProc, &ctx, 0x10000));
  asyncLoop(base);
  return 0;
}
