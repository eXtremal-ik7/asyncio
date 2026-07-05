#include "asyncio/socket.h"

void initializeSocketSubsystem()
{
  WSADATA wsadata;
  WSAStartup(MAKEWORD(2, 2), &wsadata);
}


socketTy socketCreate(int af, int type, int protocol, int isAsync)
{
  SOCKET hSocket = WSASocket(af, type, protocol, NULL, 0, isAsync ? WSA_FLAG_OVERLAPPED : 0);
  if (isAsync) {
    u_long arg = 1;
    ioctlsocket(hSocket, FIONBIO, &arg);
  }

  return hSocket;
}

void socketClose(socketTy hSocket)
{
  closesocket(hSocket);
}

int socketBind(socketTy hSocket, const HostAddress *address)
{
  struct sockaddr_in localAddr;
  localAddr.sin_family = address->family;
  localAddr.sin_addr.s_addr = address->ipv4;
  localAddr.sin_port = htons(address->port);
  return bind(hSocket, (struct sockaddr*)&localAddr, sizeof(localAddr));
}


int socketListen(socketTy hSocket)
{
  return listen(hSocket, SOMAXCONN);
}


int socketShutdown(socketTy hSocket, int how)
{
  return shutdown(hSocket, how);
}


void socketReuseAddr(socketTy hSocket)
{
  char optval = 1;
  setsockopt(hSocket, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(int));
}


uint32_t addrfromAscii(const char *cp)
{
  uint32_t res = inet_addr(cp);
  return (res != INADDR_NONE) ? res : 0;
}

int socketSyncRead(socketTy hSocket, void *buffer, size_t size, int waitAll, size_t *bytesTransferred)
{
  DWORD bytesNum = 0;
  WSABUF wsabuf;
  if (!waitAll) {
    // TODO: correct processing >4Gb data blocks
    wsabuf.buf = buffer;
    wsabuf.len = (ULONG)size;
    DWORD flags = 0;
    if (WSARecv(hSocket, &wsabuf, 1, &bytesNum, &flags, 0, 0) == 0 && bytesNum != 0) {
      *bytesTransferred = bytesNum;
      return 1;
    } else {
      return 0;
    }
  } else {
    size_t transferred = 0;
    DWORD flags;
    do {
      flags = 0;
      transferred += (size_t)bytesNum;
      wsabuf.buf = (uint8_t*)buffer + transferred;
      // TODO: correct processing >4Gb data blocks
      wsabuf.len = (ULONG)(size - transferred);
    } while (transferred != size && WSARecv(hSocket, &wsabuf, 1, &bytesNum, &flags, 0, 0) == 0);
    *bytesTransferred = transferred;
    return transferred == size;
  }
}

int socketSyncWrite(socketTy hSocket, const void *buffer, size_t size, int waitAll, size_t *bytesTransferred)
{
  DWORD bytesNum = 0;
  WSABUF wsabuf;
  if (!waitAll) {
    // TODO: correct processing >4Gb data blocks
    wsabuf.buf = (char*)buffer;
    wsabuf.len = (ULONG)size;
    if (WSASend(hSocket, &wsabuf, 1, &bytesNum, 0, 0, 0) == 0 && bytesNum != 0) {
      *bytesTransferred = bytesNum;
      return 1;
    }
    else {
      return 0;
    }
  } else {
    size_t transferred = 0;
    do {
      transferred += (size_t)bytesNum;
      wsabuf.buf = (uint8_t*)buffer + transferred;
      // TODO: correct processing >4Gb data blocks
      wsabuf.len = (ULONG)(size - transferred);
    } while (transferred != size && WSASend(hSocket, &wsabuf, 1, &bytesNum, 0, 0, 0) == 0);
    *bytesTransferred = transferred;
    return transferred == size;
  }
}
