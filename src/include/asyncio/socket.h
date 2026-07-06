#ifndef __ASYNCIO_SOCKET_H_
#define __ASYNCIO_SOCKET_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/asyncioTypes.h"
#include <stdint.h>

#if defined(OS_WINDOWS)
#define SOCKET_SHUTDOWN_READ SD_RECEIVE
#define SOCKET_SHUTDOWN_WRITE SD_SEND
#define SOCKET_SHUTDOWN_READWRITE SD_BOTH
#else
#define SOCKET_SHUTDOWN_READ SHUT_RD
#define SOCKET_SHUTDOWN_WRITE SHUT_WR
#define SOCKET_SHUTDOWN_READWRITE SHUT_RDWR
#endif

socketTy socketCreate(int af, int type, int protocol, int isAsync);
void socketClose(socketTy hSocket);
int socketBind(socketTy hSocket, const HostAddress *address);
int socketListen(socketTy hSocket);
int socketShutdown(socketTy hSocket, int how);
void socketReuseAddr(socketTy hSocket);

int socketSyncRead(socketTy hSocket, void *buffer, size_t size, int waitAll, size_t *bytesTransferred);
int socketSyncWrite(socketTy hSocket, const void *buffer, size_t size, int waitAll, size_t *bytesTransferred);

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_SOCKET_H_
