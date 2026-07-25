#ifndef __ASYNCIO_IO_INTERNAL_H_
#define __ASYNCIO_IO_INTERNAL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncioImpl.h"
#include <stdlib.h>
#ifndef OS_WINDOWS
#include <errno.h>
#include <signal.h>
#endif

#ifdef MSG_NOSIGNAL
#define ASYNCIO_MSG_NOSIGNAL MSG_NOSIGNAL
#else
#define ASYNCIO_MSG_NOSIGNAL 0
#endif

#define DEFAULT_SOCKET_BUFFER_SIZE 16384

typedef enum IoActionTy {
  actAccept = OPCODE_READ,
  actRead = OPCODE_READ + 1,
  actReadMsg = OPCODE_READ + 2,
  actConnect = OPCODE_WRITE | OPCODE_INIT,
  actWrite = OPCODE_WRITE + 1,
  actWriteMsg = OPCODE_WRITE + 2
} IoActionTy;

typedef struct asyncOp asyncOp;

struct ioBuffer {
  void *ptr;
  size_t totalSize;
  size_t dataSize;
  size_t offset;
};

struct aioObject {
  aioObjectRoot root;
  union {
    iodevTy hDevice;
    socketTy hSocket;
  };
  struct ioBuffer buffer;
  int needSigpipeGuard;
};

struct asyncOp {
  asyncOpRoot root;
  int state;
  void *buffer;
  size_t transactionSize;
  size_t bytesTransferred;
  socketTy acceptSocket;
  HostAddress host;
  void *internalBuffer;
  size_t internalBufferSize;
};

static inline void ioBufferEnsureCapacity(struct ioBuffer *buffer, size_t size)
{
  if (size > buffer->totalSize) {
    buffer->ptr = realloc(buffer->ptr, size);
    buffer->totalSize = size;
    poolCacheHandoff(buffer->ptr);
  }
}

#ifndef OS_WINDOWS
extern int sigpipeIgnored;

struct SigpipeGuard {
  sigset_t savedMask;
  int wasPending;
};

void sigpipeGuardEnter(struct SigpipeGuard *guard);
void sigpipeGuardLeave(struct SigpipeGuard *guard, int consumeSigpipe);

static inline AsyncOpStatus socketStatusFromErrno(int error)
{
  switch (error) {
    case EINTR:
    case EAGAIN: return aosPending;
    case EPIPE:
    case ENOTCONN: return aosNotConnected;
    case ECONNREFUSED:
    case ECONNRESET: return aosDisconnected;
    default: return aosUnknownError;
  }
}
#endif

static inline void *opEnsureTimerCell(asyncOpRoot *op)
{
  if (!op->timerId) {
    asyncBase *base = op->object->header.base;
    base->methodImpl.initializeTimer(base, op);
    poolCacheHandoff(op->timerId);
  }
  return op->timerId;
}

static inline void asyncOpEnsureInternalBuffer(void **buffer, size_t *bufferSize, size_t required)
{
  if (*bufferSize < required) {
    *buffer = realloc(*buffer, required);
    *bufferSize = required;
    poolCacheHandoff(*buffer);
  }
}

int copyFromBuffer(void *dst, size_t *offset, struct ioBuffer *src, size_t size);

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_IO_INTERNAL_H_
