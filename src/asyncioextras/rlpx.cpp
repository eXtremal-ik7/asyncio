#include "asyncioextras/rlpx.h"
#include "asyncio/api.h"
#include "macro.h"
#include <stdlib.h>

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;
static ConcurrentQueue objectPool;

enum rlpxOpTy {
  rlpxOpAccept = OPCODE_READ,
  rlpxOpRecv,
  rlpxOpConnect = OPCODE_WRITE,
  rlpxOpSend
};

enum RlpxOperationState {
  StInitialize = 0,

  // Connect
  StConnectWriteAuth,

  stFinished
};

__NO_PADDING_BEGIN
struct rlpxSocket {
  aioObjectRoot root;
  aioObject *plainSocket;
};

struct RlpxOperation {
  asyncOpRoot root;
  HostAddress address;
  RlpxOperationState state;
};
__NO_PADDING_END

static void resumeConnectCb(AsyncOpStatus status, aioObject*, void *arg)
{
  resumeParent(static_cast<asyncOpRoot*>(arg), status);
}

static int cancel(asyncOpRoot *opptr)
{
  rlpxSocket *socket = reinterpret_cast<rlpxSocket*>(opptr->object);
  cancelIo(reinterpret_cast<aioObjectRoot*>(socket->plainSocket));
  return 0;
}

// rlpxAcceptCb and rlpxConnectCb are the same function type, so accept and
// connect operations share one finish thunk
static void socketOpFinish(asyncOpRoot *opptr)
{
  reinterpret_cast<rlpxConnectCb*>(opptr->callback)(opGetStatus(opptr), reinterpret_cast<rlpxSocket*>(opptr->object), opptr->arg);
}

// Operations have no resources of their own, so no release procedure is needed
static RlpxOperation *initOp(aioExecuteProc *start,
                             aioFinishProc *finish,
                             rlpxSocket *socket,
                             AsyncFlags flags,
                             uint64_t timeout,
                             void *callback,
                             void *arg,
                             int opCode)
{
  RlpxOperation *op = 0;
  asyncOpAlloc(socket->root.header.base, sizeof(RlpxOperation), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op);
  initAsyncOpRoot(&op->root, start, cancel, finish, 0, &socket->root, callback, arg, flags, opCode, timeout);
  return op;
}

static void rlpxSocketDestructor(aioObjectRoot *object)
{
  deleteAioObject(reinterpret_cast<rlpxSocket*>(object)->plainSocket);
  objectFree(&objectPool, object, sizeof(rlpxSocket));
}

static AsyncOpStatus startRlpxConnect(asyncOpRoot *opptr)
{
  RlpxOperation *op = reinterpret_cast<RlpxOperation*>(opptr);
  rlpxSocket *socket = reinterpret_cast<rlpxSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  while (!childOp) {
    switch (op->state) {
      case StInitialize : {
        op->state = StConnectWriteAuth;
        aioConnect(socket->plainSocket, &op->address, 0, resumeConnectCb, op);
        return aosPending;
      }
      case StConnectWriteAuth : {
        // auth = auth-size || ecies.encrypt([sig, initiator-pubk, initiator-nonce, auth-vsn, ...])
      }

      default:
        return aosUnknownError;
    }
  }

  combinerPushOperation(childOp);
  return aosPending;
}

rlpxSocket *rlpxSocketNew(asyncBase *base, aioObject *plainSocket)
{
  if (!plainSocket)
    return nullptr;
  rlpxSocket *socket = static_cast<rlpxSocket*>(
    objectAlloc(&objectPool, sizeof(rlpxSocket), 16));
  if (!socket)
    return nullptr;
  initObjectRoot(&socket->root, base, ioObjectUserDefined, rlpxSocketDestructor);
  socket->plainSocket = plainSocket;
  return socket;
}

void rlpxSocketDelete(rlpxSocket *socket)
{
  objectDelete(&socket->root);
}

aioObjectRoot *rlpxSocketHandle(rlpxSocket *socket)
{
  return &socket->root;
}

aioObject *rlpxGetPlainSocket(rlpxSocket *socket)
{
  return socket->plainSocket;
}

// RLPx is currently an unfinished API stub, not a supported transport path.
// The operation entry points below only reserve the intended API/state-machine
// shape; missing submission and wire processing are deliberate until the
// protocol implementation is added.
static AsyncOpStatus startRlpxAccept(asyncOpRoot *opptr)
{
  __UNUSED(opptr);
  return aosUnknownError;
}

void aioRlpxAccept(rlpxSocket *socket, AsyncFlags flags, uint64_t timeout, rlpxAcceptCb callback, void *arg)
{
  RlpxOperation *op =
    initOp(startRlpxAccept, socketOpFinish, socket, flags, timeout, reinterpret_cast<void*>(callback), arg, rlpxOpAccept);
  combinerPushOperation(&op->root);
}

void aioRlpxConnect(rlpxSocket *socket, HostAddress address, AsyncFlags flags, uint64_t timeout, rlpxConnectCb callback, void *arg)
{
  RlpxOperation *op = initOp(startRlpxConnect, socketOpFinish, socket, flags, timeout, reinterpret_cast<void*>(callback), arg, rlpxOpConnect);
  op->address = address;

}

ssize_t aioRlpxRecv(rlpxSocket *socket, xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout, rlpxRecvCb callback, void *arg)
{
  __UNUSED(socket);
  __UNUSED(stream);
  __UNUSED(sizeLimit);
  __UNUSED(flags);
  __UNUSED(timeout);
  __UNUSED(callback);
  __UNUSED(arg);
  return 0;
}

ssize_t aioRlpxSend(rlpxSocket *socket, void *data, size_t size, AsyncFlags flags, uint64_t timeout, rlpxSendCb callback, void *arg)
{
  __UNUSED(socket);
  __UNUSED(data);
  __UNUSED(size);
  __UNUSED(flags);
  __UNUSED(timeout);
  __UNUSED(callback);
  __UNUSED(arg);
  return 0;
}

int ioRlpxAccept(rlpxSocket *socket, AsyncFlags flags, uint64_t timeout)
{
  __UNUSED(socket);
  __UNUSED(flags);
  __UNUSED(timeout);
  return 0;
}

int ioRlpxConnect(rlpxSocket *socket, HostAddress address, AsyncFlags flags, uint64_t timeout)
{
  __UNUSED(socket);
  __UNUSED(address);
  __UNUSED(flags);
  __UNUSED(timeout);
  return 0;
}

ssize_t ioRlpxRecv(rlpxSocket *socket, xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout)
{
  __UNUSED(socket);
  __UNUSED(stream);
  __UNUSED(sizeLimit);
  __UNUSED(flags);
  __UNUSED(timeout);
  return 0;
}

ssize_t ioRlpxSend(rlpxSocket *socket, void *data, size_t size, AsyncFlags flags, uint64_t timeout)
{
  __UNUSED(socket);
  __UNUSED(data);
  __UNUSED(size);
  __UNUSED(flags);
  __UNUSED(timeout);
  return 0;
}
