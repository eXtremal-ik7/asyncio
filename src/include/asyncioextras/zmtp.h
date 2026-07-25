#include "asyncio/asyncio.h"
#include "zmtpProto.h"

typedef struct zmtpSocket zmtpSocket;

enum zmtpSocketTy {
  zmtpSocketREQ = 0,
  zmtpSocketREP,
  zmtpSocketDEALER,
  zmtpSocketROUTER,
  zmtpSocketPUB,
  zmtpSocketXPUB,
  zmtpSocketSUB,
  zmtpSocketXSUB,
  zmtpSocketPUSH,
  zmtpSocketPULL,
  zmtpSocketPAIR
};

enum zmtpUserMsgTy {
  zmtpUnknown = -1,
  zmtpCommand = 0,
  zmtpMessage,
  zmtpMessagePart,
};

typedef void zmtpAcceptCb(AsyncOpStatus, zmtpSocket*, void*);
typedef void zmtpConnectCb(AsyncOpStatus, zmtpSocket*, void*);
typedef void zmtpRecvCb(AsyncOpStatus, zmtpSocket*, zmtpUserMsgTy, zmtpStream*, void*);
typedef void zmtpSendCb(AsyncOpStatus, zmtpSocket*, void*);

zmtpSocket *zmtpSocketNew(asyncBase *base, aioObject *socket, zmtpSocketTy type);
void zmtpSocketDelete(zmtpSocket *socket);

void aioZmtpAccept(zmtpSocket *socket, AsyncFlags flags, uint64_t timeout, zmtpAcceptCb callback, void *arg);
void aioZmtpConnect(zmtpSocket *socket, const HostAddress *address, AsyncFlags flags, uint64_t timeout, zmtpConnectCb callback, void *arg);
ssize_t aioZmtpRecv(zmtpSocket *socket, zmtpStream &msg, size_t limit, AsyncFlags flags, uint64_t timeout, zmtpRecvCb callback, void *arg);
ssize_t aioZmtpSend(zmtpSocket *socket,
                    void *data,
                    size_t size,
                    zmtpUserMsgTy type,
                    AsyncFlags flags,
                    uint64_t timeout,
                    zmtpSendCb callback,
                    void *arg);

int ioZmtpAccept(zmtpSocket *socket, AsyncFlags flags, uint64_t timeout);
int ioZmtpConnect(zmtpSocket *socket, const HostAddress *address, AsyncFlags flags, uint64_t timeout);
ssize_t ioZmtpRecv(zmtpSocket *socket, zmtpStream &msg, size_t limit, AsyncFlags flags, uint64_t timeout, zmtpUserMsgTy *type);
ssize_t ioZmtpSend(zmtpSocket *socket, void *data, size_t size, zmtpUserMsgTy type, AsyncFlags flags, uint64_t timeout);
