#include "asyncio/asyncio.h"

typedef struct rlpxSocket rlpxSocket;
class xmstream;

enum rplxStatusTy {
  // rlpx status
};

static inline AsyncOpStatus rlpxMakeStatus(rplxStatusTy status)
{
  return static_cast<AsyncOpStatus>(status);
}

typedef void rlpxAcceptCb(AsyncOpStatus, rlpxSocket*, void*);
typedef void rlpxConnectCb(AsyncOpStatus, rlpxSocket*, void*);
typedef void rlpxRecvCb(AsyncOpStatus, rlpxSocket*, char*, xmstream*, void*);
typedef void rlpxSendCb(AsyncOpStatus, rlpxSocket*, void*);

rlpxSocket *rlpxSocketNew(asyncBase *base, aioObject *plainSocket);
void rlpxSocketDelete(rlpxSocket *socket);
aioObjectRoot *rlpxSocketHandle(rlpxSocket *socket);
aioObject *rlpxGetPlainSocket(rlpxSocket *socket);

void aioRlpxAccept(rlpxSocket *socket, AsyncFlags flags, uint64_t timeout, rlpxAcceptCb callback, void *arg);
void aioRlpxConnect(rlpxSocket *socket, HostAddress address, AsyncFlags flags, uint64_t timeout, rlpxConnectCb callback, void *arg);
ssize_t aioRlpxRecv(rlpxSocket *socket, xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout, rlpxRecvCb callback, void *arg);
ssize_t aioRlpxSend(rlpxSocket *socket, void *data, size_t size, AsyncFlags flags, uint64_t timeout, rlpxSendCb callback, void *arg);

int ioRlpxAccept(rlpxSocket *socket, AsyncFlags flags, uint64_t timeout);
int ioRlpxConnect(rlpxSocket *socket, HostAddress address, AsyncFlags flags, uint64_t timeout);
ssize_t ioRlpxRecv(rlpxSocket *socket, xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout);
ssize_t ioRlpxSend(rlpxSocket *socket, void *data, size_t size, AsyncFlags flags, uint64_t timeout);

#pragma pack(push, 1)
struct RlpxAuthBody {
  uint8_t Signature[65];
  uint8_t HePubKey[32];
  uint8_t PubKey[64];
  uint8_t InitiatorNonce[32];
  uint8_t Zero;
};
#pragma pack(pop)
