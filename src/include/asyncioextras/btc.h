#include "asyncio/asyncio.h"

typedef struct BTCSocket BTCSocket;
class xmstream;

enum btcStatusTy {
  // btc status
  btcInvalidMagic = aosLast,
  btcInvalidCommand,
  btcInvalidChecksum
};

static inline AsyncOpStatus btcMakeStatus(btcStatusTy status)
{
  return static_cast<AsyncOpStatus>(status);
}

typedef void btcRecvCb(AsyncOpStatus, BTCSocket*, char*, xmstream*, void*);
typedef void btcSendCb(AsyncOpStatus, BTCSocket*, void*);

aioObjectRoot *btcSocketHandle(BTCSocket *socket);
BTCSocket *btcSocketNew(asyncBase *base, aioObject *socket);
void btcSocketDelete(BTCSocket *socket);

aioObject *btcGetPlainSocket(BTCSocket *socket);
void btcSocketSetMagic(BTCSocket *socket, uint32_t magic);

ssize_t aioBtcRecv(BTCSocket *socket,
                   char command[12],
                   xmstream &stream,
                   size_t sizeLimit,
                   AsyncFlags flags,
                   uint64_t timeout,
                   btcRecvCb callback,
                   void *arg);
ssize_t aioBtcSend(BTCSocket *socket,
                   const char *command,
                   void *data,
                   size_t size,
                   AsyncFlags flags,
                   uint64_t timeout,
                   btcSendCb callback,
                   void *arg);

ssize_t ioBtcRecv(BTCSocket *socket, char command[12], xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout);
ssize_t ioBtcSend(BTCSocket *socket, const char *command, void *data, size_t size, AsyncFlags flags, uint64_t timeout);
