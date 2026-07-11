#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include "asyncio/socket.h"
#include "asyncio/timer.h"
#include <errno.h>
#include <inttypes.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <memory.h>
#include <stdio.h>
#include <stdlib.h>
#include <thread>
#include <vector>

static unsigned gDebug = 0;
static uint16_t gPortBase = 63300;
#if defined(OS_WINDOWS)
// Windows have low performance loopback interface
static uint64_t gTotalPacketNum = 640000ULL;
#elif defined(OS_DARWIN)
static uint64_t gTotalPacketNum = 1600000ULL;
#else
static uint64_t gTotalPacketNum = 4000000ULL;
#endif

static unsigned gGroupSize = 1000;
static unsigned gMessageSize = 16;

// For debugging
static __tls uint64_t threadPacketsNum = 0;

struct Context {
  uint64_t totalPacketNum;
  unsigned groupSize;
  unsigned messageSize;
  uint16_t port;
  Context(uint16_t portArg) : totalPacketNum(gTotalPacketNum), groupSize(gGroupSize), messageSize(gMessageSize), port(portArg) {}
};

enum AIOSenderTy {
  aioSenderBlocking = 0,
  aioSenderAsync,
  aioSenderCoroutine
};

enum AIOReceiverTy {
  aioReceiverBlocking = 0,
  aioReceiverAsync,
  aioReceiverAsyncTimer,
  aioReceiverAsyncRT,
  aioReceiverCoroutine
};

struct ReceiverControl {
  std::atomic<bool> stopping;
  asyncBase *base;

  ReceiverControl() : stopping(false), base(nullptr) {}
};

__NO_PADDING_BEGIN
struct SenderCtx {
  Context *config;
  asyncBase *localBase;
  socketTy clientSocket;
  aioObject *client;
  unsigned counter;
  char buffer[65536];  
};

struct ReceiverCtx {
  Context *config;
  ReceiverControl *control;
  asyncBase *base;
  socketTy serverSocket;
  aioObject *server;
  AIOReceiverTy type;
  
  timeMark beginPt;
  timeMark endPt;  
  bool started;
  uint64_t oldPacketsNum;
  uint64_t packetsNum; 
  std::atomic<uint64_t> publishedPacketsNum;
  char buffer[65536];  
  
  ReceiverCtx()
    : config(nullptr),
      control(nullptr),
      base(nullptr),
      serverSocket(),
      server(nullptr),
      type(aioReceiverBlocking),
      beginPt(),
      endPt(),
      started(false),
      oldPacketsNum(0),
      packetsNum(0),
      publishedPacketsNum(0)
  {}
};
__NO_PADDING_END


static const char *aioSenderName[] = {
  "blocking",
  "async",
  "coroutine"
};

static const char *aioReceiverName[] = {
  "blocking",
  "async",
  "async+timer",
  "async+timer+rt",
  "coroutine"
};

// ======================================================================
// =                                                                    =
// =                         Senders                                    =
// =                                                                    =
// ======================================================================

void *test_sync_sender(void *arg)
{
  char msg[65536];
  memset(msg, 'm', sizeof(msg));
  SenderCtx *senderCtx = static_cast<SenderCtx*>(arg);
  
  sockaddr_in destAddr;
  destAddr.sin_family = AF_INET;  
  destAddr.sin_addr.s_addr = inet_addr("127.0.0.1");
  destAddr.sin_port = htons(senderCtx->config->port);

  for (uint64_t i = 0; i < senderCtx->config->totalPacketNum; i++) {
    if (sendto(senderCtx->clientSocket, msg, senderCtx->config->messageSize, 0, reinterpret_cast<sockaddr*>(&destAddr), sizeof(destAddr)) == -1) {
      fprintf(stderr, "sendto return error %s\n", strerror(errno));
      exit(1);
    }
  }
  
  return nullptr;
}

void test_aio_writecb(AsyncOpStatus status, aioObject *object, size_t transferred, void *arg)
{ 
  __UNUSED(transferred);
  SenderCtx *senderCtx = static_cast<SenderCtx*>(arg);
  if (status != aosSuccess) {
    postQuitOperation(aioGetBase(object));
    return;
  }

  senderCtx->counter++;
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = senderCtx->config->port;
  while (senderCtx->counter < senderCtx->config->totalPacketNum) {
    ssize_t result = aioWriteMsg(object,
                                 &address,
                                 &senderCtx->buffer,
                                 senderCtx->config->messageSize,
                                 afActiveOnce,
                                 0,
                                 test_aio_writecb,
                                 senderCtx);
    if (result > 0)
      senderCtx->counter++;
    else
      return;
  }

  postQuitOperation(aioGetBase(object));
}

void *test_aio_sender(void *arg)
{
  SenderCtx *senderCtx = static_cast<SenderCtx*>(arg);
  asyncBase *localBase = createAsyncBase(amOSDefault, 1);

  senderCtx->localBase = localBase;
  senderCtx->client = newSocketIo(localBase, senderCtx->clientSocket);  
  senderCtx->counter = 0;
  
  // Send loop start
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = senderCtx->config->port;
  aioWriteMsg(senderCtx->client, &address, &senderCtx->buffer, senderCtx->config->messageSize, afNone, 0, test_aio_writecb, senderCtx);
  asyncLoop(localBase);
  return nullptr;
}

void test_coroutine_sender_coro(void *arg)
{
  char msg[65536];
  memset(msg, 'm', sizeof(msg));
  SenderCtx *senderCtx = static_cast<SenderCtx*>(arg);
  
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = senderCtx->config->port; 
  for (uint64_t i = 0; i < senderCtx->config->totalPacketNum; i++)
    ioWriteMsg(senderCtx->client, &address, msg, senderCtx->config->messageSize, afNone, 0);

  postQuitOperation(senderCtx->localBase);
}

void *test_coroutine_sender(void *arg)
{
  asyncBase *localBase = createAsyncBase(amOSDefault, 1); 
  
  SenderCtx *senderCtx = static_cast<SenderCtx*>(arg);
  senderCtx->localBase = localBase;
  senderCtx->client = newSocketIo(localBase, senderCtx->clientSocket);
  coroutineCall(coroutineNew(test_coroutine_sender_coro, senderCtx, 0x40000));
  asyncLoop(localBase);  
  return nullptr;
}

// ======================================================================
// =                                                                    =
// =                         Receivers                                  =
// =                                                                    =
// ======================================================================

static bool receiverStopping(const ReceiverCtx *ctx)
{
  return ctx->control->stopping.load(std::memory_order_acquire);
}

static void receiverAccountPacket(ReceiverCtx *ctx);

// Blocking synchronous receiver
void *test_sync_receiver(void *arg)
{
  char msg[65536];
  ReceiverCtx *receiverCtx = static_cast<ReceiverCtx*>(arg);
  
  while (!receiverStopping(receiverCtx)) {
    sockaddr_in addr;
    socketLenTy len = sizeof(addr);
    int result = recvfrom(receiverCtx->serverSocket,
                          msg,
                          sizeof(msg),
                          0,
                          reinterpret_cast<sockaddr*>(&addr),
                          &len);
    if (receiverStopping(receiverCtx))
      break;
    if (result < 0)
      continue;
    receiverAccountPacket(receiverCtx);
  }

  return nullptr;
}

// Account one received datagram for rate/liveness bookkeeping
static void receiverAccountPacket(ReceiverCtx *ctx)
{
  if (!ctx->started) {
    ctx->started = true;
    ctx->beginPt = getTimeMark();
  }
  ctx->packetsNum++;
  if (ctx->packetsNum % ctx->config->groupSize == 0) {
    ctx->endPt = getTimeMark();
    ctx->publishedPacketsNum.store(ctx->packetsNum, std::memory_order_relaxed);
  }
}

// Asynchronous receiver callback
void test_readcb(AsyncOpStatus status,
                 aioObject *socket,
                 HostAddress address,
                 size_t transferred,
                 void *arg)
{
  __UNUSED(transferred);
  __UNUSED(address);
  threadPacketsNum++;
  ReceiverCtx *ctx = static_cast<ReceiverCtx*>(arg);
  if (receiverStopping(ctx))
    return;
  if (status == aosSuccess)
    receiverAccountPacket(ctx);
  // Drain the socket buffer inline, mirroring the sender at test_aio_writecb:
  // afActiveOnce reports every buffered datagram through the return value,
  // one completion per budget window comes back through this callback and
  // restarts the drain
  while (!receiverStopping(ctx) &&
         aioReadMsg(socket, &ctx->buffer, sizeof(ctx->buffer), afActiveOnce, 0, test_readcb, ctx) > 0) {
    threadPacketsNum++;
    receiverAccountPacket(ctx);
  }
}

// Asynchronous receiver with timer callback
void test_readcb_timer(AsyncOpStatus status,
                       aioObject *socket,
                       HostAddress address,
                       size_t transferred,
                       void *arg)
{
  __UNUSED(address);
  __UNUSED(transferred);
  threadPacketsNum++;
  ReceiverCtx *ctx = static_cast<ReceiverCtx*>(arg);
  if (receiverStopping(ctx))
    return;
  
  if (status == aosSuccess) {
    receiverAccountPacket(ctx);
    while (!receiverStopping(ctx) &&
           aioReadMsg(socket, &ctx->buffer, sizeof(ctx->buffer), afActiveOnce, 1000000, test_readcb_timer, ctx) > 0) {
      threadPacketsNum++;
      receiverAccountPacket(ctx);
    }
  } else {
    if (!ctx->started || ctx->oldPacketsNum != ctx->packetsNum) {
      ctx->oldPacketsNum = ctx->packetsNum;
      aioReadMsg(socket, &ctx->buffer, sizeof(ctx->buffer), afNone, 1000000, test_readcb_timer, ctx);
    }
  }
}

// Asynchronous receiver with RT timer callback
void test_readcb_timer_rt(AsyncOpStatus status,
                          aioObject *socket,
                          HostAddress address,
                          size_t transferred,
                          void *arg)
{
  __UNUSED(address);
  __UNUSED(transferred);
  threadPacketsNum++;
  ReceiverCtx *ctx = static_cast<ReceiverCtx*>(arg);
  if (receiverStopping(ctx))
    return;
  
  if (status == aosSuccess) {
    receiverAccountPacket(ctx);
    while (!receiverStopping(ctx) &&
           aioReadMsg(socket,
                      &ctx->buffer,
                      sizeof(ctx->buffer),
                      static_cast<AsyncFlags>(afRealtime | afActiveOnce),
                      1000000,
                      test_readcb_timer_rt,
                      ctx) > 0) {
      threadPacketsNum++;
      receiverAccountPacket(ctx);
    }
  } else {
    if (!ctx->started || ctx->oldPacketsNum != ctx->packetsNum) {
      ctx->oldPacketsNum = ctx->packetsNum;
      aioReadMsg(socket, &ctx->buffer, sizeof(ctx->buffer), afRealtime, 1000000, test_readcb_timer_rt, ctx);
    }
  }
}

// Asynchronous receiver thread
void *test_aio_receiver(void *arg)
{
  ReceiverCtx *ctx = static_cast<ReceiverCtx*>(arg);
  
  threadPacketsNum = 0;
  switch (ctx->type) {
    case aioReceiverAsync :
      aioReadMsg(ctx->server, &ctx->buffer, sizeof(ctx->buffer), afNone, 0, test_readcb, ctx);
      break;
    case aioReceiverAsyncTimer :
      aioReadMsg(ctx->server, &ctx->buffer, sizeof(ctx->buffer), afNone, 1000000, test_readcb_timer, ctx);
      break;      
    case aioReceiverAsyncRT :
      aioReadMsg(ctx->server, &ctx->buffer, sizeof(ctx->buffer), afRealtime, 1000000, test_readcb_timer_rt, ctx);
      break;
    default :
      fprintf(stderr, "Invalid receiver type, exiting...\n");
      exit(1);
  }
  
  asyncLoop(ctx->base);
  if (gDebug)
    printf("Thread packets num: %" PRIu64 "\n", threadPacketsNum);
  return nullptr;
}

// Asynchronous receiver coroutine
void test_coroutine_receiver_coro(void *arg)
{
  char msg[65536];
  ReceiverCtx *receiverCtx = static_cast<ReceiverCtx*>(arg);
  
  while (!receiverStopping(receiverCtx)) {
    ssize_t result = ioReadMsg(receiverCtx->server, msg, sizeof(msg), afNone, 1000000);
    if (result < 0 || receiverStopping(receiverCtx))
      return;
    receiverAccountPacket(receiverCtx);
  }
}

// Asynchronous coroutine receiver thread
void *test_coroutine_receiver(void *arg)
{
  ReceiverCtx *receiverCtx = static_cast<ReceiverCtx*>(arg);
  coroutineTy *receiverCoro = coroutineNew(test_coroutine_receiver_coro, receiverCtx, 0x20000);
  coroutineCall(receiverCoro);
  asyncLoop(receiverCtx->base);
  return nullptr;
}


// ======================================================================
// =                                                                    =
// =                       Benchmark function                           =
// =                                                                    =
// ======================================================================

static void receiverObjectDeleted(aioObjectRoot *object, void *arg)
{
  __UNUSED(object);
  ReceiverControl *control = static_cast<ReceiverControl*>(arg);
  postQuitOperation(control->base);
}

static uint64_t publishedReceiverPackets(const ReceiverCtx *receivers, unsigned receiverThreads)
{
  uint64_t packets = 0;
  for (unsigned i = 0; i < receiverThreads; i++)
    packets += receivers[i].publishedPacketsNum.load(std::memory_order_relaxed);
  return packets;
}

static void wakeBlockingReceivers(uint16_t port, unsigned receiverThreads)
{
  socketTy wakeSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, 0);
  sockaddr_in address;
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = inet_addr("127.0.0.1");
  address.sin_port = htons(port);
  char byte = 0;
  for (unsigned i = 0; i < receiverThreads; i++)
    sendto(wakeSocket, &byte, sizeof(byte), 0, reinterpret_cast<sockaddr*>(&address), sizeof(address));
  socketClose(wakeSocket);
}

void test_aio(unsigned senderThreads, unsigned receiverThreads, uint16_t port, AIOSenderTy senderTy, AIOReceiverTy receiverTy)
{
  if (gDebug)
    fprintf(stderr, "BEGIN port=%u S/R=%u/%u sender=%s receiver=%s\n",
            port, senderThreads, receiverThreads,
            aioSenderName[senderTy], aioReceiverName[receiverTy]);
  Context config(port);
  std::unique_ptr<SenderCtx[]> allSenders(new SenderCtx[senderThreads]());
  std::unique_ptr<ReceiverCtx[]> allReceivers(new ReceiverCtx[receiverThreads]);
  ReceiverControl receiverControl;
  std::vector<std::thread> senderPool;
  std::vector<std::thread> receiverPool;
  senderPool.reserve(senderThreads);
  receiverPool.reserve(receiverThreads);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = config.port;
  socketTy serverSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, receiverTy == aioReceiverBlocking ? 0 : 1);
  socketReuseAddr(serverSocket);
  if (socketBind(serverSocket, &address) != 0) {
    socketClose(serverSocket);
    return;
  }

  asyncBase *base = receiverTy == aioReceiverBlocking ? nullptr : createAsyncBase(amOSDefault, receiverThreads);
  receiverControl.base = base;

  timeMark pt = getTimeMark();
  aioObject *object = receiverTy != aioReceiverBlocking ? newSocketIo(base, serverSocket) : nullptr;
  if (object)
    objectSetDestructorCb(aioObjectHandle(object), receiverObjectDeleted, &receiverControl);

  for (unsigned i = 0; i < receiverThreads; i++) {
    allReceivers[i].base = base;
    allReceivers[i].config = &config;
    allReceivers[i].control = &receiverControl;
    allReceivers[i].type = receiverTy;
    allReceivers[i].serverSocket = serverSocket;
    if (receiverTy != aioReceiverBlocking)
      allReceivers[i].server = object;
    allReceivers[i].beginPt = pt;
    allReceivers[i].endPt = pt;    
    
    switch (receiverTy) {
      case aioReceiverBlocking:
        receiverPool.emplace_back(test_sync_receiver, &allReceivers[i]);
        break;
      case aioReceiverAsync :
      case aioReceiverAsyncTimer :
      case aioReceiverAsyncRT:
        receiverPool.emplace_back(test_aio_receiver, &allReceivers[i]);
        break;
      case aioReceiverCoroutine:
        receiverPool.emplace_back(test_coroutine_receiver, &allReceivers[i]);
        break;
    }
  }  
  
  for (unsigned i = 0; i < senderThreads; i++) {
    address.family = AF_INET;
    address.ipv4 = INADDR_ANY;
    address.port = 0;
    socketTy clientSocket = socketCreate(AF_INET, SOCK_DGRAM, IPPROTO_UDP, senderTy == aioSenderBlocking ? 0 : 1);
    if (socketBind(clientSocket, &address) != 0)
      exit(1);
    
    allSenders[i].config = &config;
    allSenders[i].clientSocket = clientSocket;
    
    switch (senderTy) {
      case aioSenderBlocking:
        senderPool.emplace_back(test_sync_sender, &allSenders[i]);
        break;
      case aioSenderAsync:
        senderPool.emplace_back(test_aio_sender, &allSenders[i]);
        break;
      case aioSenderCoroutine:
        senderPool.emplace_back(test_coroutine_sender, &allSenders[i]);
        break;
    }
  }

  for (std::thread &sender : senderPool)
    sender.join();

  for (unsigned i = 0; i < senderThreads; i++) {
    if (senderTy == aioSenderBlocking)
      socketClose(allSenders[i].clientSocket);
    else
      deleteAioObject(allSenders[i].client);
  }

  uint64_t previousPackets = publishedReceiverPackets(allReceivers.get(), receiverThreads);
  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    uint64_t currentPackets = publishedReceiverPackets(allReceivers.get(), receiverThreads);
    if (currentPackets == previousPackets)
      break;
    previousPackets = currentPackets;
  }

  receiverControl.stopping.store(true, std::memory_order_release);
  if (receiverTy == aioReceiverBlocking) {
    wakeBlockingReceivers(config.port, receiverThreads);
    for (std::thread &receiver : receiverPool)
      receiver.join();
    socketClose(serverSocket);
  } else {
    // Cancellation callbacks observe stopping and do not re-arm reads. The
    // destructor fires after the last operation callback and posts the quit
    // marker, so every loop remains available to finish its own coroutine and
    // drain the object before it is joined.
    deleteAioObject(object);
    for (std::thread &receiver : receiverPool)
      receiver.join();
  }

  uint64_t packetsNum = allReceivers[0].packetsNum;
  timeMark beginPt = allReceivers[0].beginPt;
  timeMark endPt = allReceivers[0].endPt;
  if (gDebug)
    printf("Receiver 1: %" PRIu64 " packets\n", allReceivers[0].packetsNum);
  for (unsigned i = 1; i < receiverThreads; i++) {
    if (allReceivers[i].beginPt.mark < beginPt.mark)
      beginPt = allReceivers[i].beginPt;
    if (allReceivers[i].endPt.mark > endPt.mark)
      endPt = allReceivers[i].endPt;
    packetsNum += allReceivers[i].packetsNum;
    if (gDebug)
      printf("Receiver %u: %" PRIu64 " packets\n", i+1, allReceivers[i].packetsNum);
  }
  
  double totalSeconds = usDiff(beginPt, endPt) / 1000000.0;
  double rate = totalSeconds > 0 ? packetsNum / totalSeconds : 0;
  printf("Threads S/R %u/%u sender=%s, receiver=%s, total messages: %" PRIu64 ", packet lost: %.2lf%%, elapsed time: %.3lf, rate: %.3lf msg/s\n",
         senderThreads,
         receiverThreads,
         aioSenderName[senderTy],
         aioReceiverName[receiverTy],
         packetsNum,
         (1.0 - (packetsNum / static_cast<double>(config.totalPacketNum*senderThreads))) * 100.0,
         totalSeconds,
         rate);
  fflush(stdout);
  if (gDebug)
    fprintf(stderr, "END port=%u\n", port);

  // Let the previous run fully wind down (worker threads exiting, in-flight
  // datagrams draining, deferred object reclamation) before the next one
  // starts, so TSan does not attribute its leftovers to the next run
  std::this_thread::sleep_for(std::chrono::seconds(1));
}

int main(int argc, char **argv)
{
  if (argc > 1) {
    uint64_t packetNum = strtoull(argv[1], nullptr, 10);
    if (packetNum)
      gTotalPacketNum = packetNum;
  }

  initializeAsyncIo(aiNone);
  uint16_t port = gPortBase;
  
  // Blocking tests
  test_aio(1, 1, port++, aioSenderBlocking, aioReceiverBlocking);
  test_aio(4, 1, port++, aioSenderBlocking, aioReceiverBlocking);
  test_aio(1, 2, port++, aioSenderBlocking, aioReceiverBlocking);
  test_aio(1, 4, port++, aioSenderBlocking, aioReceiverBlocking);
  test_aio(4, 4, port++, aioSenderBlocking, aioReceiverBlocking);
  
  // Senders test with blocking receiver
  test_aio(1, 1, port++, aioSenderAsync, aioReceiverBlocking);
  test_aio(4, 1, port++, aioSenderAsync, aioReceiverBlocking);
  test_aio(1, 1, port++, aioSenderCoroutine, aioReceiverBlocking);
  test_aio(4, 1, port++, aioSenderCoroutine, aioReceiverBlocking);

  // Receivers test with blocking sender
  test_aio(1, 1, port++, aioSenderBlocking, aioReceiverAsync);
  test_aio(4, 1, port++, aioSenderBlocking, aioReceiverAsync);
  test_aio(1, 1, port++, aioSenderBlocking, aioReceiverAsyncTimer);
  test_aio(4, 1, port++, aioSenderBlocking, aioReceiverAsyncTimer);
  test_aio(1, 1, port++, aioSenderBlocking, aioReceiverAsyncRT);
  test_aio(4, 1, port++, aioSenderBlocking, aioReceiverAsyncRT);
  test_aio(1, 1, port++, aioSenderBlocking, aioReceiverCoroutine);
  test_aio(4, 1, port++, aioSenderBlocking, aioReceiverCoroutine);

  // Multi-threading receivers
  test_aio(1, 2, port++, aioSenderBlocking, aioReceiverAsync);
  test_aio(1, 4, port++, aioSenderBlocking, aioReceiverAsync);
  test_aio(4, 4, port++, aioSenderBlocking, aioReceiverAsync);
  
  test_aio(1, 2, port++, aioSenderBlocking, aioReceiverAsyncTimer);
  test_aio(1, 4, port++, aioSenderBlocking, aioReceiverAsyncTimer);
  test_aio(4, 4, port++, aioSenderBlocking, aioReceiverAsyncTimer);

  test_aio(1, 2, port++, aioSenderBlocking, aioReceiverAsyncRT);
  test_aio(1, 4, port++, aioSenderBlocking, aioReceiverAsyncRT);
  test_aio(4, 4, port++, aioSenderBlocking, aioReceiverAsyncRT);
  
  test_aio(1, 2, port++, aioSenderBlocking, aioReceiverCoroutine);
  test_aio(1, 4, port++, aioSenderBlocking, aioReceiverCoroutine);
  test_aio(4, 4, port++, aioSenderBlocking, aioReceiverCoroutine);
  return 0;
}
