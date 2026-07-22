#include <asyncio/coroutine.h>
#include <asyncio/socket.h>
#include <asyncioextras/zmtp.h>
#include <zmq.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#ifndef OS_WINDOWS
#include <fcntl.h>
#endif

constexpr unsigned groupSize = 16;
static unsigned gInterval = 6;
static uint16_t gPort = 65333;
static unsigned gPacketSize = 1;

enum SenderTy {
  senderTyZMQ = 0,
  senderTyAio,
  senderTyCoroutine,
  senderTyRawSend
};

enum ReceiverTy {
  receiverTyZMQ = 0,
  receiverTyAio,
  receiverTyCoroutine
};

static const char *SenderTyNames[] = {
  "ZMQ",
  "Async(NO BATCH!)",
  "Coroutine(NO BATCH!)",
  "RAW(NO BATCH!)"
};

static const char *ReceiverTyNames[] = {
  "ZMQ",
  "Async",
  "Coroutine"
};

__NO_PADDING_BEGIN
struct Configuration {
  uint16_t port;
  unsigned packetSize;
  Configuration() {}
};

struct SenderContext {
  Configuration *cfg;
  uint64_t sent;
  asyncBase *base;
  socketTy socketId;
  zmtpSocket *socket;
  decltype (std::chrono::steady_clock::now()) startPoint;
  std::unique_ptr<uint8_t[]> data;
  uint64_t counter;
  std::atomic<unsigned> deleted;
  SenderContext(Configuration *cfgArg) : cfg(cfgArg), sent(0), deleted(0) {
    data.reset(new uint8_t[gPacketSize]);
  }
};

struct ReceiverContext {
  Configuration *cfg;
  uint64_t received;
  int64_t milliSecondsElapsed;
  decltype (std::chrono::steady_clock::now()) startPoint;
  decltype (std::chrono::steady_clock::now()) endPoint;
  asyncBase *base;
  aioObject *listener;
  zmtpStream stream;
  uint64_t counter;
  std::atomic<unsigned> deleted;
  ReceiverContext(Configuration *cfgArg) : cfg(cfgArg), received(0), milliSecondsElapsed(0), deleted(0) {}
};
__NO_PADDING_END

// The measured send loop shared by every sender flavor: batches of 128
// packets until gInterval seconds elapse or the send function reports an
// error. Returns the number of packets sent; the send lambda inlines, so the
// hot loop compiles to the same code as the former per-sender copies.
template<typename SendFn>
uint64_t timedBatchSend(SendFn send)
{
  bool run = true;
  auto startTime = std::chrono::steady_clock::now();
  uint64_t packetsNum = 0;
  while (run) {
    for (unsigned i = 0; i < 128; i++) {
      if (send() < 0) {
        run = false;
        break;
      }
      packetsNum++;
    }

    auto currentTime = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::seconds>(currentTime-startTime).count() >= gInterval)
      run = false;
  }

  return packetsNum;
}

// An async TCP socket bound to an ephemeral local port; false when the bind
// fails and the benchmark leg must be skipped.
bool createBoundClient(socketTy &clientSocket)
{
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = 0;
  clientSocket = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  return socketBind(clientSocket, &address) == 0;
}

// Listener on the benchmark port; the bind retry lets the previous run's
// socket leave TIME_WAIT. Returns nullptr when the port stays unavailable.
aioObject *createListener(asyncBase *base, uint16_t port)
{
  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = INADDR_ANY;
  address.port = port;
  socketTy listener = socketCreate(AF_INET, SOCK_STREAM, IPPROTO_TCP, 1);
  if (socketBind(listener, &address) != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (socketBind(listener, &address) != 0)
      return nullptr;
  }

  if (socketListen(listener) != 0)
    return nullptr;

  return newSocketIo(base, listener);
}

void senderZMQ(SenderContext *ctx)
{
  auto zmqCtx = zmq_ctx_new();
  auto socket = zmq_socket(zmqCtx, ZMQ_PUSH);
  int linger = 0;
  int timeout = 1000;
  int msgBufferSize = 100000;
  zmq_setsockopt (socket, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
  zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(int));
  zmq_setsockopt(socket, ZMQ_SNDHWM, &msgBufferSize, sizeof(msgBufferSize));
  zmq_setsockopt(socket, ZMQ_RCVHWM, &msgBufferSize, sizeof(msgBufferSize));


  char address[128];
  snprintf(address, sizeof(address), "tcp://127.0.0.1:%u", static_cast<unsigned>(ctx->cfg->port));
  int connectResult = zmq_connect(socket, address);
  if (connectResult == 0) {
    auto data = std::unique_ptr<uint8_t[]>(new uint8_t[ctx->cfg->packetSize]);
    for (unsigned i = 0; i < ctx->cfg->packetSize; i++)
      data[i] = static_cast<uint8_t>(rand());

    // Send first packet
    int sendResult = zmq_send(socket, data.get(), ctx->cfg->packetSize, 0);
    if (sendResult > 0) {
      ctx->sent += timedBatchSend([&]() {
        return zmq_send(socket, data.get(), ctx->cfg->packetSize, 0);
      });
    } else {
      fprintf(stderr, "senderZMQ: zmq_send (first packet) failed error=%i\n", sendResult);
    }
  } else {
    fprintf(stderr, "senderZMQ: zmq_connect failed error=%i\n", connectResult);
  }

  zmq_close(socket);
  zmq_term(zmqCtx);
}

static void senderAsyncSendCb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  auto ctx = static_cast<SenderContext*>(arg);
  // Completions still queued behind the cutoff arrive after zmtpSocketDelete:
  // the socket is gone, they must not pump new sends into it
  if (ctx->deleted.load(std::memory_order_relaxed) != 0)
    return;
  if (status == aosSuccess) {
    if (ctx->sent == 0 && ctx->counter == 0)
      ctx->startPoint = std::chrono::steady_clock::now();
    ctx->counter++;
    if (ctx->counter >= groupSize) {
      ctx->sent += groupSize;
      ctx->counter = 0;
      auto endPoint = std::chrono::steady_clock::now();
      auto diff = std::chrono::duration_cast<std::chrono::seconds>(endPoint - ctx->startPoint).count();
      if (diff >= gInterval && ctx->deleted.fetch_add(1) == 0) {
        zmtpSocketDelete(socket);
        postQuitOperation(ctx->base);
        return;
      }
    }

    for (;;) {
      ssize_t result = aioZmtpSend(socket, ctx->data.get(), gPacketSize, zmtpMessage, afActiveOnce, 1000000, senderAsyncSendCb, ctx);
      if (result == -aosPending) {
        break;
      } else {
        ctx->sent += groupSize;
        ctx->counter = 0;
        auto endPoint = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration_cast<std::chrono::seconds>(endPoint - ctx->startPoint).count();
        if (diff >= gInterval && ctx->deleted.fetch_add(1) == 0) {
          zmtpSocketDelete(socket);
          postQuitOperation(ctx->base);
          return;
        }
      }
    }

  } else {
    fprintf(stderr, "senderAsync: zmtp send failed code %i\n", status);
    if (ctx->deleted.fetch_add(1) == 0) {
      zmtpSocketDelete(socket);
      postQuitOperation(ctx->base);
    }
  }
}

static void senderAsyncConnectCb(AsyncOpStatus status, zmtpSocket *client, void *arg)
{
  auto ctx = static_cast<SenderContext*>(arg);
  if (status == aosSuccess) {
    ctx->counter = 0;
    aioZmtpSend(client, ctx->data.get(), gPacketSize, zmtpMessage, afNone, 1000000, senderAsyncSendCb, ctx);
  } else {
    fprintf(stderr, "senderAsync: zmtp connect failed code %i\n", status);
    if (ctx->deleted.fetch_add(1) == 0) {
      zmtpSocketDelete(client);
      postQuitOperation(ctx->base);
    }
  }
}

void senderAsync(SenderContext *ctx)
{
  ctx->base = createAsyncBase(amOSDefault, 1);

  socketTy senderSocket;
  if (!createBoundClient(senderSocket))
    return;

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = ctx->cfg->port;
  zmtpSocket *zmtpSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, senderSocket), zmtpSocketPUSH);
  aioZmtpConnect(zmtpSocket, &address, afNone, 1000000, senderAsyncConnectCb, ctx);
  asyncLoop(ctx->base);
}

void senderCoroutineProc(void *arg)
{
  auto ctx = static_cast<SenderContext*>(arg);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = ctx->cfg->port;
  int connectResult = ioZmtpConnect(ctx->socket, &address, afNone, 1000000);
  if (connectResult == 0) {
    auto data = std::unique_ptr<uint8_t[]>(new uint8_t[ctx->cfg->packetSize]);
    for (unsigned i = 0; i < ctx->cfg->packetSize; i++)
      data[i] = static_cast<uint8_t>(rand());

    // Send first packet
    ssize_t sendResult = ioZmtpSend(ctx->socket, data.get(), ctx->cfg->packetSize, zmtpMessage, afNone, 1000000);
    if (sendResult > 0) {
      ctx->sent += timedBatchSend([&]() {
        return ioZmtpSend(ctx->socket, data.get(), ctx->cfg->packetSize, zmtpMessage, afNone, 1000000);
      });
    } else {
      fprintf(stderr, "senderCoroutine: ioZmtpSend (first packet) failed error=%zi\n", -sendResult);
    }
  } else {
    fprintf(stderr, "senderCoroutine: ioZmtpConnect failed error=%i\n", connectResult);
  }

  zmtpSocketDelete(ctx->socket);
  postQuitOperation(ctx->base);
}


void senderCoroutine(SenderContext *ctx)
{
  ctx->base = createAsyncBase(amOSDefault, 1);

  socketTy senderSocket;
  if (!createBoundClient(senderSocket))
    return;

  ctx->socket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, senderSocket), zmtpSocketPUSH);
  coroutineCall(coroutineNew(senderCoroutineProc, ctx, 0x10000));
  asyncLoop(ctx->base);
}

void senderRawProc(void *arg)
{
  auto ctx = static_cast<SenderContext*>(arg);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = ctx->cfg->port;
  int connectResult = ioZmtpConnect(ctx->socket, &address, afNone, 1000000);
  if (connectResult == 0) {
    auto data = std::unique_ptr<uint8_t[]>(new uint8_t[ctx->cfg->packetSize]);
    for (unsigned i = 0; i < ctx->cfg->packetSize; i++)
      data[i] = static_cast<uint8_t>(rand());

    // Build packet
    uint8_t packet[128];
    memset(packet, 0, sizeof(packet));
    packet[0] = 0; // zmtpMsgFlagNone
    packet[1] = static_cast<uint8_t>(ctx->cfg->packetSize);

    // Switch socket to blocking mode
#ifdef OS_WINDOWS
    u_long arg = 0;
    ioctlsocket(ctx->socketId, FIONBIO, &arg);
#else
    int current = fcntl(ctx->socketId, F_GETFL);
    fcntl(ctx->socketId, F_SETFL, current & (~O_NONBLOCK));
#endif

    // Send first packet
    ssize_t sendResult = send(ctx->socketId, reinterpret_cast<const char*>(packet), ctx->cfg->packetSize+2, 0);
    if (sendResult > 0) {
      ctx->sent += timedBatchSend([&]() {
        return send(ctx->socketId, reinterpret_cast<const char*>(packet), ctx->cfg->packetSize+2, 0);
      });
    } else {
      fprintf(stderr, "senderRaw: send (first packet) failed error=%zi\n", -sendResult);
    }
  } else {
    fprintf(stderr, "senderRaw: ioZmtpConnect failed error=%i\n", connectResult);
  }

  zmtpSocketDelete(ctx->socket);
  postQuitOperation(ctx->base);
}

void senderRaw(SenderContext *ctx)
{
  ctx->base = createAsyncBase(amOSDefault, 1);

  if (!createBoundClient(ctx->socketId))
    return;

  ctx->socket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, ctx->socketId), zmtpSocketPUSH);
  coroutineCall(coroutineNew(senderRawProc, ctx, 0x10000));
  asyncLoop(ctx->base);
}

void receiverZMQ(ReceiverContext *ctx)
{
  auto zmqCtx = zmq_ctx_new();
  auto socket = zmq_socket(zmqCtx, ZMQ_PULL);
  int linger = 0;
  int timeout = 3000;
  int msgBufferSize = 100000;
  zmq_setsockopt (socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(int));
  zmq_setsockopt(socket, ZMQ_SNDHWM, &msgBufferSize, sizeof(msgBufferSize));
  zmq_setsockopt(socket, ZMQ_RCVHWM, &msgBufferSize, sizeof(msgBufferSize));

  char address[128];
  snprintf(address, sizeof(address), "tcp://127.0.0.1:%u", static_cast<unsigned>(ctx->cfg->port));
  int bindResult = zmq_bind(socket, address);
  if (bindResult == 0) {
    uint64_t packetsNum = 0;
    auto data = std::unique_ptr<uint8_t[]>(new uint8_t[ctx->cfg->packetSize]);
    if (zmq_recv(socket, data.get(), ctx->cfg->packetSize, 0) > 0) {
      packetsNum++;
      auto startTime = std::chrono::steady_clock::now();
      auto endTime = std::chrono::steady_clock::now();
      while (zmq_recv(socket, data.get(), ctx->cfg->packetSize, 0) > 0) {
        packetsNum++;
        if (packetsNum >= groupSize) {
          packetsNum = 0;
          ctx->received += groupSize;
          endTime = std::chrono::steady_clock::now();
        }
      }

      ctx->milliSecondsElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(endTime-startTime).count();
    }
  } else {
    fprintf(stderr, "receiverZMQ: zmq_bind failed error=%i\n", bindResult);
  }

  zmq_close(socket);
  zmq_term(zmqCtx);
}

static void receiverAsyncRecvCb(AsyncOpStatus status, zmtpSocket *socket, zmtpUserMsgTy, zmtpStream *stream, void *arg)
{
  auto ctx = static_cast<ReceiverContext*>(arg);
  // Same cutoff contract as the sender: late completions arrive after
  // zmtpSocketDelete and must not touch the socket
  if (ctx->deleted.load(std::memory_order_relaxed) != 0)
    return;
  if (status == aosSuccess) {
    if (ctx->received == 0 && ctx->counter == 0)
      ctx->startPoint = std::chrono::steady_clock::now();

    ctx->counter++;
    if (ctx->counter >= groupSize) {
      ctx->counter = 0;
      ctx->received += groupSize;
      ctx->endPoint = std::chrono::steady_clock::now();
    }

    for (;;) {
      ssize_t result = aioZmtpRecv(socket, *stream, gPacketSize, afActiveOnce, 1000000, receiverAsyncRecvCb, ctx);
      if (result == -aosPending) {
        break;
      } else {
        ctx->counter++;
        if (ctx->counter >= groupSize) {
          ctx->counter = 0;
          ctx->received += groupSize;
          ctx->endPoint = std::chrono::steady_clock::now();
        }
      }
    }

  } else if (status == aosTimeout || status == aosDisconnected) {
    if (ctx->received == 0)
      fprintf(stderr, "receiverAsync: zmtp receive timeout\n");
    ctx->milliSecondsElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(ctx->endPoint-ctx->startPoint).count();
    if (ctx->deleted.fetch_add(1) == 0) {
      zmtpSocketDelete(socket);
      postQuitOperation(ctx->base);
    }
  } else {
    fprintf(stderr, "receiverAsync: zmtp receive failed code %i\n", status);
    if (ctx->deleted.fetch_add(1) == 0) {
      zmtpSocketDelete(socket);
      postQuitOperation(ctx->base);
    }
  }
}

static void receiverAsyncZmtpAcceptCb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  auto ctx = static_cast<ReceiverContext*>(arg);
  if (status == aosSuccess) {
    ctx->counter = 0;
    aioZmtpRecv(socket, ctx->stream, gPacketSize, afNone, 1000000, receiverAsyncRecvCb, ctx);
  } else {
    fprintf(stderr, "receiverAsync: zmtp accept failed code %i\n", status);
    if (ctx->deleted.fetch_add(1) == 0) {
      zmtpSocketDelete(socket);
      postQuitOperation(ctx->base);
    }
  }
}

static void receiverAsyncAcceptCb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<ReceiverContext*>(arg);
  if (status == aosSuccess) {
    zmtpSocket *socket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPULL);
    aioZmtpAccept(socket, afNone, 1000000, receiverAsyncZmtpAcceptCb, ctx);
  } else {
    fprintf(stderr, "receiverAsync: accept failed code %i\n", status);
    postQuitOperation(ctx->base);
  }
}

static void receiverAsync(ReceiverContext *ctx)
{
  aioObject *listenerObject = createListener(ctx->base, ctx->cfg->port);
  if (!listenerObject)
    return;

  aioAccept(listenerObject, 3000000, receiverAsyncAcceptCb, ctx);
  asyncLoop(ctx->base);
  deleteAioObject(listenerObject);
}

void receiverCoroutineProc(void *arg)
{
  auto ctx = static_cast<ReceiverContext*>(arg);
  socketTy socketId;
  int status = ioAccept(ctx->listener, &socketId, nullptr, 3000000);
  if (status < 0) {
    fprintf(stderr, "receiverCoroutine: accept failed code %i\n", -status);
    postQuitOperation(ctx->base);
    return;
  }

  zmtpSocket *socket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, socketId), zmtpSocketPULL);
  int acceptResult = ioZmtpAccept(socket, afNone, 1000000);
  if (acceptResult == 0) {
    uint64_t packetsNum = 0;
    zmtpUserMsgTy msgType;
    if (ioZmtpRecv(socket, ctx->stream, ctx->cfg->packetSize, afNone, 1000000, &msgType) > 0) {
      packetsNum++;
      auto startTime = std::chrono::steady_clock::now();
      auto endTime = std::chrono::steady_clock::now();
      while (ioZmtpRecv(socket, ctx->stream, ctx->cfg->packetSize, afNone, 1000000, &msgType) > 0) {
        packetsNum++;
        if (packetsNum >= groupSize) {
          packetsNum = 0;
          ctx->received += groupSize;
          endTime = std::chrono::steady_clock::now();
        }
      }

      ctx->milliSecondsElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(endTime-startTime).count();
    }
  } else {
    fprintf(stderr, "receiverCoroutine: ioZmtpAccept failed error=%i\n", acceptResult);
  }

  zmtpSocketDelete(socket);
  postQuitOperation(ctx->base);
}

static void receiverCoroutine(ReceiverContext *ctx)
{
  ctx->listener = createListener(ctx->base, ctx->cfg->port);
  if (!ctx->listener)
    return;

  coroutineCall(coroutineNew(receiverCoroutineProc, ctx, 0x10000));
  asyncLoop(ctx->base);
  deleteAioObject(ctx->listener);
}

void runBenchmark(SenderTy senderType, ReceiverTy receiverType, uint16_t port)
{
  Configuration cfg;
  cfg.packetSize = gPacketSize;
  cfg.port = port;
  SenderContext senderCtx(&cfg);
  ReceiverContext receiverCtx(&cfg);
  receiverCtx.base = createAsyncBase(amOSDefault, 1);

  std::thread receiver;
  std::thread sender;

  switch (receiverType) {
    case receiverTyZMQ :
      receiver = std::thread(receiverZMQ, &receiverCtx);
      break;
    case receiverTyAio :
      receiver = std::thread(receiverAsync, &receiverCtx);
      break;
    case receiverTyCoroutine :
      receiver = std::thread(receiverCoroutine, &receiverCtx);
      break;
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(333));

  switch (senderType) {
    case senderTyZMQ :
      sender = std::thread(senderZMQ, &senderCtx);
      break;
    case senderTyAio :
      sender = std::thread(senderAsync, &senderCtx);
      break;
    case senderTyCoroutine :
      sender = std::thread(senderCoroutine, &senderCtx);
      break;
    case senderTyRawSend :
      sender = std::thread(senderRaw, &senderCtx);
      break;
  }

  sender.join();
  receiver.join();

  printf("Sender=%s Receiver=%s ", SenderTyNames[senderType], ReceiverTyNames[receiverType]);
  if (receiverCtx.milliSecondsElapsed)
    printf("rate: %.3lf messages/second; time elapsed: %.3lf sec\n", receiverCtx.received / (receiverCtx.milliSecondsElapsed/1000.0), receiverCtx.milliSecondsElapsed/1000.0);
  else
    printf("rate: unknown (test failed)\n");
}

int main(int, char **)
{
  initializeAsyncIo(aiNone);
  uint16_t port = gPort;

  // Original
  runBenchmark(senderTyZMQ, receiverTyZMQ, port++);

  // Senders
  runBenchmark(senderTyAio, receiverTyZMQ, port++);
  runBenchmark(senderTyCoroutine, receiverTyZMQ, port++);
  runBenchmark(senderTyRawSend, receiverTyZMQ, port++);

  // Receivers
  runBenchmark(senderTyZMQ, receiverTyAio, port++);
  runBenchmark(senderTyZMQ, receiverTyCoroutine, port++);

  // Together
  runBenchmark(senderTyAio, receiverTyAio, port++);
  runBenchmark(senderTyCoroutine, receiverTyCoroutine, port++);

  return 0;
}
