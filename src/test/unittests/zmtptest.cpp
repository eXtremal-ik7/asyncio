#include "unittest.h"
#include <asyncio/coroutine.h>
#include <asyncio/socket.h>
#include <asyncioextras/zmtp.h>
#include <zmq.h>
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

__NO_PADDING_BEGIN
struct zmtpContext {
  asyncBase *base;
  std::atomic<bool> serverRunning;
  std::atomic<bool> clientRunning;
  int clientState;
  int serverState;
  aioObject *listener;
  zmtpSocket *clientSocket;
  zmtpSocket *serverSocket;
  zmtpStream stream;
  zmtpContext(asyncBase *baseArg) : base(baseArg), serverRunning(false), clientRunning(false), clientState(0), serverState(0) {}
};
__NO_PADDING_END

static bool waitClient(zmtpContext *ctx)
{
  for (unsigned i = 0; i < 5000; i++) {
    if (ctx->clientRunning == false)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  return ctx->clientRunning == false;
}

static bool waitServer(zmtpContext *ctx, bool event=false)
{
  for (unsigned i = 0; i < 5000; i++) {
    if (ctx->serverRunning == event)
      break;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  return ctx->serverRunning == event;
}

static void zmq_server_pull(zmtpContext *ctx, uint16_t port)
{
  auto zmqCtx = zmq_ctx_new();
  auto socket = zmq_socket(zmqCtx, ZMQ_PULL);

  int timeout = 3000;
  zmq_setsockopt (socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));

  char address[128];
  snprintf(address, sizeof(address), "tcp://127.0.0.1:%u", static_cast<unsigned>(port));
  int bindResult;
  if ( (bindResult = zmq_bind(socket, address)) != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    bindResult = zmq_bind(socket, address);
  }

  EXPECT_EQ(bindResult, 0);
  if (bindResult == 0) {
    int recvResult;
    ctx->serverRunning = true;
    ctx->serverState = 1;

    {
      reqStruct msg = {0, 0};
      recvResult = zmq_recv(socket, &msg, sizeof(msg), 0);
      EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(msg)));
      EXPECT_EQ(msg.a, 11u);
      EXPECT_EQ(msg.b, 77u);
      if (recvResult == sizeof(msg) && msg.a == 11 && msg.b == 77)
        ctx->serverState++;
    }

    {
      reqStruct longMsg[1024];
      recvResult = zmq_recv(socket, longMsg, sizeof(longMsg), 0);
      EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(longMsg)));
      bool valid = true;
      for (unsigned i = 0; i < 1024; i++) {
        EXPECT_EQ(longMsg[i].a, i);
        EXPECT_EQ(longMsg[i].b, i);
        if (longMsg[i].a != i || longMsg[i].b != i) {
          valid = false;
          break;
        }
      }

      if (valid)
        ctx->serverState++;
    }
  }

  ctx->serverRunning = false;
  EXPECT_EQ(waitClient(ctx), true);
  zmq_close(socket);
  zmq_term(zmqCtx);
}

static void zmq_server_rep(zmtpContext *ctx, uint16_t port)
{
  auto zmqCtx = zmq_ctx_new();
  auto socket = zmq_socket(zmqCtx, ZMQ_REP);

  int linger = 0;
  int timeout = 3000;
  zmq_setsockopt(socket, ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
  zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(int));

  char address[128];
  snprintf(address, sizeof(address), "tcp://127.0.0.1:%u", static_cast<unsigned>(port));
  int bindResult;
  if ( (bindResult = zmq_bind(socket, address)) != 0) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    bindResult = zmq_bind(socket, address);
  }

  EXPECT_EQ(bindResult, 0);
  if (bindResult == 0) {
    int recvResult;
    ctx->serverRunning = true;
    ctx->serverState = 1;

    {
      reqStruct msg = {0, 0};
      recvResult = zmq_recv(socket, &msg, sizeof(msg), 0);
      EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(msg)));
      EXPECT_EQ(msg.a, 11u);
      EXPECT_EQ(msg.b, 77u);
      if (recvResult == sizeof(msg) && msg.a == 11 && msg.b == 77) {
        repStruct rep;
        rep.c = msg.a + msg.b;
        if (zmq_send(socket, &rep, sizeof(rep), 0) == sizeof(rep))
          ctx->serverState++;
      }
    }

    {
      reqStruct longReq[1024];
      repStruct longRep[1024];
      recvResult = zmq_recv(socket, longReq, sizeof(longReq), 0);
      EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(longReq)));
      bool valid = true;
      for (unsigned i = 0; i < 1024; i++) {
        longRep[i].c = longReq[i].a + longReq[i].b;
        EXPECT_EQ(longReq[i].a, i);
        EXPECT_EQ(longReq[i].b, i);
        if (longReq[i].a != i || longReq[i].b != i) {
          valid = false;
          break;
        }
      }

      if (valid) {
        if (zmq_send(socket, longRep, sizeof(longRep), 0) == sizeof(longRep))
          ctx->serverState++;
      }
    }

    {
      reqStruct msg;
      recvResult = zmq_recv(socket, &msg, sizeof(msg), 0);
      EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(msg)));
      if (recvResult == sizeof(msg)) {
        repStruct rep;
        rep.c = msg.a + msg.b;
        if (zmq_send(socket, &rep, sizeof(rep), 0) == sizeof(rep))
          ctx->serverState++;
      }
    }
  }

  ctx->serverRunning = false;
  EXPECT_EQ(waitClient(ctx), true);
  zmq_close(socket);
  zmq_term(zmqCtx);
}

static void zmq_server_push(zmtpContext *ctx, uint16_t port)
{
  auto zmqCtx = zmq_ctx_new();
  auto socket = zmq_socket(zmqCtx, ZMQ_PUSH);

  int linger = 0;
  int timeout = 3000;
  zmq_setsockopt (socket, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
  zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(int));

  char address[128];
  snprintf(address, sizeof(address), "tcp://127.0.0.1:%u", static_cast<unsigned>(port));
  int connectResult = zmq_connect(socket, address);
  EXPECT_EQ(connectResult, 0);
  if (connectResult == 0) {
    ctx->clientState = 1;
    // Short message
    {
      reqStruct req;
      req.a = 11;
      req.b = 77;
      if (zmq_send(socket, &req, sizeof(req), 0) == sizeof(req))
        ctx->clientState++;
    }

    // Long Message
    {
      auto data = std::unique_ptr<reqStruct[]>(new reqStruct[1024]);
      for (unsigned i = 0; i < 1024; i++) {
        data[i].a = i;
        data[i].b = i;
      }
      if (zmq_send(socket, data.get(), 1024*sizeof(reqStruct), 0) == 1024*sizeof(reqStruct))
        ctx->clientState++;
    }

    // Limit check
    {
      reqStruct req;
      req.a = 99;
      req.b = 99;
      if (zmq_send(socket, &req, sizeof(req), 0) == sizeof(req))
        ctx->clientState++;
    }
  }

  EXPECT_EQ(waitServer(ctx), true);
  zmq_close(socket);
  zmq_term(zmqCtx);
  ctx->clientRunning = false;
}

static void zmq_server_req(zmtpContext *ctx, uint16_t port)
{
  auto zmqCtx = zmq_ctx_new();
  auto socket = zmq_socket(zmqCtx, ZMQ_REQ);

  int linger = 0;
  int timeout = 3000;
  zmq_setsockopt (socket, ZMQ_SNDTIMEO, &timeout, sizeof(timeout));
  zmq_setsockopt(socket, ZMQ_LINGER, &linger, sizeof(int));

  char address[128];
  snprintf(address, sizeof(address), "tcp://127.0.0.1:%u", static_cast<unsigned>(port));
  int connectResult = zmq_connect(socket, address);
  EXPECT_EQ(connectResult, 0);
  if (connectResult == 0) {
    int recvResult;
    ctx->clientState = 1;
    // Short message
    {
      reqStruct req;
      repStruct rep = {0};
      req.a = 11;
      req.b = 77;
      zmq_send(socket, &req, sizeof(req), 0);
      recvResult = zmq_recv(socket, &rep, sizeof(rep), 0);
      EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(rep)));
      EXPECT_EQ(rep.c, req.a+req.b);
      if (recvResult == sizeof(rep) && rep.c == req.a+req.b)
        ctx->clientState++;
    }

    // Long Message
    {
      bool valid = true;
      auto data = std::unique_ptr<reqStruct[]>(new reqStruct[1024]);
      auto rep = std::unique_ptr<repStruct[]>(new repStruct[1024]);
      for (unsigned i = 0; i < 1024; i++) {
        data[i].a = i;
        data[i].b = i;
      }
      zmq_send(socket, data.get(), 1024*sizeof(reqStruct), 0);
      recvResult = zmq_recv(socket, rep.get(), 1024*sizeof(repStruct), 0);
      EXPECT_EQ(recvResult, static_cast<ssize_t>(1024*sizeof(repStruct)));
      if (recvResult == 1024*sizeof(repStruct)) {
        for (unsigned i = 0; i < 1024; i++) {
          EXPECT_EQ(rep[i].c, data[i].a + data[i].b);
          if (rep[i].c != data[i].a + data[i].b) {
            valid = false;
            break;
          }
        }
      } else {
        valid = false;
      }

      if (valid) {
        ctx->clientState++;
      }
    }

    // Limit check
    {
      reqStruct req;
      req.a = 99;
      req.b = 99;
      zmq_send(socket, &req, sizeof(req), 0);
      ctx->clientState++;
    }
  }

  EXPECT_EQ(waitServer(ctx), true);
  zmq_close(socket);
  zmq_term(zmqCtx);
  ctx->clientRunning = false;
}

static void aio_push_writecb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status == aosSuccess)
    ctx->clientState++;
}

static void aio_push_connectcb(AsyncOpStatus status, zmtpSocket *client, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status == aosSuccess) {
    reqStruct req;
    req.a = 11;
    req.b = 77;
    ctx->clientState = 1;
    // short message
    aioZmtpSend(ctx->clientSocket, &req, sizeof(req), zmtpMessage, afNone, 1000000, aio_push_writecb, ctx);

    // long message
    {
      auto data = std::unique_ptr<reqStruct[]>(new reqStruct[1024]);
      for (unsigned i = 0; i < 1024; i++) {
        data[i].a = i;
        data[i].b = i;
      }
      aioZmtpSend(ctx->clientSocket, data.get(), 1024*sizeof(reqStruct), zmtpMessage, afNone, 1000000, aio_push_writecb, ctx);
    }
  }

  EXPECT_EQ(waitServer(ctx), true);
  zmtpSocketDelete(client);
  postQuitOperation(ctx->base);
  ctx->clientRunning = false;
}


TEST(zmtp, aio_push)
{
  zmtpContext context(gBase);
  std::thread serverThread(zmq_server_pull, &context, gPort);

  // Wait server
  EXPECT_EQ(waitServer(&context, true), true);
  context.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPUSH);
  context.clientRunning = true;

  HostAddress address;
  address.ipv4 = inet_addr("127.0.0.1");
  address.family = AF_INET;
  address.port = gPort;
  aioZmtpConnect(context.clientSocket, &address, afNone, 1000000, aio_push_connectcb, &context);
  asyncLoop(gBase);
  serverThread.join();
  ASSERT_EQ(context.clientState, 3);
  ASSERT_EQ(context.serverState, 3);
}

void aio_push_client_coro(void *arg)
{
  auto ctx = static_cast<zmtpContext*>(arg);

  HostAddress address;
  address.ipv4 = inet_addr("127.0.0.1");
  address.family = AF_INET;
  address.port = gPort;
  int connectResult = ioZmtpConnect(ctx->clientSocket, &address, afNone, 1000000);
  EXPECT_EQ(connectResult, 0);
  if (connectResult == 0) {
    ctx->clientState = 1;
    ssize_t sendResult;
    {
      // short message
      reqStruct req;
      req.a = 11;
      req.b = 77;
      sendResult = ioZmtpSend(ctx->clientSocket, &req, sizeof(req), zmtpMessage, afNone, 1000000);
      EXPECT_EQ(sendResult, static_cast<ssize_t>(sizeof(req)));
      if (sendResult == sizeof(req))
        ctx->clientState++;
    }

    {
      // long message
      auto data = std::unique_ptr<reqStruct[]>(new reqStruct[1024]);
      for (unsigned i = 0; i < 1024; i++) {
        data[i].a = i;
        data[i].b = i;
      }
      sendResult = ioZmtpSend(ctx->clientSocket, data.get(), 1024*sizeof(reqStruct), zmtpMessage, afNone, 1000000);
      EXPECT_EQ(sendResult, static_cast<ssize_t>(1024*sizeof(reqStruct)));
      if (sendResult == 1024*sizeof(reqStruct))
        ctx->clientState++;
    }
  }

  EXPECT_EQ(waitServer(ctx), true);
  zmtpSocketDelete(ctx->clientSocket);
  postQuitOperation(ctx->base);
  ctx->clientRunning = false;
}

TEST(zmtp, aio_push_coro)
{
  zmtpContext context(gBase);
  std::thread serverThread(zmq_server_pull, &context, gPort);

  // Wait server
  EXPECT_EQ(waitServer(&context, true), true);
  context.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPUSH);
  context.clientRunning = true;

  coroutineCall(coroutineNew(aio_push_client_coro, &context, 0x10000));
  asyncLoop(gBase);
  serverThread.join();
  ASSERT_EQ(context.clientState, 3);
  ASSERT_EQ(context.serverState, 3);
}

static void aio_pull_readcb(AsyncOpStatus status, zmtpSocket *socket, zmtpUserMsgTy type, zmtpStream *stream, void *arg)
{
  bool end = true;
  auto ctx = static_cast<zmtpContext*>(arg);
  if (ctx->serverState == 1) {
    // Check short message, read long message
    reqStruct *req = stream->data<reqStruct>();
    EXPECT_EQ(status, aosSuccess);
    EXPECT_EQ(type, zmtpMessage);
    EXPECT_EQ(stream->sizeOf(), sizeof(reqStruct));
    EXPECT_EQ(req->a, 11u);
    EXPECT_EQ(req->b, 77u);
    if (status == aosSuccess &&
        stream->sizeOf() == sizeof(reqStruct) &&
        req->a == 11 &&
        req->b == 77) {
      ctx->serverState = 2;
      aioZmtpRecv(socket, ctx->stream, 65536, afNone, 1000000, aio_pull_readcb, ctx);
      end = false;
    }
  } else if (ctx->serverState == 2) {
    // Check long message, read limited
    bool valid = true;
    reqStruct *req = stream->data<reqStruct>();
    EXPECT_EQ(status, aosSuccess);
    EXPECT_EQ(type, zmtpMessage);
    EXPECT_EQ(stream->sizeOf(), 1024*sizeof(reqStruct));
    for (unsigned i = 0; i < 1024; i++) {
      EXPECT_EQ(req[i].a, i);
      EXPECT_EQ(req[i].b, i);
      if (req[i].a != i || req[i].b != i) {
        valid = false;
        break;
      }
    }

    if (status == aosSuccess && stream->sizeOf() == 1024*sizeof(reqStruct) && valid) {
      ctx->serverState = 3;
      aioZmtpRecv(socket, ctx->stream, 8, afNone, 1000000, aio_pull_readcb, ctx);
      end = false;
    }
  } else if (ctx->serverState == 3) {
    // Check limit
    EXPECT_EQ(status, aosBufferTooSmall);
    if (status == aosBufferTooSmall)
      ctx->serverState = 4;
  }

  if (end) {
    ctx->serverRunning = false;
    EXPECT_EQ(waitClient(ctx), true);
    zmtpSocketDelete(ctx->serverSocket);
    postQuitOperation(ctx->base);
  }
}

static void aio_pull_zmtpacceptcb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status == aosSuccess) {
    ctx->serverState = 1;
    aioZmtpRecv(socket, ctx->stream, 1024, afNone, 1000000, aio_pull_readcb, ctx);
  } else {
    ctx->serverRunning = false;
    EXPECT_EQ(waitClient(ctx), true);
    zmtpSocketDelete(ctx->serverSocket);
    postQuitOperation(ctx->base);
  }
}

static void aio_pull_acceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status == aosSuccess) {
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPULL);
    aioZmtpAccept(ctx->serverSocket, afNone, 1000000, aio_pull_zmtpacceptcb, ctx);
  } else {
    ctx->serverRunning = false;
    postQuitOperation(ctx->base);
  }
}

TEST(zmtp, aio_pull)
{
  zmtpContext context(gBase);
  context.listener = startTCPServer(gBase, aio_pull_acceptcb, &context, gPort);
  context.clientRunning = true;
  context.serverRunning = true;
  ASSERT_NE(context.listener, nullptr);
  std::thread clientThread(zmq_server_push, &context, gPort);
  asyncLoop(gBase);
  clientThread.join();
  deleteAioObject(context.listener);
  ASSERT_EQ(context.clientState, 4);
  ASSERT_EQ(context.serverState, 4);
}

void aio_pull_accept_coro(void *arg)
{
  zmtpSocket *socket = nullptr;
  auto ctx = static_cast<zmtpContext*>(arg);
  socketTy fd;
  int status = ioAccept(ctx->listener, &fd, nullptr, 1000000);
  EXPECT_EQ(status, 0);
  if (status == 0) {
    socket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, fd), zmtpSocketPULL);
    int acceptResult = ioZmtpAccept(socket, afNone, 3000000);
    EXPECT_EQ(acceptResult, 0);
    if (acceptResult == 0) {
      ssize_t recvResult;
      zmtpUserMsgTy msgType;
      ctx->serverState = 1;

      {
        // Short message
        recvResult = ioZmtpRecv(socket, ctx->stream, 1024, afNone, 1000000, &msgType);
        reqStruct *req = ctx->stream.data<reqStruct>();
        EXPECT_EQ(msgType, zmtpMessage);
        EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(reqStruct)));
        EXPECT_EQ(ctx->stream.sizeOf(), sizeof(reqStruct));
        EXPECT_EQ(req->a, 11u);
        EXPECT_EQ(req->b, 77u);
        if (msgType == zmtpMessage &&
            recvResult == sizeof(reqStruct) &&
            ctx->stream.sizeOf() == sizeof(reqStruct) &&
            req->a == 11 &&
            req->b == 77) {
          ctx->serverState++;
        }
      }

      {
        // Long message
        bool valid = true;
        recvResult = ioZmtpRecv(socket, ctx->stream, 65536, afNone, 1000000, &msgType);
        reqStruct *req = ctx->stream.data<reqStruct>();
        EXPECT_EQ(msgType, zmtpMessage);
        EXPECT_EQ(recvResult, static_cast<ssize_t>(1024*sizeof(reqStruct)));
        EXPECT_EQ(ctx->stream.sizeOf(), 1024*sizeof(reqStruct));
        for (unsigned i = 0; i < 1024; i++) {
          EXPECT_EQ(req[i].a, i);
          EXPECT_EQ(req[i].b, i);
          if (req[i].a != i || req[i].b != i) {
            valid = false;
            break;
          }
        }

        if (msgType == zmtpMessage &&
            recvResult == 1024*sizeof(reqStruct) &&
            ctx->stream.sizeOf() == 1024*sizeof(reqStruct) &&
            valid) {
          ctx->serverState++;
        }
      }

      {
        // Small limit
        recvResult = ioZmtpRecv(socket, ctx->stream, 8, afNone, 1000000, &msgType);
        EXPECT_EQ(recvResult, -aosBufferTooSmall);
        if (recvResult == -aosBufferTooSmall)
          ctx->serverState++;
      }
    }
  }

  ctx->serverRunning = false;
  EXPECT_EQ(waitClient(ctx), true);
  if (socket)
    zmtpSocketDelete(socket);
  postQuitOperation(ctx->base);
}

TEST(zmtp, aio_pull_coro)
{
  zmtpContext context(gBase);
  context.listener = startTCPServer(gBase, nullptr, nullptr, gPort);
  context.clientRunning = true;
  context.serverRunning = true;
  ASSERT_NE(context.listener, nullptr);
  std::thread clientThread(zmq_server_push, &context, gPort);
  coroutineCall(coroutineNew(aio_pull_accept_coro, &context, 0x10000));
  asyncLoop(gBase);
  clientThread.join();
  deleteAioObject(context.listener);
  ASSERT_EQ(context.clientState, 4);
  ASSERT_EQ(context.serverState, 4);
}

static void aio_req_writecb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status != aosSuccess) {
    EXPECT_EQ(waitServer(ctx), true);
    zmtpSocketDelete(socket);
    postQuitOperation(ctx->base);
    ctx->clientRunning = false;
  }
}

static void aio_req_readcb(AsyncOpStatus status, zmtpSocket *socket, zmtpUserMsgTy type, zmtpStream *stream, void *arg)
{
  bool end = true;
  auto ctx = static_cast<zmtpContext*>(arg);
  if (ctx->clientState == 1) {
    EXPECT_EQ(status, aosSuccess);
    EXPECT_EQ(type, zmtpMessage);
    EXPECT_EQ(stream->sizeOf(), sizeof(repStruct));
    EXPECT_EQ(stream->data<repStruct>()->c, 88u);
    if (status == aosSuccess &&
        type == zmtpMessage &&
        stream->sizeOf() == sizeof(repStruct) &&
        stream->data<repStruct>()->c == 88) {
      ctx->clientState = 2;
      end = false;

      // send long message
      {
        auto data = std::unique_ptr<reqStruct[]>(new reqStruct[1024]);
        for (unsigned i = 0; i < 1024; i++) {
          data[i].a = i;
          data[i].b = i;
        }
        aioZmtpSend(socket, data.get(), 1024*sizeof(reqStruct), zmtpMessage, afNone, 1000000, aio_req_writecb, ctx);
        aioZmtpRecv(socket, ctx->stream, 65536, afNone, 1000000, aio_req_readcb, ctx);
      }
    }
  } else if (ctx->clientState == 2) {
    // check long response
    bool valid = true;
    repStruct *rep = ctx->stream.data<repStruct>();
    EXPECT_EQ(status, aosSuccess);
    EXPECT_EQ(type, zmtpMessage);
    EXPECT_EQ(stream->sizeOf(), 1024*sizeof(repStruct));
    if (stream->sizeOf() == 1024*sizeof(repStruct)) {
      for (unsigned i = 0; i < 1024; i++) {
        EXPECT_EQ(rep[i].c, i+i);
        if (rep[i].c != i+i) {
          valid = false;
          break;
        }
      }
    } else {
      valid = false;
    }

    if (status == aosSuccess && valid) {
      // send message and receive to buffer with small limit
      reqStruct req;
      req.a = 11;
      req.b = 77;
      aioZmtpSend(socket, &req, sizeof(req), zmtpMessage, afNone, 1000000, aio_req_writecb, ctx);
      aioZmtpRecv(socket, ctx->stream, 4, afNone, 1000000, aio_req_readcb, ctx);
      ctx->clientState = 3;
      end = false;
    }
  } else if (ctx->clientState == 3) {
    EXPECT_EQ(status, aosBufferTooSmall);
    if (status == aosBufferTooSmall)
      ctx->clientState = 4;
  }

  if (end) {
    EXPECT_EQ(waitServer(ctx), true);
    zmtpSocketDelete(ctx->clientSocket);
    postQuitOperation(ctx->base);
    ctx->clientRunning = false;
  }
}

static void aio_req_connectcb(AsyncOpStatus status, zmtpSocket *client, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status == aosSuccess) {
    reqStruct req;
    req.a = 11;
    req.b = 77;
    ctx->clientState = 1;
    // short message
    aioZmtpSend(ctx->clientSocket, &req, sizeof(req), zmtpMessage, afNone, 1000000, aio_req_writecb, ctx);
    aioZmtpRecv(client, ctx->stream, 1024, afNone, 1000000, aio_req_readcb, ctx);
  } else {
    EXPECT_EQ(waitServer(ctx), true);
    zmtpSocketDelete(client);
    postQuitOperation(ctx->base);
    ctx->clientRunning = false;
  }
}

TEST(zmtp, aio_req)
{
  zmtpContext context(gBase);
  std::thread serverThread(zmq_server_rep, &context, gPort);

  // Wait server
  EXPECT_EQ(waitServer(&context, true), true);
  context.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketREQ);
  context.clientRunning = true;

  HostAddress address;
  address.ipv4 = inet_addr("127.0.0.1");
  address.family = AF_INET;
  address.port = gPort;
  aioZmtpConnect(context.clientSocket, &address, afNone, 1000000, aio_req_connectcb, &context);
  asyncLoop(gBase);
  serverThread.join();
  ASSERT_EQ(context.clientState, 4);
  ASSERT_EQ(context.serverState, 4);
}

void aio_req_client_coro(void *arg)
{
  auto ctx = static_cast<zmtpContext*>(arg);

  HostAddress address;
  address.ipv4 = inet_addr("127.0.0.1");
  address.family = AF_INET;
  address.port = gPort;
  int connectResult = ioZmtpConnect(ctx->clientSocket, &address, afNone, 1000000);
  EXPECT_EQ(connectResult, 0);
  if (connectResult == 0) {
    ctx->clientState = 1;
    zmtpUserMsgTy msgType;
    ssize_t recvResult;
    ssize_t sendResult;
    {
      // short message
      reqStruct req;
      req.a = 11;
      req.b = 77;
      sendResult = ioZmtpSend(ctx->clientSocket, &req, sizeof(req), zmtpMessage, afNone, 1000000);
      recvResult = ioZmtpRecv(ctx->clientSocket, ctx->stream, 1024, afNone, 1000000, &msgType);
      repStruct *rep = ctx->stream.data<repStruct>();
      EXPECT_EQ(sendResult, static_cast<ssize_t>(sizeof(req)));
      EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(repStruct)));
      EXPECT_EQ(msgType, zmtpMessage);
      EXPECT_EQ(ctx->stream.sizeOf(), sizeof(repStruct));
      EXPECT_EQ(rep->c, req.a + req.b);
      if (msgType == zmtpMessage &&
          recvResult == sizeof(repStruct) &&
          ctx->stream.sizeOf() == sizeof(repStruct) &&
          rep->c == req.a + req.b) {
        ctx->clientState++;
      }
    }

    {
      // long message
      bool valid = true;
      auto data = std::unique_ptr<reqStruct[]>(new reqStruct[1024]);
      for (unsigned i = 0; i < 1024; i++) {
        data[i].a = i;
        data[i].b = i;
      }
      sendResult = ioZmtpSend(ctx->clientSocket, data.get(), 1024*sizeof(reqStruct), zmtpMessage, afNone, 1000000);
      recvResult = ioZmtpRecv(ctx->clientSocket, ctx->stream, 65536, afNone, 1000000, &msgType);
      repStruct *rep = ctx->stream.data<repStruct>();
      EXPECT_EQ(sendResult, static_cast<ssize_t>(1024*sizeof(reqStruct)));
      EXPECT_EQ(recvResult, static_cast<ssize_t>(1024*sizeof(repStruct)));
      EXPECT_EQ(ctx->stream.sizeOf(), 1024*sizeof(repStruct));
      if (ctx->stream.sizeOf() == 1024*sizeof(repStruct)) {
        for (unsigned i = 0; i < 1024; i++) {
          EXPECT_EQ(rep[i].c, i+i);
          if (rep[i].c != i+i) {
            valid = false;
            break;
          }
        }
      } else {
        valid = false;
      }

      if (valid)
        ctx->clientState++;
    }

    {
      // limit check
      reqStruct req;
      req.a = 99;
      req.b = 99;
      ioZmtpSend(ctx->clientSocket, &req, sizeof(req), zmtpMessage, afNone, 1000000);
      recvResult = ioZmtpRecv(ctx->clientSocket, ctx->stream, 4, afNone, 1000000, &msgType);
      EXPECT_EQ(recvResult, -aosBufferTooSmall);
      if (recvResult == -aosBufferTooSmall)
        ctx->clientState++;
    }
  }

  EXPECT_EQ(waitServer(ctx), true);
  zmtpSocketDelete(ctx->clientSocket);
  postQuitOperation(ctx->base);
  ctx->clientRunning = false;
}

TEST(zmtp, aio_req_coro)
{
  zmtpContext context(gBase);
  std::thread serverThread(zmq_server_rep, &context, gPort);

  // Wait server
  EXPECT_EQ(waitServer(&context, true), true);
  context.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketREQ);
  context.clientRunning = true;

  coroutineCall(coroutineNew(aio_req_client_coro, &context, 0x10000));
  asyncLoop(gBase);
  serverThread.join();
  ASSERT_EQ(context.clientState, 4);
  ASSERT_EQ(context.serverState, 4);
}

static void aio_rep_writecb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status != aosSuccess) {
    ctx->serverRunning = false;
    EXPECT_EQ(waitClient(ctx), true);
    zmtpSocketDelete(socket);
    postQuitOperation(ctx->base);
  }
}

static void aio_rep_readcb(AsyncOpStatus status, zmtpSocket *socket, zmtpUserMsgTy type, zmtpStream *stream, void *arg)
{
  bool end = true;
  auto ctx = static_cast<zmtpContext*>(arg);
  if (ctx->serverState == 1) {
    // Check short message, read long message
    reqStruct *req = stream->data<reqStruct>();
    EXPECT_EQ(status, aosSuccess);
    EXPECT_EQ(type, zmtpMessage);
    EXPECT_EQ(stream->sizeOf(), sizeof(reqStruct));
    EXPECT_EQ(req->a, 11u);
    EXPECT_EQ(req->b, 77u);
    if (status == aosSuccess &&
        stream->sizeOf() == sizeof(reqStruct) &&
        req->a == 11 &&
        req->b == 77) {
      ctx->serverState = 2;
      repStruct rep;
      rep.c = req->a + req->b;
      aioZmtpSend(socket, &rep, sizeof(rep), zmtpMessage, afNone, 1000000, aio_rep_writecb, ctx);
      aioZmtpRecv(socket, ctx->stream, 65536, afNone, 1000000, aio_rep_readcb, ctx);
      end = false;
    }
  } else if (ctx->serverState == 2) {
    // Check long message, read limited
    bool valid = true;
    repStruct rep[1024];
    reqStruct *req = stream->data<reqStruct>();
    EXPECT_EQ(status, aosSuccess);
    EXPECT_EQ(type, zmtpMessage);
    EXPECT_EQ(stream->sizeOf(), 1024*sizeof(reqStruct));
    for (unsigned i = 0; i < 1024; i++) {
      rep[i].c = req[i].a + req[i].b;
      EXPECT_EQ(req[i].a, i);
      EXPECT_EQ(req[i].b, i);
      if (req[i].a != i || req[i].b != i) {
        valid = false;
        break;
      }
    }

    if (status == aosSuccess && stream->sizeOf() == 1024*sizeof(reqStruct) && valid) {
      ctx->serverState = 3;
      aioZmtpSend(socket, rep, sizeof(rep), zmtpMessage, afNone, 1000000, aio_rep_writecb, ctx);
      aioZmtpRecv(socket, ctx->stream, 8, afNone, 1000000, aio_rep_readcb, ctx);
      end = false;
    }
  } else if (ctx->serverState == 3) {
    // Check limit
    EXPECT_EQ(status, aosBufferTooSmall);
    if (status == aosBufferTooSmall)
      ctx->serverState = 4;
  }

  if (end) {
    ctx->serverRunning = false;
    EXPECT_EQ(waitClient(ctx), true);
    zmtpSocketDelete(ctx->serverSocket);
    postQuitOperation(ctx->base);
  }
}

static void aio_rep_zmtpacceptcb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status == aosSuccess) {
    ctx->serverState = 1;
    aioZmtpRecv(socket, ctx->stream, 1024, afNone, 1000000, aio_rep_readcb, ctx);
  } else {
    ctx->serverRunning = false;
    EXPECT_EQ(waitClient(ctx), true);
    zmtpSocketDelete(ctx->serverSocket);
    postQuitOperation(ctx->base);
  }
}


static void aio_rep_acceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  EXPECT_EQ(status, aosSuccess);
  auto ctx = static_cast<zmtpContext*>(arg);
  if (status == aosSuccess) {
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketREP);
    aioZmtpAccept(ctx->serverSocket, afNone, 1000000, aio_rep_zmtpacceptcb, ctx);
  } else {
    ctx->serverRunning = false;
    postQuitOperation(ctx->base);
  }
}

TEST(zmtp, aio_rep)
{
  zmtpContext context(gBase);
  context.listener = startTCPServer(gBase, aio_rep_acceptcb, &context, gPort);
  context.clientRunning = true;
  context.serverRunning = true;
  ASSERT_NE(context.listener, nullptr);
  std::thread clientThread(zmq_server_req, &context, gPort);
  asyncLoop(gBase);
  clientThread.join();
  deleteAioObject(context.listener);
  ASSERT_EQ(context.clientState, 4);
  ASSERT_EQ(context.serverState, 4);
}

void aio_rep_accept_coro(void *arg)
{
  auto ctx = static_cast<zmtpContext*>(arg);
  zmtpSocket *socket = nullptr;
  socketTy fd;
  int status = ioAccept(ctx->listener, &fd, nullptr, 1000000);
  EXPECT_EQ(status, 0);
  if (status == 0) {
    socket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, fd), zmtpSocketREP);
    int acceptResult = ioZmtpAccept(socket, afNone, 3000000);
    EXPECT_EQ(acceptResult, 0);
    if (acceptResult == 0) {
      ssize_t recvResult;
      ssize_t sendResult;
      zmtpUserMsgTy msgType;
      ctx->serverState = 1;

      {
        // Short message
        recvResult = ioZmtpRecv(socket, ctx->stream, 1024, afNone, 1000000, &msgType);
        reqStruct *req = ctx->stream.data<reqStruct>();
        repStruct rep;
        EXPECT_EQ(msgType, zmtpMessage);
        EXPECT_EQ(recvResult, static_cast<ssize_t>(sizeof(reqStruct)));
        EXPECT_EQ(ctx->stream.sizeOf(), sizeof(reqStruct));
        EXPECT_EQ(req->a, 11u);
        EXPECT_EQ(req->b, 77u);
        if (msgType == zmtpMessage &&
            recvResult == sizeof(reqStruct) &&
            ctx->stream.sizeOf() == sizeof(reqStruct) &&
            req->a == 11 &&
            req->b == 77) {
          rep.c = req->a + req->b;
          sendResult = ioZmtpSend(socket, &rep, sizeof(rep), zmtpMessage, afNone, 1000000);
          EXPECT_EQ(sendResult, static_cast<ssize_t>(sizeof(rep)));
          if (sendResult == sizeof(rep))
            ctx->serverState++;
        }
      }

      {
        // Long message
        bool valid = true;
        recvResult = ioZmtpRecv(socket, ctx->stream, 65536, afNone, 1000000, &msgType);
        reqStruct *req = ctx->stream.data<reqStruct>();
        repStruct rep[1024];
        EXPECT_EQ(msgType, zmtpMessage);
        EXPECT_EQ(recvResult, static_cast<ssize_t>(1024*sizeof(reqStruct)));
        EXPECT_EQ(ctx->stream.sizeOf(), 1024*sizeof(reqStruct));
        for (unsigned i = 0; i < 1024; i++) {
          rep[i].c = req[i].a + req[i].b;
          EXPECT_EQ(req[i].a, i);
          EXPECT_EQ(req[i].b, i);
          if (req[i].a != i || req[i].b != i) {
            valid = false;
            break;
          }
        }

        if (msgType == zmtpMessage &&
            recvResult == 1024*sizeof(reqStruct) &&
            ctx->stream.sizeOf() == 1024*sizeof(reqStruct) &&
            valid) {
          sendResult = ioZmtpSend(socket, rep, sizeof(rep), zmtpMessage, afNone, 1000000);
          EXPECT_EQ(sendResult, static_cast<ssize_t>(sizeof(rep)));
          if (sendResult == sizeof(rep))
            ctx->serverState++;
        }
      }

      {
        // Small limit
        recvResult = ioZmtpRecv(socket, ctx->stream, 8, afNone, 1000000, &msgType);
        EXPECT_EQ(recvResult, -aosBufferTooSmall);
        if (recvResult == -aosBufferTooSmall)
          ctx->serverState++;
      }
    }
  }

  ctx->serverRunning = false;
  EXPECT_EQ(waitClient(ctx), true);
  if (socket)
    zmtpSocketDelete(socket);
  postQuitOperation(ctx->base);
}

TEST(zmtp, aio_rep_coro)
{
  zmtpContext context(gBase);
  context.listener = startTCPServer(gBase, nullptr, nullptr, gPort);
  context.clientRunning = true;
  context.serverRunning = true;
  ASSERT_NE(context.listener, nullptr);
  std::thread clientThread(zmq_server_req, &context, gPort);
  coroutineCall(coroutineNew(aio_rep_accept_coro, &context, 0x10000));
  asyncLoop(gBase);
  clientThread.join();
  deleteAioObject(context.listener);
  ASSERT_EQ(context.clientState, 4);
  ASSERT_EQ(context.serverState, 4);
}

// The ZMTP handshake operations (connect and accept) initialize the transport:
// the combiner stores them in initializationOp, and recv/send submitted
// behind them stay frozen in the object queues until the handshake outcome. Without the
// slot the opposite lane is open: connect lives in the write queue, so a
// pipelined recv used to take the inline fast path and read on the plain
// socket, stealing greeting bytes from the handshake state machine (and
// mirrored for send during accept). Both ends run on this library in one
// event loop - the peer must also be driven while the handshake is parked.
struct zmtpPipelineContext {
  asyncBase *base;
  aioObject *listener;
  zmtpSocket *clientSocket;
  zmtpSocket *serverSocket;
  zmtpStream stream;
  reqStruct serverReq;
  socketTy rawServerFd;
  std::thread rawSender;
  AsyncOpStatus connectStatus;
  AsyncOpStatus acceptStatus;
  AsyncOpStatus sendStatus;
  AsyncOpStatus recvStatus;
  int pending;
  zmtpPipelineContext(asyncBase *baseArg) :
    base(baseArg), listener(nullptr), clientSocket(nullptr), serverSocket(nullptr),
    rawServerFd(0),
    connectStatus(aosUnknown), acceptStatus(aosUnknown),
    sendStatus(aosUnknown), recvStatus(aosUnknown), pending(0) {}
  void completed() {
    if (--pending == 0)
      postQuitOperation(base);
  }
};

static void pipeline_sendcb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->sendStatus = status;
  ctx->completed();
}

static void pipeline_recvcb(AsyncOpStatus status, zmtpSocket*, zmtpUserMsgTy, zmtpStream*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->recvStatus = status;
  ctx->completed();
}

static void pipeline_connectcb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->connectStatus = status;
  ctx->completed();
}

static void recv_pipeline_zmtpacceptcb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->acceptStatus = status;
  if (status == aosSuccess) {
    ctx->serverReq.a = 11;
    ctx->serverReq.b = 77;
    aioZmtpSend(socket, &ctx->serverReq, sizeof(ctx->serverReq), zmtpMessage, afNone, 3000000, pipeline_sendcb, ctx);
  } else {
    ctx->completed();  // the send that will never be submitted
  }
  ctx->completed();
}

static void recv_pipeline_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPUSH);
    aioZmtpAccept(ctx->serverSocket, afNone, 3000000, recv_pipeline_zmtpacceptcb, ctx);
  } else {
    ctx->pending -= 2;  // neither accept nor send will be submitted
    ctx->completed();
  }
}

TEST(zmtp, recv_pipelined_with_connect)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 4;  // connect, recv, accept, send
  ctx.listener = startTCPServer(gBase, recv_pipeline_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPULL);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 3000000, pipeline_connectcb, &ctx);
  // pipelined right behind the connect: must wait for the handshake outcome
  // instead of racing it for the greeting bytes
  aioZmtpRecv(ctx.clientSocket, ctx.stream, 1024, afNone, 3000000, pipeline_recvcb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.connectStatus, aosSuccess);
  EXPECT_EQ(ctx.acceptStatus, aosSuccess);
  EXPECT_EQ(ctx.sendStatus, aosSuccess);
  EXPECT_EQ(ctx.recvStatus, aosSuccess);
  if (ctx.recvStatus == aosSuccess) {
    ASSERT_EQ(ctx.stream.sizeOf(), sizeof(reqStruct));
    reqStruct *req = ctx.stream.data<reqStruct>();
    EXPECT_EQ(req->a, 11u);
    EXPECT_EQ(req->b, 77u);
  }

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.serverSocket)
    zmtpSocketDelete(ctx.serverSocket);
  deleteAioObject(ctx.listener);
}

static void send_pipeline_zmtpacceptcb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->acceptStatus = status;
  ctx->completed();
}

static void send_pipeline_connectcb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->connectStatus = status;
  if (status == aosSuccess) {
    aioZmtpRecv(socket, ctx->stream, 1024, afNone, 3000000, pipeline_recvcb, ctx);
  } else {
    ctx->completed();  // the recv that will never be submitted
  }
  ctx->completed();
}

static void send_pipeline_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPUSH);
    ctx->serverReq.a = 11;
    ctx->serverReq.b = 77;
    aioZmtpAccept(ctx->serverSocket, afNone, 3000000, send_pipeline_zmtpacceptcb, ctx);
    // pipelined right behind the accept: the frame must not hit the wire
    // before the greeting exchange completes
    aioZmtpSend(ctx->serverSocket, &ctx->serverReq, sizeof(ctx->serverReq), zmtpMessage, afNone, 3000000, pipeline_sendcb, ctx);
  } else {
    ctx->pending -= 2;  // neither accept nor send will be submitted
    ctx->completed();
  }
}

TEST(zmtp, send_pipelined_with_accept)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 4;  // connect, recv, accept, send
  ctx.listener = startTCPServer(gBase, send_pipeline_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPULL);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 3000000, send_pipeline_connectcb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.connectStatus, aosSuccess);
  EXPECT_EQ(ctx.acceptStatus, aosSuccess);
  EXPECT_EQ(ctx.sendStatus, aosSuccess);
  EXPECT_EQ(ctx.recvStatus, aosSuccess);
  if (ctx.recvStatus == aosSuccess) {
    ASSERT_EQ(ctx.stream.sizeOf(), sizeof(reqStruct));
    reqStruct *req = ctx.stream.data<reqStruct>();
    EXPECT_EQ(req->a, 11u);
    EXPECT_EQ(req->b, 77u);
  }

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.serverSocket)
    zmtpSocketDelete(ctx.serverSocket);
  deleteAioObject(ctx.listener);
}

static void capture_pipeline_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPUSH);
    aioZmtpAccept(ctx->serverSocket, afNone, 3000000, send_pipeline_zmtpacceptcb, ctx);
    // queued behind the accept from a scoped buffer: without afNoCopy the
    // payload must be captured before the call returns
    reqStruct req;
    req.a = 11;
    req.b = 77;
    aioZmtpSend(ctx->serverSocket, &req, sizeof(req), zmtpMessage, afNone, 3000000, pipeline_sendcb, ctx);
    memset(&req, 0xAA, sizeof(req));
  } else {
    ctx->pending -= 2;  // neither accept nor send will be submitted
    ctx->completed();
  }
}

TEST(zmtp, queued_send_captures_payload)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 4;  // connect, recv, accept, send
  ctx.listener = startTCPServer(gBase, capture_pipeline_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPULL);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 3000000, send_pipeline_connectcb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.connectStatus, aosSuccess);
  EXPECT_EQ(ctx.acceptStatus, aosSuccess);
  EXPECT_EQ(ctx.sendStatus, aosSuccess);
  EXPECT_EQ(ctx.recvStatus, aosSuccess);
  if (ctx.recvStatus == aosSuccess) {
    ASSERT_EQ(ctx.stream.sizeOf(), sizeof(reqStruct));
    reqStruct *req = ctx.stream.data<reqStruct>();
    EXPECT_EQ(req->a, 11u);
    EXPECT_EQ(req->b, 77u);
  }

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.serverSocket)
    zmtpSocketDelete(ctx.serverSocket);
  deleteAioObject(ctx.listener);
}

// After its handshake the server writes one frame to the raw fd in two
// chunks split inside the 2-byte short header: the length byte arrives a
// pause later than the flags byte, so the receiver must wait for it instead
// of decoding a stale one.
static void split_header_zmtpacceptcb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->acceptStatus = status;
  if (status == aosSuccess) {
    ctx->rawSender = std::thread([ctx]() {
      uint8_t frame[2 + sizeof(reqStruct)];
      reqStruct req;
      req.a = 11;
      req.b = 77;
      frame[0] = 0;
      frame[1] = sizeof(req);
      memcpy(frame + 2, &req, sizeof(req));
      send(ctx->rawServerFd, reinterpret_cast<const char*>(frame), 1, 0);
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      send(ctx->rawServerFd, reinterpret_cast<const char*>(frame) + 1, sizeof(frame) - 1, 0);
    });
  }
  ctx->completed();
}

static void split_header_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->rawServerFd = acceptSocket;
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPUSH);
    aioZmtpAccept(ctx->serverSocket, afNone, 3000000, split_header_zmtpacceptcb, ctx);
  } else {
    ctx->pending -= 1;  // the accept that will never be submitted
    ctx->completed();
  }
}

TEST(zmtp, recv_split_frame_header)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 3;  // connect, recv, accept
  ctx.listener = startTCPServer(gBase, split_header_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPULL);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 3000000, send_pipeline_connectcb, &ctx);

  asyncLoop(gBase);
  if (ctx.rawSender.joinable())
    ctx.rawSender.join();

  EXPECT_EQ(ctx.connectStatus, aosSuccess);
  EXPECT_EQ(ctx.acceptStatus, aosSuccess);
  EXPECT_EQ(ctx.recvStatus, aosSuccess);
  if (ctx.recvStatus == aosSuccess) {
    ASSERT_EQ(ctx.stream.sizeOf(), sizeof(reqStruct));
    reqStruct *req = ctx.stream.data<reqStruct>();
    EXPECT_EQ(req->a, 11u);
    EXPECT_EQ(req->b, 77u);
  }

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.serverSocket)
    zmtpSocketDelete(ctx.serverSocket);
  deleteAioObject(ctx.listener);
}

// 6+6 multipart via the raw fd: each frame alone fits the receive limit of 8,
// the accumulated message must not
static void multipart_limit_zmtpacceptcb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->acceptStatus = status;
  if (status == aosSuccess) {
    uint8_t frames[16];
    frames[0] = 1;  // MORE
    frames[1] = 6;
    memset(frames + 2, 'A', 6);
    frames[8] = 0;  // final
    frames[9] = 6;
    memset(frames + 10, 'B', 6);
    send(ctx->rawServerFd, reinterpret_cast<const char*>(frames), sizeof(frames), 0);
  }
  ctx->completed();
}

static void multipart_limit_connectcb(AsyncOpStatus status, zmtpSocket *socket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->connectStatus = status;
  if (status == aosSuccess) {
    aioZmtpRecv(socket, ctx->stream, 8, afNone, 3000000, pipeline_recvcb, ctx);
  } else {
    ctx->completed();  // the recv that will never be submitted
  }
  ctx->completed();
}

static void multipart_limit_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->rawServerFd = acceptSocket;
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPUSH);
    aioZmtpAccept(ctx->serverSocket, afNone, 3000000, multipart_limit_zmtpacceptcb, ctx);
  } else {
    ctx->pending -= 1;  // the accept that will never be submitted
    ctx->completed();
  }
}

TEST(zmtp, recv_limit_caps_whole_message)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 3;  // connect, recv, accept
  ctx.listener = startTCPServer(gBase, multipart_limit_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPULL);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 3000000, multipart_limit_connectcb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.connectStatus, aosSuccess);
  EXPECT_EQ(ctx.acceptStatus, aosSuccess);
  EXPECT_EQ(ctx.recvStatus, aosBufferTooSmall);

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.serverSocket)
    zmtpSocketDelete(ctx.serverSocket);
  deleteAioObject(ctx.listener);
}

// ZMTP 3.0 mandates peer socket-type validation: PUSH pairs only with PULL,
// so a PUSH client must be rejected by a PUSH server on the accept side
static void incompat_zmtpacceptcb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->acceptStatus = status;
  ctx->completed();
}

static void incompat_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->serverSocket = zmtpSocketNew(ctx->base, newSocketIo(ctx->base, acceptSocket), zmtpSocketPUSH);
    aioZmtpAccept(ctx->serverSocket, afNone, 300000, incompat_zmtpacceptcb, ctx);
  } else {
    ctx->pending -= 1;  // the accept that will never be submitted
    ctx->completed();
  }
}

static void incompat_connectcb(AsyncOpStatus status, zmtpSocket*, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  ctx->connectStatus = status;
  ctx->completed();
}

TEST(zmtp, accept_rejects_incompatible_peer)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 2;  // connect, accept
  ctx.listener = startTCPServer(gBase, incompat_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPUSH);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 300000, incompat_connectcb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.acceptStatus, aosUnknownError);
  EXPECT_NE(ctx.connectStatus, aosSuccess);

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.serverSocket)
    zmtpSocketDelete(ctx.serverSocket);
  deleteAioObject(ctx.listener);
}

// Scripted greeting plus READY(PUSH) written to the raw fd in one shot: the
// connect side itself must parse the READY and reject the incompatible peer
static void scripted_ready_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->rawServerFd = acceptSocket;
    static const uint8_t script[] = {
      0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F,                                    // signature
      3,                                                                     // major
      0,                                                                     // minor
      'N', 'U', 'L', 'L', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   // mechanism
      0,                                                                     // as-server
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,                       // filler
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0x04, 0x1A,                                                            // command frame
      5, 'R', 'E', 'A', 'D', 'Y',
      11, 'S', 'o', 'c', 'k', 'e', 't', '-', 'T', 'y', 'p', 'e',
      0, 0, 0, 4, 'P', 'U', 'S', 'H'
    };
    static_assert(sizeof(script) == 10 + 1 + 53 + 2 + 26, "greeting+READY layout");
    send(acceptSocket, reinterpret_cast<const char*>(script), sizeof(script), 0);
  }
  ctx->completed();
}

// The same scripted handshake, but the READY payload sits in a plain data
// frame (no COMMAND flag) with a compatible Socket-Type: the connect side
// must reject it by the frame type alone
static void data_frame_ready_tcpacceptcb(AsyncOpStatus status, aioObject*, HostAddress, socketTy acceptSocket, void *arg)
{
  auto ctx = static_cast<zmtpPipelineContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status == aosSuccess) {
    ctx->rawServerFd = acceptSocket;
    static const uint8_t script[] = {
      0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0x7F,                                    // signature
      3,                                                                     // major
      0,                                                                     // minor
      'N', 'U', 'L', 'L', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,   // mechanism
      0,                                                                     // as-server
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,                       // filler
      0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      0x00, 0x1A,                                                            // data frame
      5, 'R', 'E', 'A', 'D', 'Y',
      11, 'S', 'o', 'c', 'k', 'e', 't', '-', 'T', 'y', 'p', 'e',
      0, 0, 0, 4, 'P', 'U', 'L', 'L'
    };
    static_assert(sizeof(script) == 10 + 1 + 53 + 2 + 26, "greeting+READY layout");
    send(acceptSocket, reinterpret_cast<const char*>(script), sizeof(script), 0);
  }
  ctx->completed();
}

TEST(zmtp, connect_rejects_ready_without_command_flag)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 2;  // connect, raw accept
  ctx.listener = startTCPServer(gBase, data_frame_ready_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPUSH);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 3000000, incompat_connectcb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.connectStatus, aosUnknownError);

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.rawServerFd)
    socketClose(ctx.rawServerFd);
  deleteAioObject(ctx.listener);
}

TEST(zmtp, connect_rejects_incompatible_ready)
{
  zmtpPipelineContext ctx(gBase);
  ctx.pending = 2;  // connect, raw accept
  ctx.listener = startTCPServer(gBase, scripted_ready_tcpacceptcb, &ctx, gPort);
  ASSERT_NE(ctx.listener, nullptr);
  ctx.clientSocket = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPUSH);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("127.0.0.1");
  address.port = gPort;
  aioZmtpConnect(ctx.clientSocket, &address, afNone, 3000000, incompat_connectcb, &ctx);

  asyncLoop(gBase);

  EXPECT_EQ(ctx.connectStatus, aosUnknownError);

  zmtpSocketDelete(ctx.clientSocket);
  if (ctx.rawServerFd)
    socketClose(ctx.rawServerFd);
  deleteAioObject(ctx.listener);
}

// Same contract as connect.double_connect_rejected on the plain socket: a
// second handshake operation on a zmtp socket must fail immediately while
// the first one is still in flight.
TEST(zmtp, double_connect_rejected)
{
  DoubleConnectRecorder ctx(gBase);
  zmtpSocket *client = zmtpSocketNew(gBase, initializeTCPClient(gBase, nullptr, nullptr, 0), zmtpSocketPUSH);

  HostAddress address;
  address.family = AF_INET;
  address.ipv4 = inet_addr("192.0.2.1");
  address.port = 9;
  aioZmtpConnect(client, &address, afNone, 150000, doubleConnectFirstCb<zmtpSocket>, &ctx);
  aioZmtpConnect(client, &address, afNone, 150000, doubleConnectSecondCb<zmtpSocket>, &ctx);

  asyncLoop(gBase);
  zmtpSocketDelete(client);

  if (ctx.firstStatus != aosTimeout)
    GTEST_SKIP() << "blackhole answered (first connect status " << ctx.firstStatus
                 << "), initialization slot contention cannot be exercised on this network";
  EXPECT_EQ(ctx.secondStatus, aosUnknownError);
  EXPECT_LT(ctx.secondOrder, ctx.firstOrder)
    << "the second zmtp connect was not rejected while the first was in flight";
}
