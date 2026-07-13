#include "asyncioextras/zmtp.h"
#include "asyncio/coroutine.h"

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;
static ConcurrentQueue objectPool;

enum zmtpMsgTy {
  zmtpMsgFlagNone,
  zmtpMsgFlagMore = 1,
  zmtpMsgFlagLong = 2,
  zmtpMsgFlagCommand = 4
};

struct Context {
  aioExecuteProc *StartProc;
  aioFinishProc *FinishProc;
  zmtpStream *Stream;
  void *Buffer;
  size_t TransactionSize;
  size_t BytesTransferred;
  zmtpUserMsgTy UserMsgType;
  zmtpMsgTy MsgType;
  ssize_t Result;
  Context(aioExecuteProc *startProc,
          aioFinishProc *finishProc,
          zmtpStream *stream,
          void *buffer,
          size_t transactionSize,
          zmtpUserMsgTy userMsgType) :

    StartProc(startProc),
    FinishProc(finishProc),
    Stream(stream),
    Buffer(buffer),
    TransactionSize(transactionSize),
    BytesTransferred(0),
    UserMsgType(userMsgType),
    MsgType(zmtpMsgFlagNone),
    Result(-aosPending) {}
};

static uint8_t localSignature[] = {0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x7F};
static uint8_t localMajorVersion = 3;
static uint8_t localGreetingOther[] = {
  0,
  'N', 'U', 'L', 'L', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
  0,
  0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
};



static const char *socketTypeNames[] = {
  "REQ",
  "REP",
  "DEALER",
  "ROUTER",
  "PUB",
  "XPUB",
  "SUB",
  "XSUB",
  "PUSH",
  "PULL",
  "PAIR"
};

static inline zmtpMsgTy operator|(zmtpMsgTy a, zmtpMsgTy b) {
  return static_cast<zmtpMsgTy>(static_cast<int>(a) | static_cast<int>(b));
}

enum btcOpTy {
  zmtpOpAccept = OPCODE_READ,
  zmtpOpRecv,
  zmtpOpConnect = OPCODE_WRITE,
  zmtpOpSend
};

enum zmtpOpState {
  stInitialize = 0,
  stAcceptWriteLocalSignature,
  stAcceptReadMajorVersion,
  stAcceptWriteLocalMajorVersion,
  stAcceptReadMinorVersion,
  stAcceptWriteLocalGreeting,
  stAcceptReadReadyMsg,
  stAcceptWriteReadyMsg,
  stAcceptWriteReadyMsgWaiting,

  stConnectWriteLocalSignature,
  stConnectReadSignature,
  stConnectWriteLocalMajorVersion,
  stConnectReadMajorVersion,
  stConnectWriteLocalGreeting,
  stConnectReadGreeting,
  stConnectWriteReadyMsg,
  stConnectReadReadyMsg,
  stConnectReadReadyMsgWaiting,

  stRecvReadType,
  stRecvReadSize,
  stRecvReadData,

  stWriteSize,
  stWriteData,

  stFinished
};

__NO_PADDING_BEGIN
struct zmtpSocket {
  aioObjectRoot root;
  aioObject *plainSocket;
  zmtpSocketTy type;
  bool needSendMore;
  uint8_t buffer[256];
};

struct zmtpOp {
  asyncOpRoot root;
  HostAddress address;
  zmtpOpState state;
  zmtpOpState stateRw;
  zmtpStream *stream;
  zmtpMsgTy type;
  void *data;
  size_t size;
  size_t transferred;
};
__NO_PADDING_END

static AsyncOpStatus startZmtpRecv(asyncOpRoot *opptr);

static void resumeConnectCb(AsyncOpStatus status, aioObject*, void *arg)
{
  resumeParent(static_cast<asyncOpRoot*>(arg), status);
}

static void resumeRwCb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  resumeParent(static_cast<asyncOpRoot*>(arg), status);
}

static int cancel(asyncOpRoot *opptr)
{
  zmtpSocket *socket = reinterpret_cast<zmtpSocket*>(opptr->object);
  cancelIo(reinterpret_cast<aioObjectRoot*>(socket->plainSocket));
  return 0;
}

static void acceptFinish(asyncOpRoot *opptr)
{
  reinterpret_cast<zmtpAcceptCb*>(opptr->callback)(opGetStatus(opptr),
                                                   reinterpret_cast<zmtpSocket*>(opptr->object),
                                                   opptr->arg);
}

static void connectFinish(asyncOpRoot *opptr)
{
  reinterpret_cast<zmtpConnectCb*>(opptr->callback)(opGetStatus(opptr),
                                                    reinterpret_cast<zmtpSocket*>(opptr->object),
                                                    opptr->arg);
}

static void recvFinish(asyncOpRoot *opptr)
{
  zmtpOp *op = reinterpret_cast<zmtpOp*>(opptr);
  zmtpUserMsgTy type = op->type & zmtpMsgFlagCommand ? zmtpCommand : zmtpMessage;
  reinterpret_cast<zmtpRecvCb*>(opptr->callback)(opGetStatus(opptr),
                                                 reinterpret_cast<zmtpSocket*>(opptr->object),
                                                 type,
                                                 op->stream,
                                                 opptr->arg);
}

static void sendFinish(asyncOpRoot *opptr)
{
  reinterpret_cast<zmtpSendCb*>(opptr->callback)(opGetStatus(opptr),
                                                 reinterpret_cast<zmtpSocket*>(opptr->object),
                                                 opptr->arg);
}

static void releaseOp(asyncOpRoot*)
{
}

static asyncOpRoot *newAsyncOp(aioObjectRoot *object,
                               AsyncFlags flags,
                               uint64_t usTimeout,
                               void *callback,
                               void *arg,
                               int opCode,
                               void *contextPtr)
{
  zmtpOp *op = 0;
  struct Context *context = (struct Context*)contextPtr;
  if (asyncOpAlloc(object->base, sizeof(zmtpOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseOp, object, callback, arg, flags, opCode, usTimeout);
  op->state = stInitialize;
  op->stateRw = stInitialize;
  return &op->root;
}

static asyncOpRoot *newReadAsyncOp(aioObjectRoot *object,
                                   AsyncFlags flags,
                                   uint64_t usTimeout,
                                   void *callback,
                                   void *arg,
                                   int opCode,
                                   void *contextPtr)
{
  zmtpOp *op = 0;
  struct Context *context = (struct Context*)contextPtr;
  if (asyncOpAlloc(object->base, sizeof(zmtpOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseOp, object, callback, arg, flags, opCode, usTimeout);
  op->state = stInitialize;
  op->stateRw = stInitialize;
  op->data = nullptr;
  op->stream = context->Stream;
  op->size = context->TransactionSize;
  return &op->root;
}

static asyncOpRoot *newWriteAsyncOp(aioObjectRoot *object,
                                    AsyncFlags flags,
                                    uint64_t usTimeout,
                                    void *callback,
                                    void *arg,
                                    int opCode,
                                    void *contextPtr)
{
  zmtpOp *op = 0;
  struct Context *context = (struct Context*)contextPtr;
  if (asyncOpAlloc(object->base, sizeof(zmtpOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseOp, object, callback, arg, flags, opCode, usTimeout);
  op->state = stInitialize;
  op->stateRw = stInitialize;
  op->data = context->Buffer;
  op->stream = nullptr;
  op->size = context->TransactionSize;
  switch (context->UserMsgType) {
    case zmtpCommand :
      op->type = (context->TransactionSize < 256) ? zmtpMsgFlagCommand : zmtpMsgFlagCommand | zmtpMsgFlagLong;
      break;
    case zmtpMessagePart :
      op->type = (context->TransactionSize < 256) ? zmtpMsgFlagMore : zmtpMsgFlagMore | zmtpMsgFlagLong;
      break;
    case zmtpMessage :
    default :
      op->type = (context->TransactionSize < 256) ? zmtpMsgFlagNone : zmtpMsgFlagLong;
      break;
  }
  return &op->root;
}

static AsyncOpStatus startZmtpAccept(asyncOpRoot *opptr)
{
  zmtpOp *op = reinterpret_cast<zmtpOp*>(opptr);
  zmtpSocket *socket = reinterpret_cast<zmtpSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  while (!childOp) {
    switch (op->state) {
      case stInitialize : {
        op->state = stAcceptWriteLocalSignature;
        childOp = implRead(socket->plainSocket, socket->buffer, 10, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptWriteLocalSignature : {
        if (socket->buffer[0] != 0xFF || socket->buffer[9] != 0x7F)
          return aosUnknownError;
        op->state = stAcceptReadMajorVersion;
        childOp = implWrite(socket->plainSocket, localSignature, sizeof(localSignature), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptReadMajorVersion : {
        op->state = stAcceptWriteLocalMajorVersion;
        childOp = implRead(socket->plainSocket, socket->buffer, 1, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptWriteLocalMajorVersion : {
        op->state = stAcceptReadMinorVersion;
        childOp = implWrite(socket->plainSocket, &localMajorVersion, 1, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptReadMinorVersion : {
        op->state = stAcceptWriteLocalGreeting;
        childOp = implRead(socket->plainSocket, socket->buffer, 1+20+1+31, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptWriteLocalGreeting : {
        op->state = stAcceptReadReadyMsg;
        childOp = implWrite(socket->plainSocket, localGreetingOther, sizeof(localGreetingOther), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptReadReadyMsg : {
        op->state = stAcceptWriteReadyMsgWaiting;
        op->stateRw = stInitialize;
        op->stream = nullptr;
        op->data = socket->buffer;
        op->size = sizeof(socket->buffer);
        break;
      }

      case stAcceptWriteReadyMsgWaiting : {
        AsyncOpStatus result = startZmtpRecv(&op->root);
        if (result != aosSuccess)
          return result;

        // TODO: check message
        RawData rawSocketType;
        RawData rawIdentity;
        zmtpStream stream(socket->buffer, op->transferred);
        if (!stream.readReadyCmd(&rawSocketType, &rawIdentity))
          return aosUnknownError;

        op->state = stAcceptWriteReadyMsg;
        stream.reset();
        stream.write<uint16_t>(0);
        stream.writeReadyCmd(socketTypeNames[socket->type], nullptr);
        stream.data<uint8_t>()[0] = zmtpMsgFlagCommand;
        stream.data<uint8_t>()[1] = static_cast<uint8_t>(stream.offsetOf() - 2);
        childOp = implWrite(socket->plainSocket, stream.data(), stream.offsetOf(), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptWriteReadyMsg : {
        return aosSuccess;
      }

      default :
        return aosUnknownError;
    }
  }

  combinerPushOperation(childOp);
  return aosPending;
}

static AsyncOpStatus startZmtpConnect(asyncOpRoot *opptr)
{
  zmtpOp *op = reinterpret_cast<zmtpOp*>(opptr);
  zmtpSocket *socket = reinterpret_cast<zmtpSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  while (!childOp) {
    switch (op->state) {
      case stInitialize : {
        op->state = stConnectWriteLocalSignature;
        aioConnect(socket->plainSocket, &op->address, 0, resumeConnectCb, op);
        return aosPending;
      }
      case stConnectWriteLocalSignature : {
        op->state = stConnectReadSignature;
        childOp = implWrite(socket->plainSocket, localSignature, sizeof(localSignature), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectReadSignature : {
        op->state = stConnectWriteLocalMajorVersion;
        childOp = implRead(socket->plainSocket, socket->buffer, 10, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectWriteLocalMajorVersion : {
        op->state = stConnectReadMajorVersion;
        childOp = implWrite(socket->plainSocket, &localMajorVersion, sizeof(localMajorVersion), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectReadMajorVersion : {
        op->state = stConnectWriteLocalGreeting;
        childOp = implRead(socket->plainSocket, socket->buffer, 1, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectWriteLocalGreeting : {
        op->state = stConnectReadGreeting;
        childOp = implWrite(socket->plainSocket, localGreetingOther, sizeof(localGreetingOther), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectReadGreeting : {
        op->state = stConnectWriteReadyMsg;
        childOp = implRead(socket->plainSocket, socket->buffer, 1+20+1+31, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectWriteReadyMsg : {
        op->state = stConnectReadReadyMsg;
        zmtpStream stream(socket->buffer, sizeof(socket->buffer));
        stream.reset();
        stream.write<uint16_t>(0);
        stream.writeReadyCmd(socketTypeNames[socket->type], "");
        stream.data<uint8_t>()[0] = zmtpMsgFlagCommand;
        stream.data<uint8_t>()[1] = static_cast<uint8_t>(stream.offsetOf() - 2);
        childOp = implWrite(socket->plainSocket, stream.data(), stream.offsetOf(), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectReadReadyMsg : {
        op->state = stConnectReadReadyMsgWaiting;
        op->stateRw = stInitialize;
        op->stream = nullptr;
        op->data = socket->buffer;
        op->size = sizeof(socket->buffer);
        break;
      }

      case stConnectReadReadyMsgWaiting : {
        AsyncOpStatus result = startZmtpRecv(&op->root);
        if (result != aosSuccess)
          return result;

        // TODO: check message
        return aosSuccess;
      }

      default :
        return aosUnknownError;
    }
  }

  combinerPushOperation(childOp);
  return aosPending;
}

static AsyncOpStatus startZmtpRecv(asyncOpRoot *opptr)
{
  zmtpOp *op = reinterpret_cast<zmtpOp*>(opptr);
  zmtpSocket *socket = reinterpret_cast<zmtpSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  while (!childOp) {
    switch (op->stateRw) {
      case stInitialize : {
        op->stateRw = stRecvReadType;
        if (op->stream)
          op->stream->reset();
        break;
      }
      case stRecvReadType : {
        op->stateRw = stRecvReadSize;
        childOp = implRead(socket->plainSocket, socket->buffer, 2, afNone, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stRecvReadSize : {
        op->stateRw = stRecvReadData;
        op->type = static_cast<zmtpMsgTy>(socket->buffer[0]);
        if (op->type & zmtpMsgFlagLong)
          childOp = implRead(socket->plainSocket, socket->buffer+2, 7, afNone, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stRecvReadData : {
        op->stateRw = (op->type & zmtpMsgFlagMore) ? stRecvReadType : stFinished;
        if (!(op->type & zmtpMsgFlagLong))
          op->transferred = static_cast<size_t>(socket->buffer[1]);
        else
          op->transferred = static_cast<size_t>(xntoh<uint64_t>(reinterpret_cast<uint64_t*>(socket->buffer+1)[0]));

        if (op->transferred <= op->size)
          childOp = implRead(socket->plainSocket, op->stream ? op->stream->reserve(op->transferred) : op->data, op->transferred, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        else
          return aosBufferTooSmall;
        break;
      }

      case stFinished : {
        if (op->stream)
          op->stream->seekSet(0);
        return aosSuccess;
      }

      default :
        return aosUnknownError;
    }
  }

  combinerPushOperation(childOp);
  return aosPending;
}

static asyncOpRoot *implZmtpRecvStream(zmtpSocket *socket, zmtpStream &msg, size_t limit, AsyncFlags flags, uint64_t timeout, zmtpRecvCb callback, void *arg, size_t *bytesRead, zmtpMsgTy *msgRead)
{
  size_t bytes;
  asyncOpRoot *childOp = nullptr;
  zmtpMsgTy type = zmtpMsgFlagNone;
  zmtpOpState state = stInitialize;
  size_t transferred = 0;

  bool finish = false;
  msg.reset();
  while (!finish) {
    if ( (childOp = implRead(socket->plainSocket, socket->buffer, 2, afNone, 0, resumeRwCb, nullptr, &bytes)) ) {
      state = stRecvReadSize;
      break;
    }

    type = static_cast<zmtpMsgTy>(socket->buffer[0]);
    if (type & zmtpMsgFlagLong) {
      childOp = implRead(socket->plainSocket, socket->buffer+2, 7, afNone, 0, resumeRwCb, nullptr, &bytes);
      if (childOp) {
        state = stRecvReadData;
        break;
      }
    }

    if (!(type & zmtpMsgFlagLong))
      transferred = static_cast<size_t>(socket->buffer[1]);
    else
      transferred = static_cast<size_t>(xntoh<uint64_t>(reinterpret_cast<uint64_t*>(socket->buffer+1)[0]));

    if (transferred <= limit) {
      if ( (childOp = implRead(socket->plainSocket, msg.reserve(transferred), transferred, afWaitAll, 0, resumeRwCb, nullptr, &bytes)) ) {
        state = (type & zmtpMsgFlagMore) ? stRecvReadType : stFinished;
        break;
      }
    } else {
      Context context(startZmtpRecv, recvFinish, &msg, nullptr, limit, zmtpUnknown);
      asyncOpRoot *op = newReadAsyncOp(&socket->root, flags, 0, reinterpret_cast<void*>(callback), arg, zmtpOpRecv, &context);
      opForceStatus(op, aosBufferTooSmall);
      return op;
    }

    finish = (type & zmtpMsgFlagMore) ? false : true;
  }

  if (childOp) {
    Context context(startZmtpRecv, recvFinish, &msg, nullptr, limit, zmtpUnknown);
    zmtpOp *op = reinterpret_cast<zmtpOp*>(newReadAsyncOp(&socket->root, flags | afRunning, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpRecv, &context));
    op->stateRw = state;
    op->type = type;
    op->transferred = transferred;
    childOp->arg = op;
    combinerPushOperation(childOp);
    return &op->root;
  }

  msg.seekSet(0);
  *msgRead = type;
  *bytesRead = transferred;
  return nullptr;
}

static asyncOpRoot *implZmtpRecvStreamProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  return implZmtpRecvStream(reinterpret_cast<zmtpSocket*>(object), *context->Stream, context->TransactionSize, flags, usTimeout, reinterpret_cast<zmtpRecvCb*>(callback), arg, &context->BytesTransferred, &context->MsgType);
}

static AsyncOpStatus startZmtpSend(asyncOpRoot *opptr)
{
  zmtpOp *op = reinterpret_cast<zmtpOp*>(opptr);
  zmtpSocket *socket = reinterpret_cast<zmtpSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  unsigned offset = 0;
  while (!childOp) {
    switch (op->stateRw) {
      case stInitialize : {
        op->stateRw = stWriteSize;
        if (!(op->type & zmtpMsgFlagCommand) &&
            (socket->type == zmtpSocketREQ || socket->type == zmtpSocketREP) && !socket->needSendMore) {
          socket->buffer[0] = zmtpMsgFlagMore;
          socket->buffer[1] = 0;
          offset = 2;
        }

        socket->needSendMore = (op->type & zmtpMsgFlagMore);
        break;
      }
      case stWriteSize : {
        op->stateRw = stWriteData;
        if (!(op->type & zmtpMsgFlagLong)) {
          socket->buffer[offset] = static_cast<uint8_t>(op->type);
          socket->buffer[offset+1] = static_cast<uint8_t>(op->size);
          offset += 2;
        } else {
          socket->buffer[offset] = static_cast<uint8_t>(op->type);
          reinterpret_cast<uint64_t*>(socket->buffer+offset+1)[0] = xhton<uint64_t>(op->size);
          offset += 9;
        }

        if (op->size <= (sizeof(socket->buffer) - offset)) {
          op->stateRw = stFinished;
          memcpy(socket->buffer+offset, op->data, op->size);
          childOp = implWrite(socket->plainSocket, socket->buffer, op->size+offset, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        } else {
          op->stateRw = stWriteData;
          childOp = implWrite(socket->plainSocket, socket->buffer, offset, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        }

        break;
      }

      case stWriteData : {
        op->stateRw = stFinished;
        childOp = implWrite(socket->plainSocket, op->data, op->size, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stFinished :
        return aosSuccess;

      default :
        return aosUnknownError;
    }
  }

  combinerPushOperation(childOp);
  return aosPending;
}

asyncOpRoot *implZmtpSend(zmtpSocket *socket, void *data, size_t size, zmtpUserMsgTy type, AsyncFlags flags, uint64_t timeout, zmtpSendCb callback, void *arg, size_t *bytesTransferred)
{
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  zmtpOpState state = stInitialize;
  unsigned offset = 0;
  zmtpMsgTy msgType;

  switch (type) {
    case zmtpCommand :
      msgType = (size < 256) ? zmtpMsgFlagCommand : zmtpMsgFlagCommand | zmtpMsgFlagLong;
      break;
    case zmtpMessagePart :
      msgType = (size < 256) ? zmtpMsgFlagMore : zmtpMsgFlagMore | zmtpMsgFlagLong;
      break;
    case zmtpMessage :
    default :
      msgType = (size < 256) ? zmtpMsgFlagNone : zmtpMsgFlagLong;
      break;
  }

  if (!(msgType & zmtpMsgFlagCommand) &&
      (socket->type == zmtpSocketREQ || socket->type == zmtpSocketREP) && !socket->needSendMore) {
    socket->buffer[0] = zmtpMsgFlagMore;
    socket->buffer[1] = 0;
    offset = 2;
  }

  socket->needSendMore = (msgType & zmtpMsgFlagMore);

  if (!(msgType & zmtpMsgFlagLong)) {
    socket->buffer[offset] = static_cast<uint8_t>(msgType);
    socket->buffer[offset+1] = static_cast<uint8_t>(size);
    offset += 2;
  } else {
    socket->buffer[offset] = static_cast<uint8_t>(msgType);
    reinterpret_cast<uint64_t*>(socket->buffer+offset+1)[0] = xhton<uint64_t>(size);
    offset += 9;
  }

  if (size <= (sizeof(socket->buffer) - offset)) {
    memcpy(socket->buffer+offset, data, size);
    childOp = implWrite(socket->plainSocket, socket->buffer, size+offset, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
    state = stFinished;
  } else {
    childOp = implWrite(socket->plainSocket, socket->buffer, offset, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
    if (!childOp) {
      childOp = implWrite(socket->plainSocket, data, size, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
      state = stFinished;
    } else {
      state = stWriteData;
    }
  }

  if (childOp) {
    Context context(startZmtpSend, sendFinish, nullptr, data, size, type);
    zmtpOp *op = reinterpret_cast<zmtpOp*>(newWriteAsyncOp(&socket->root, flags | afRunning, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpSend, &context));
    op->stateRw = state;
    op->type = msgType;
    childOp->arg = op;
    combinerPushOperation(childOp);
    return &op->root;
  }

  *bytesTransferred = size;
  return nullptr;
}

static asyncOpRoot *implZmtpSendProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  return implZmtpSend(reinterpret_cast<zmtpSocket*>(object), context->Buffer, context->TransactionSize, context->UserMsgType, flags, usTimeout, reinterpret_cast<zmtpSendCb*>(callback), arg, &context->BytesTransferred);
}

void zmtpSocketDestructor(aioObjectRoot *object)
{
  deleteAioObject(reinterpret_cast<zmtpSocket*>(object)->plainSocket);
  objectFree(&objectPool, object, sizeof(zmtpSocket));
}

zmtpSocket *zmtpSocketNew(asyncBase *base, aioObject *plainSocket, zmtpSocketTy type)
{
  if (!plainSocket)
    return nullptr;
  zmtpSocket *socket = static_cast<zmtpSocket*>(
    objectAlloc(&objectPool, sizeof(zmtpSocket), 16));
  if (!socket)
    return nullptr;

  initObjectRoot(&socket->root, base, ioObjectUserDefined, zmtpSocketDestructor);
  socket->plainSocket = plainSocket;
  socket->type = type;
  socket->needSendMore = false;
  setSocketBuffer(plainSocket, 256);
  return socket;
}

void zmtpSocketDelete(zmtpSocket *socket)
{
  objectDelete(&socket->root);
}

void aioZmtpAccept(zmtpSocket *socket, AsyncFlags flags, uint64_t timeout, zmtpAcceptCb callback, void *arg)
{
  Context context(startZmtpAccept, acceptFinish, nullptr, nullptr, 0, zmtpUnknown);
  asyncOpRoot *op = newAsyncOp(&socket->root, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpAccept, &context);
  if (!__uintptr_atomic_compare_and_swap(&socket->root.initializationOp,
                                         0,
                                         reinterpret_cast<uintptr_t>(op),
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(op, aosUnknownError);
    addToGlobalQueue(op);
    return;
  }

  combinerPushOperation(op);
}

void aioZmtpConnect(zmtpSocket *socket, const HostAddress *address, AsyncFlags flags, uint64_t timeout, zmtpConnectCb callback, void *arg)
{
  Context context(startZmtpConnect, connectFinish, nullptr, nullptr, 0, zmtpUnknown);
  zmtpOp *op =
    reinterpret_cast<zmtpOp*>(newAsyncOp(&socket->root, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpConnect, &context));
  op->address = *address;
  if (!__uintptr_atomic_compare_and_swap(&socket->root.initializationOp,
                                         0,
                                         reinterpret_cast<uintptr_t>(&op->root),
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(&op->root, aosUnknownError);
    addToGlobalQueue(&op->root);
    return;
  }

  combinerPushOperation(&op->root);
}

ssize_t aioZmtpRecv(zmtpSocket *socket, zmtpStream &msg, size_t limit, AsyncFlags flags, uint64_t timeout, zmtpRecvCb callback, void *arg)
{
  Context context(startZmtpRecv, recvFinish, &msg, nullptr, limit, zmtpUnknown);
  auto makeResult = [](void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    context->Result = static_cast<ssize_t>(context->BytesTransferred);
  };
  auto initOp = [](asyncOpRoot *op, void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    reinterpret_cast<zmtpOp*>(op)->type = context->MsgType;
    reinterpret_cast<zmtpOp*>(op)->transferred = context->BytesTransferred;
  };

  runAioOperation(&socket->root, newReadAsyncOp, implZmtpRecvStreamProxy, makeResult, initOp, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpRecv, &context);
  return context.Result;
}

ssize_t aioZmtpSend(zmtpSocket *socket, void *data, size_t size, zmtpUserMsgTy type, AsyncFlags flags, uint64_t timeout, zmtpSendCb callback, void *arg)
{
  Context context(startZmtpSend, sendFinish, nullptr, data, size, type);
  auto makeResult = [](void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    context->Result = static_cast<ssize_t>(context->BytesTransferred);
  };
  auto initOp = [](asyncOpRoot *op, void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    reinterpret_cast<zmtpOp*>(op)->transferred = context->BytesTransferred;
  };

  runAioOperation(&socket->root, newWriteAsyncOp, implZmtpSendProxy, makeResult, initOp, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpSend, &context);
  return context.Result;
}

int ioZmtpAccept(zmtpSocket *socket, AsyncFlags flags, uint64_t timeout)
{
  Context context(startZmtpAccept, 0, nullptr, nullptr, 0, zmtpUnknown);
  asyncOpRoot *op = newAsyncOp(&socket->root, flags | afCoroutine, timeout, nullptr, nullptr, zmtpOpAccept, &context);
  if (!__uintptr_atomic_compare_and_swap(&socket->root.initializationOp,
                                         0,
                                         reinterpret_cast<uintptr_t>(op),
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(op, aosUnknownError);
    addToGlobalQueue(op);
  } else {
    combinerPushOperation(op);
  }
  coroutineYield();

  AsyncOpStatus status = opGetStatus(op);
  releaseAsyncOp(op);
  return status == aosSuccess ? 0 : -status;
}

int ioZmtpConnect(zmtpSocket *socket, const HostAddress *address, AsyncFlags flags, uint64_t timeout)
{
  Context context(startZmtpConnect, 0, nullptr, nullptr, 0, zmtpUnknown);
  zmtpOp *op =
    reinterpret_cast<zmtpOp*>(newAsyncOp(&socket->root, flags | afCoroutine, timeout, nullptr, nullptr, zmtpOpConnect, &context));
  op->address = *address;
  if (!__uintptr_atomic_compare_and_swap(&socket->root.initializationOp,
                                         0,
                                         reinterpret_cast<uintptr_t>(&op->root),
                                         amoSeqCst)) {
    // Transport initialization is one-shot for an object.
    opForceStatus(&op->root, aosUnknownError);
    addToGlobalQueue(&op->root);
  } else {
    combinerPushOperation(&op->root);
  }
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -status;
}

ssize_t ioZmtpRecv(zmtpSocket *socket, zmtpStream &msg, size_t limit, AsyncFlags flags, uint64_t timeout, zmtpUserMsgTy *type)
{
  Context context(startZmtpRecv, 0, &msg, nullptr, limit, zmtpUnknown);
  auto initOp = [](asyncOpRoot *op, void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    reinterpret_cast<zmtpOp*>(op)->type = context->MsgType;
    reinterpret_cast<zmtpOp*>(op)->transferred = context->BytesTransferred;
  };

  zmtpOp *op = reinterpret_cast<zmtpOp*>(runIoOperation(&socket->root, newReadAsyncOp, implZmtpRecvStreamProxy, initOp, flags, timeout, zmtpOpRecv, &context));

  if (op) {
    AsyncOpStatus status = opGetStatus(&op->root);
    *type = (op->type & zmtpMsgFlagCommand) ? zmtpCommand : zmtpMessage;
    releaseAsyncOp(&op->root);
    return status == aosSuccess ? static_cast<ssize_t>(msg.sizeOf()) : -status;
  } else {
    *type = (context.MsgType & zmtpMsgFlagCommand) ? zmtpCommand : zmtpMessage;
    return static_cast<ssize_t>(msg.sizeOf());
  }
}

ssize_t ioZmtpSend(zmtpSocket *socket, void *data, size_t size, zmtpUserMsgTy type, AsyncFlags flags, uint64_t timeout)
{
  Context context(startZmtpSend, 0, nullptr, data, size, type);
  auto initOp = [](asyncOpRoot *op, void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    reinterpret_cast<zmtpOp*>(op)->transferred = context->BytesTransferred;
  };

  asyncOpRoot *op = runIoOperation(&socket->root, newWriteAsyncOp, implZmtpSendProxy, initOp, flags, timeout, zmtpOpSend, &context);

  if (op) {
    AsyncOpStatus status = opGetStatus(op);
    releaseAsyncOp(op);
    return status == aosSuccess ? static_cast<ssize_t>(size) : -status;
  } else {
    return static_cast<ssize_t>(size);
  }
}
