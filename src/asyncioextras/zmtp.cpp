#include "asyncioextras/zmtp.h"
#include "asyncio/coroutine.h"
#include <stdlib.h>
#include <string.h>

constexpr size_t POOLED_BUFFER_SIZE_LIMIT = 16384;

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

// Valid peer socket types per ZMTP 3.0 (23/ZMTP), indexed by the local type;
// the relation is symmetric, so both handshake sides use one table
static const uint16_t zmtpValidPeers[] = {
  (1u << zmtpSocketREP) | (1u << zmtpSocketROUTER),                          // REQ
  (1u << zmtpSocketREQ) | (1u << zmtpSocketDEALER),                          // REP
  (1u << zmtpSocketREP) | (1u << zmtpSocketDEALER) | (1u << zmtpSocketROUTER),  // DEALER
  (1u << zmtpSocketREQ) | (1u << zmtpSocketDEALER) | (1u << zmtpSocketROUTER),  // ROUTER
  (1u << zmtpSocketSUB) | (1u << zmtpSocketXSUB),                            // PUB
  (1u << zmtpSocketSUB) | (1u << zmtpSocketXSUB),                            // XPUB
  (1u << zmtpSocketPUB) | (1u << zmtpSocketXPUB),                            // SUB
  (1u << zmtpSocketPUB) | (1u << zmtpSocketXPUB),                            // XSUB
  (1u << zmtpSocketPULL),                                                    // PUSH
  (1u << zmtpSocketPUSH),                                                    // PULL
  (1u << zmtpSocketPAIR)                                                     // PAIR
};

static bool zmtpPeerCompatible(zmtpSocketTy type, const RawData &peerType)
{
  for (size_t i = 0; i < sizeof(socketTypeNames)/sizeof(socketTypeNames[0]); i++) {
    if (peerType.size == strlen(socketTypeNames[i]) && memcmp(peerType.data, socketTypeNames[i], peerType.size) == 0)
      return (zmtpValidPeers[type] >> i) & 1u;
  }
  return false;
}

static inline zmtpMsgTy operator|(zmtpMsgTy a, zmtpMsgTy b) {
  return static_cast<zmtpMsgTy>(static_cast<int>(a) | static_cast<int>(b));
}

enum btcOpTy {
  zmtpOpAccept = OPCODE_READ | OPCODE_INIT,
  zmtpOpRecv = OPCODE_READ + 1,
  zmtpOpConnect = OPCODE_WRITE | OPCODE_INIT,
  zmtpOpSend = OPCODE_WRITE + 1
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

  stWriteData,

  stFinished
};

__NO_PADDING_BEGIN
struct zmtpSocket {
  aioObjectRoot root;
  aioObject *plainSocket;
  zmtpSocketTy type;
  bool needSendMore;
  // the read and write lanes run concurrently, so each stages its wire
  // headers in its own buffer; the exclusive handshake uses the recv one
  uint8_t recvBuffer[256];
  uint8_t sendBuffer[256];
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
  void *internalBuffer;
  size_t internalBufferSize;
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

// zmtpAcceptCb, zmtpConnectCb and zmtpSendCb are the same function type, so
// accept, connect and send operations share one finish thunk
static void socketOpFinish(asyncOpRoot *opptr)
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

// Wire message type for a user-visible message kind (a long frame is any
// payload that does not fit the one-byte length)
static zmtpMsgTy zmtpMsgTypeFor(zmtpUserMsgTy userType, size_t size)
{
  switch (userType) {
    case zmtpCommand :
      return (size < 256) ? zmtpMsgFlagCommand : zmtpMsgFlagCommand | zmtpMsgFlagLong;
    case zmtpMessagePart :
      return (size < 256) ? zmtpMsgFlagMore : zmtpMsgFlagMore | zmtpMsgFlagLong;
    case zmtpMessage :
    default :
      return (size < 256) ? zmtpMsgFlagNone : zmtpMsgFlagLong;
  }
}

static void releaseProc(asyncOpRoot *opptr)
{
  zmtpOp *op = reinterpret_cast<zmtpOp*>(opptr);
  if (op->internalBufferSize > POOLED_BUFFER_SIZE_LIMIT) {
    free(op->internalBuffer);
    op->internalBuffer = nullptr;
    op->internalBufferSize = 0;
  }
}

// Common part of the operation constructors: pooled allocation (a fresh
// operation starts with an empty capture buffer) and state setup
static zmtpOp *allocZmtpOp(aioObjectRoot *object,
                           AsyncFlags flags,
                           uint64_t usTimeout,
                           void *callback,
                           void *arg,
                           int opCode,
                           const Context *context)
{
  zmtpOp *op = 0;
  if (asyncOpAlloc(object->header.base, sizeof(zmtpOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = nullptr;
    op->internalBufferSize = 0;
  }
  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseProc, object, callback, arg, flags, opCode, usTimeout);
  op->state = stInitialize;
  op->stateRw = stInitialize;
  return op;
}

static asyncOpRoot *newAsyncOp(aioObjectRoot *object,
                               AsyncFlags flags,
                               uint64_t usTimeout,
                               void *callback,
                               void *arg,
                               int opCode,
                               void *contextPtr)
{
  zmtpOp *op = allocZmtpOp(object, flags, usTimeout, callback, arg, opCode, (const Context*)contextPtr);
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
  const Context *context = (const Context*)contextPtr;
  zmtpOp *op = allocZmtpOp(object, flags, usTimeout, callback, arg, opCode, context);
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
  const Context *context = (const Context*)contextPtr;
  zmtpOp *op = allocZmtpOp(object, flags, usTimeout, callback, arg, opCode, context);

  // without afNoCopy the payload is only valid until the send call returns,
  // so an operation that will read it later must own a copy
  if (!(flags & afNoCopy) && context->TransactionSize) {
    if (op->internalBuffer == nullptr) {
      op->internalBuffer = malloc(context->TransactionSize);
      op->internalBufferSize = context->TransactionSize;
      poolCacheHandoff(op->internalBuffer);
    } else if (op->internalBufferSize < context->TransactionSize) {
      op->internalBufferSize = context->TransactionSize;
      op->internalBuffer = realloc(op->internalBuffer, context->TransactionSize);
      poolCacheHandoff(op->internalBuffer);
    }

    memcpy(op->internalBuffer, context->Buffer, context->TransactionSize);
    op->data = op->internalBuffer;
  } else {
    op->data = context->Buffer;
  }

  op->stream = nullptr;
  op->size = context->TransactionSize;
  op->type = zmtpMsgTypeFor(context->UserMsgType, context->TransactionSize);
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
        childOp = implRead(socket->plainSocket, socket->recvBuffer, 10, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptWriteLocalSignature : {
        if (socket->recvBuffer[0] != 0xFF || socket->recvBuffer[9] != 0x7F)
          return aosUnknownError;
        op->state = stAcceptReadMajorVersion;
        childOp = implWrite(socket->plainSocket, localSignature, sizeof(localSignature), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptReadMajorVersion : {
        op->state = stAcceptWriteLocalMajorVersion;
        childOp = implRead(socket->plainSocket, socket->recvBuffer, 1, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptWriteLocalMajorVersion : {
        op->state = stAcceptReadMinorVersion;
        childOp = implWrite(socket->plainSocket, &localMajorVersion, 1, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stAcceptReadMinorVersion : {
        op->state = stAcceptWriteLocalGreeting;
        childOp = implRead(socket->plainSocket, socket->recvBuffer, 1+20+1+31, afWaitAll, 0, resumeRwCb, opptr, &bytes);
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
        op->data = socket->recvBuffer;
        op->size = sizeof(socket->recvBuffer);
        break;
      }

      case stAcceptWriteReadyMsgWaiting : {
        AsyncOpStatus result = startZmtpRecv(&op->root);
        if (result != aosSuccess)
          return result;

        // READY must arrive as a command frame, not as a data frame whose
        // payload merely parses like one
        if (!(op->type & zmtpMsgFlagCommand))
          return aosUnknownError;

        RawData rawSocketType;
        RawData rawIdentity;
        zmtpStream stream(socket->recvBuffer, op->transferred);
        if (!stream.readReadyCmd(&rawSocketType, &rawIdentity))
          return aosUnknownError;
        if (!zmtpPeerCompatible(socket->type, rawSocketType))
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
        childOp = implRead(socket->plainSocket, socket->recvBuffer, 10, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectWriteLocalMajorVersion : {
        op->state = stConnectReadMajorVersion;
        childOp = implWrite(socket->plainSocket, &localMajorVersion, sizeof(localMajorVersion), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectReadMajorVersion : {
        op->state = stConnectWriteLocalGreeting;
        childOp = implRead(socket->plainSocket, socket->recvBuffer, 1, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectWriteLocalGreeting : {
        op->state = stConnectReadGreeting;
        childOp = implWrite(socket->plainSocket, localGreetingOther, sizeof(localGreetingOther), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectReadGreeting : {
        op->state = stConnectWriteReadyMsg;
        childOp = implRead(socket->plainSocket, socket->recvBuffer, 1+20+1+31, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stConnectWriteReadyMsg : {
        op->state = stConnectReadReadyMsg;
        zmtpStream stream(socket->recvBuffer, sizeof(socket->recvBuffer));
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
        op->data = socket->recvBuffer;
        op->size = sizeof(socket->recvBuffer);
        break;
      }

      case stConnectReadReadyMsgWaiting : {
        AsyncOpStatus result = startZmtpRecv(&op->root);
        if (result != aosSuccess)
          return result;

        // READY must arrive as a command frame, not as a data frame whose
        // payload merely parses like one
        if (!(op->type & zmtpMsgFlagCommand))
          return aosUnknownError;

        RawData rawSocketType;
        RawData rawIdentity;
        zmtpStream stream(socket->recvBuffer, op->transferred);
        if (!stream.readReadyCmd(&rawSocketType, &rawIdentity))
          return aosUnknownError;
        if (!zmtpPeerCompatible(socket->type, rawSocketType))
          return aosUnknownError;
        return aosSuccess;
      }

      default :
        return aosUnknownError;
    }
  }

  combinerPushOperation(childOp);
  return aosPending;
}

// Frame length decoded from the header bytes in socket->recvBuffer (a short frame
// carries one length byte, a long frame an 8-byte network-order length)
static size_t zmtpFrameLength(zmtpSocket *socket, zmtpMsgTy type)
{
  if (!(type & zmtpMsgFlagLong)) {
    return static_cast<size_t>(socket->recvBuffer[1]);
  } else {
    // memcpy keeps the single unaligned load the cast used to compile to,
    // without the alignment UB
    uint64_t length;
    memcpy(&length, socket->recvBuffer+1, sizeof(length));
    return static_cast<size_t>(xntoh<uint64_t>(length));
  }
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
        op->transferred = 0;
        if (op->stream)
          op->stream->reset();
        break;
      }
      case stRecvReadType : {
        op->stateRw = stRecvReadSize;
        childOp = implRead(socket->plainSocket, socket->recvBuffer, 2, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stRecvReadSize : {
        op->stateRw = stRecvReadData;
        // TODO: validate reserved flags and reject COMMAND|MORE.
        op->type = static_cast<zmtpMsgTy>(socket->recvBuffer[0]);
        if (op->type & zmtpMsgFlagLong)
          childOp = implRead(socket->plainSocket, socket->recvBuffer+2, 7, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stRecvReadData : {
        op->stateRw = (op->type & zmtpMsgFlagMore) ? stRecvReadType : stFinished;
        size_t frameSize = zmtpFrameLength(socket, op->type);

        // the limit caps the whole message, so every frame draws from the
        // budget left by the previous ones; transferred accumulates the total
        if (frameSize <= op->size - op->transferred)
          childOp = implRead(socket->plainSocket,
                             op->stream ? op->stream->reserve(frameSize) : static_cast<uint8_t*>(op->data) + op->transferred,
                             frameSize, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        else
          return aosBufferTooSmall;
        op->transferred += frameSize;
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
    if ( (childOp = implRead(socket->plainSocket, socket->recvBuffer, 2, afWaitAll, 0, resumeRwCb, nullptr, &bytes)) ) {
      state = stRecvReadSize;
      break;
    }

    type = static_cast<zmtpMsgTy>(socket->recvBuffer[0]);
    if (type & zmtpMsgFlagLong) {
      childOp = implRead(socket->plainSocket, socket->recvBuffer+2, 7, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
      if (childOp) {
        state = stRecvReadData;
        break;
      }
    }

    // the limit caps the whole message, so every frame draws from the budget
    // left by the previous ones; transferred accumulates the total and counts
    // an in-flight frame before the state is handed to the queued operation
    size_t frameSize = zmtpFrameLength(socket, type);

    if (frameSize <= limit - transferred) {
      transferred += frameSize;
      if ( (childOp = implRead(socket->plainSocket, msg.reserve(frameSize), frameSize, afWaitAll, 0, resumeRwCb, nullptr, &bytes)) ) {
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

// Builds the wire header in socket->sendBuffer: the REQ/REP empty delimiter frame
// when one is due, the flags byte and the short/long length; returns the
// number of header bytes. Updates the REQ/REP delimiter state.
static unsigned zmtpBuildSendHeader(zmtpSocket *socket, zmtpMsgTy msgType, size_t size)
{
  unsigned offset = 0;
  if (!(msgType & zmtpMsgFlagCommand) &&
      (socket->type == zmtpSocketREQ || socket->type == zmtpSocketREP) && !socket->needSendMore) {
    socket->sendBuffer[0] = zmtpMsgFlagMore;
    socket->sendBuffer[1] = 0;
    offset = 2;
  }

  socket->needSendMore = (msgType & zmtpMsgFlagMore);

  if (!(msgType & zmtpMsgFlagLong)) {
    socket->sendBuffer[offset] = static_cast<uint8_t>(msgType);
    socket->sendBuffer[offset+1] = static_cast<uint8_t>(size);
    offset += 2;
  } else {
    socket->sendBuffer[offset] = static_cast<uint8_t>(msgType);
    uint64_t belength = xhton<uint64_t>(size);
    memcpy(socket->sendBuffer+offset+1, &belength, sizeof(belength));
    offset += 9;
  }

  return offset;
}

static AsyncOpStatus startZmtpSend(asyncOpRoot *opptr)
{
  zmtpOp *op = reinterpret_cast<zmtpOp*>(opptr);
  zmtpSocket *socket = reinterpret_cast<zmtpSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  while (!childOp) {
    switch (op->stateRw) {
      case stInitialize : {
        // header build and first write form one step: no child operation can
        // separate them, so no intermediate state is ever observed here
        unsigned offset = zmtpBuildSendHeader(socket, op->type, op->size);
        if (op->size <= (sizeof(socket->sendBuffer) - offset)) {
          op->stateRw = stFinished;
          memcpy(socket->sendBuffer+offset, op->data, op->size);
          childOp = implWrite(socket->plainSocket, socket->sendBuffer, op->size+offset, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        } else {
          op->stateRw = stWriteData;
          childOp = implWrite(socket->plainSocket, socket->sendBuffer, offset, afWaitAll, 0, resumeRwCb, opptr, &bytes);
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

static asyncOpRoot *implZmtpSend(zmtpSocket *socket, void *data, size_t size, zmtpUserMsgTy type, AsyncFlags flags, uint64_t timeout, zmtpSendCb callback, void *arg, size_t *bytesTransferred)
{
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  zmtpOpState state = stInitialize;
  zmtpMsgTy msgType = zmtpMsgTypeFor(type, size);
  unsigned offset = zmtpBuildSendHeader(socket, msgType, size);

  if (size <= (sizeof(socket->sendBuffer) - offset)) {
    memcpy(socket->sendBuffer+offset, data, size);
    childOp = implWrite(socket->plainSocket, socket->sendBuffer, size+offset, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
    state = stFinished;
  } else {
    childOp = implWrite(socket->plainSocket, socket->sendBuffer, offset, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
    if (!childOp) {
      childOp = implWrite(socket->plainSocket, data, size, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
      state = stFinished;
    } else {
      state = stWriteData;
    }
  }

  if (childOp) {
    Context context(startZmtpSend, socketOpFinish, nullptr, data, size, type);
    // at stFinished the payload is already staged (socket->sendBuffer or the child
    // operation's own copy), so the capture copy is skipped (afNoCopy); only a
    // header-blocked operation still reads the payload after this call returns
    AsyncFlags captureFlags = (state == stWriteData) ? flags : flags | afNoCopy;
    zmtpOp *op = reinterpret_cast<zmtpOp*>(newWriteAsyncOp(&socket->root, captureFlags | afRunning, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpSend, &context));
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

static void zmtpSocketDestructor(aioObjectRoot *object)
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
  return socket;
}

void zmtpSocketDelete(zmtpSocket *socket)
{
  objectDelete(&socket->root);
}

void aioZmtpAccept(zmtpSocket *socket, AsyncFlags flags, uint64_t timeout, zmtpAcceptCb callback, void *arg)
{
  Context context(startZmtpAccept, socketOpFinish, nullptr, nullptr, 0, zmtpUnknown);
  asyncOpRoot *op = newAsyncOp(&socket->root, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpAccept, &context);
  combinerPushOperation(op);
}

void aioZmtpConnect(zmtpSocket *socket, const HostAddress *address, AsyncFlags flags, uint64_t timeout, zmtpConnectCb callback, void *arg)
{
  Context context(startZmtpConnect, socketOpFinish, nullptr, nullptr, 0, zmtpUnknown);
  zmtpOp *op =
    reinterpret_cast<zmtpOp*>(newAsyncOp(&socket->root, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpConnect, &context));
  op->address = *address;
  combinerPushOperation(&op->root);
}

// Inline-completion glue shared by the aio/io entry points: publish the byte
// count as the call result, or seed a queued operation with the inline state
static void makeResult(void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  context->Result = static_cast<ssize_t>(context->BytesTransferred);
}

static void initRecvOp(asyncOpRoot *op, void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  reinterpret_cast<zmtpOp*>(op)->type = context->MsgType;
  reinterpret_cast<zmtpOp*>(op)->transferred = context->BytesTransferred;
}

static void initSendOp(asyncOpRoot *op, void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  reinterpret_cast<zmtpOp*>(op)->transferred = context->BytesTransferred;
}

ssize_t aioZmtpRecv(zmtpSocket *socket, zmtpStream &msg, size_t limit, AsyncFlags flags, uint64_t timeout, zmtpRecvCb callback, void *arg)
{
  Context context(startZmtpRecv, recvFinish, &msg, nullptr, limit, zmtpUnknown);
  runAioOperation(&socket->root, newReadAsyncOp, implZmtpRecvStreamProxy, makeResult, initRecvOp, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpRecv, &context);
  return context.Result;
}

ssize_t aioZmtpSend(zmtpSocket *socket, void *data, size_t size, zmtpUserMsgTy type, AsyncFlags flags, uint64_t timeout, zmtpSendCb callback, void *arg)
{
  Context context(startZmtpSend, socketOpFinish, nullptr, data, size, type);
  runAioOperation(&socket->root, newWriteAsyncOp, implZmtpSendProxy, makeResult, initSendOp, flags, timeout, reinterpret_cast<void*>(callback), arg, zmtpOpSend, &context);
  return context.Result;
}

int ioZmtpAccept(zmtpSocket *socket, AsyncFlags flags, uint64_t timeout)
{
  Context context(startZmtpAccept, 0, nullptr, nullptr, 0, zmtpUnknown);
  asyncOpRoot *op = newAsyncOp(&socket->root, flags | afCoroutine, timeout, nullptr, nullptr, zmtpOpAccept, &context);
  combinerPushOperation(op);
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
  combinerPushOperation(&op->root);
  coroutineYield();
  AsyncOpStatus status = opGetStatus(&op->root);
  releaseAsyncOp(&op->root);
  return status == aosSuccess ? 0 : -status;
}

ssize_t ioZmtpRecv(zmtpSocket *socket, zmtpStream &msg, size_t limit, AsyncFlags flags, uint64_t timeout, zmtpUserMsgTy *type)
{
  Context context(startZmtpRecv, 0, &msg, nullptr, limit, zmtpUnknown);
  zmtpOp *op = reinterpret_cast<zmtpOp*>(runIoOperation(&socket->root, newReadAsyncOp, implZmtpRecvStreamProxy, initRecvOp, flags, timeout, zmtpOpRecv, &context));

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
  asyncOpRoot *op = runIoOperation(&socket->root, newWriteAsyncOp, implZmtpSendProxy, initSendOp, flags, timeout, zmtpOpSend, &context);

  if (op) {
    AsyncOpStatus status = opGetStatus(op);
    releaseAsyncOp(op);
    return status == aosSuccess ? static_cast<ssize_t>(size) : -status;
  } else {
    return static_cast<ssize_t>(size);
  }
}
