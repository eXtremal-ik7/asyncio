#include "asyncioextras/btc.h"
#include "p2putils/xmstream.h"
#include <openssl/evp.h>
#include <stdlib.h>
#include <string.h>

static ConcurrentQueue opPool;
static ConcurrentQueue opTimerPool;
static ConcurrentQueue objectPool;

struct Context {
  aioExecuteProc *StartProc;
  aioFinishProc *FinishProc;
  xmstream *Stream;
  void *Buffer;
  size_t TransactionSize;
  char *CommandBuffer;
  const char *Command;
  size_t BytesTransferred;
  ssize_t Result;
  Context(aioExecuteProc *startProc,
          aioFinishProc *finishProc,
          xmstream *stream,
          void *buffer,
          size_t transactionSize,
          char *commandBuffer,
          const char *command) :
    StartProc(startProc),
    FinishProc(finishProc),
    Stream(stream),
    Buffer(buffer),
    TransactionSize(transactionSize),
    CommandBuffer(commandBuffer),
    Command(command),
    BytesTransferred(0),
    Result(-aosPending) {}
};

#pragma pack(push, 1)
struct MessageHeader {
  uint32_t magic;
  char command[12];
  uint32_t length;
  uint32_t checksum;
};
#pragma pack(pop)

constexpr int USERSPACE_BUFFER_SIZE=1472;

enum btcOpTy {
  btcOpRecv = OPCODE_READ,
  btcOpSend = OPCODE_WRITE
};

enum btcOpState {
  stInitialize = 0,
  stReadData,
  stWriteData,
  stFinished
};


struct BTCSocket {
  aioObjectRoot root;
  aioObject *plainSocket;
  uint32_t magic;
  uint8_t sendBuffer[USERSPACE_BUFFER_SIZE];
  uint8_t receiveBuffer[sizeof(MessageHeader)];
};

struct btcOp {
  asyncOpRoot root;
  HostAddress address;
  btcOpState state;
  char *commandPtr;
  char command[12];
  xmstream *stream;
  void *buffer;
  size_t size;
  void *internalBuffer;
  size_t internalBufferSize;
};

static uint32_t calculateCheckSum(void *data, size_t size)
{
  unsigned char hash[32];
  unsigned char hash2[32];

  EVP_MD_CTX *ctx = EVP_MD_CTX_create();
  EVP_DigestInit(ctx, EVP_sha256());
  EVP_DigestUpdate(ctx, data, size);
  EVP_DigestFinal(ctx, hash, 0);
  EVP_DigestInit(ctx, EVP_sha256());
  EVP_DigestUpdate(ctx, hash, 32);
  EVP_DigestFinal(ctx, hash2, 0);
  EVP_MD_CTX_destroy(ctx);
  return *reinterpret_cast<uint32_t*>(hash2);
}

static void buildMessageHeader(MessageHeader *out, uint32_t magic, char command[12], void *data, uint32_t size)
{
  out->magic = xhtole(magic);
  memcpy(out->command, command, 12);
  out->length = xhtole(size);
  out->checksum = xhtole(calculateCheckSum(data, size));
}

static void buildMessageHeader(MessageHeader *out, uint32_t magic, const char *command, void *data, uint32_t size)
{
  out->magic = xhtole(magic);
  memset(out->command, 0, sizeof(out->command));
  strncpy(out->command, command, sizeof(out->command));
  out->length = xhtole(size);
  out->checksum = xhtole(calculateCheckSum(data, size));
}

static void decodeMessageHeader(MessageHeader *header)
{
  header->magic = xletoh(header->magic);
  header->length = xletoh(header->length);
  header->checksum = xletoh(header->checksum);
}

static void resumeRwCb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  resumeParent(static_cast<asyncOpRoot*>(arg), status);
}

static int cancel(asyncOpRoot *opptr)
{
  BTCSocket *socket = reinterpret_cast<BTCSocket*>(opptr->object);
  cancelIo(reinterpret_cast<aioObjectRoot*>(socket->plainSocket));
  return 0;
}

static void recvFinish(asyncOpRoot *opptr)
{
  btcOp *op = reinterpret_cast<btcOp*>(opptr);
  reinterpret_cast<btcRecvCb*>(opptr->callback)(opGetStatus(opptr),
                                                      reinterpret_cast<BTCSocket*>(opptr->object),
                                                      op->command,
                                                      op->stream,
                                                      opptr->arg);
}

static void sendFinish(asyncOpRoot *opptr)
{
  reinterpret_cast<btcSendCb*>(opptr->callback)(opGetStatus(opptr),
                                                reinterpret_cast<BTCSocket*>(opptr->object),
                                                opptr->arg);
}

static void releaseProc(asyncOpRoot *opptr)
{
  btcOp *op = (btcOp*)opptr;
  if (op->internalBuffer) {
    free(op->internalBuffer);
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }
}


static asyncOpRoot *newReadAsyncOp(aioObjectRoot *object,
                                   AsyncFlags flags,
                                   uint64_t usTimeout,
                                   void *callback,
                                   void *arg,
                                   int opCode,
                                   void *contextPtr)
{
  btcOp *op = 0;
  struct Context *context = (struct Context*)contextPtr;
  if (asyncOpAlloc(object->base, sizeof(btcOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = nullptr;
    op->internalBufferSize = 0;
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseProc, object, callback, arg, flags, opCode, usTimeout);
  op->state = stInitialize;
  op->size = context->TransactionSize;
  op->stream = context->Stream;
  op->commandPtr = context->CommandBuffer;
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
  btcOp *op = 0;
  struct Context *context = (struct Context*)contextPtr;
  if (asyncOpAlloc(object->base, sizeof(btcOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = nullptr;
    op->internalBufferSize = 0;
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseProc, object, callback, arg, flags, opCode, usTimeout);
  op->state = stInitialize;

  if (!(flags & afNoCopy)) {
    if (op->internalBuffer == nullptr) {
      op->internalBuffer = malloc(context->TransactionSize);
      op->internalBufferSize = context->TransactionSize;
    } else if (op->internalBufferSize < context->TransactionSize) {
      op->internalBufferSize = context->TransactionSize;
      op->internalBuffer = realloc(op->internalBuffer, context->TransactionSize);
    }

    memcpy(op->internalBuffer, context->Buffer, context->TransactionSize);
    op->buffer = op->internalBuffer;
  } else {
    op->buffer = (void*)(uintptr_t)context->Buffer;
  }

  op->size = context->TransactionSize;
  op->stream = nullptr;
  strncpy(op->command, context->Command, sizeof(op->command));
  return &op->root;
}

static AsyncOpStatus startBtcRecv(asyncOpRoot *opptr)
{
  btcOp *op = reinterpret_cast<btcOp*>(opptr);
  BTCSocket *socket = reinterpret_cast<BTCSocket*>(opptr->object);
  MessageHeader *header = reinterpret_cast<MessageHeader*>(socket->receiveBuffer);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;

  while (!childOp) {
    switch (op->state) {
      case stInitialize : {
        // Read header first
        op->state = stReadData;
        childOp = implRead(socket->plainSocket, header, sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stReadData : {
        op->state = stFinished;
        decodeMessageHeader(header);
        if (header->magic != socket->magic)
          return btcMakeStatus(btcInvalidMagic);
        if (header->command[11] != 0)
          return btcMakeStatus(btcInvalidCommand);
        if (op->size < header->length)
          return aosBufferTooSmall;

        op->stream->reset();
        childOp = implRead(socket->plainSocket, op->stream->reserve(header->length), header->length, afWaitAll, 0, resumeRwCb, opptr, &bytes);
        break;
      }

      case stFinished : {
        uint32_t checkSum = calculateCheckSum(op->stream->data(), header->length);
        if (header->checksum != checkSum)
          return btcMakeStatus(btcInvalidChecksum);
        memcpy(op->commandPtr, header->command, 12);
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

asyncOpRoot *implBtcRecv(BTCSocket *socket,
                         char command[12],
                         xmstream &stream,
                         size_t sizeLimit,
                         AsyncFlags flags,
                         uint64_t timeout,
                         btcRecvCb callback,
                         void *arg,
                         size_t *bytesTransferred)
{
  size_t bytes;
  asyncOpRoot *childOp = nullptr;
  btcOpState state = stInitialize;
  AsyncOpStatus result = aosSuccess;
  state = stReadData;
  MessageHeader *header = reinterpret_cast<MessageHeader*>(socket->receiveBuffer);
  if ( !(childOp = implRead(socket->plainSocket, header, sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, nullptr, &bytes)) ) {
    state = stFinished;
    decodeMessageHeader(header);
    if (header->magic != socket->magic)
      result = btcMakeStatus(btcInvalidMagic);
    if (header->command[11] != 0)
      result = btcMakeStatus(btcInvalidCommand);
    if (sizeLimit < header->length)
      result = aosBufferTooSmall;

    stream.reset();
    if (result == aosSuccess &&
        ! (childOp = implRead(socket->plainSocket, stream.reserve(header->length), header->length, afWaitAll, 0, resumeRwCb, nullptr, &bytes))) {
      uint32_t checkSum = calculateCheckSum(stream.data(), header->length);
      if (header->checksum != checkSum)
        result = btcMakeStatus(btcInvalidChecksum);
    }
  }

  if (result != aosSuccess) {
    Context context(startBtcRecv, recvFinish, &stream, nullptr, sizeLimit, command, nullptr);
    asyncOpRoot *op = newReadAsyncOp(&socket->root, flags, timeout, reinterpret_cast<void*>(callback), arg, btcOpRecv, &context);
    opForceStatus(op, result);
    return op;
  }

  if (childOp) {
    Context context(startBtcRecv, recvFinish, &stream, nullptr, sizeLimit, command, nullptr);
    btcOp *op = reinterpret_cast<btcOp*>(newReadAsyncOp(&socket->root, flags | afRunning, timeout, reinterpret_cast<void*>(callback), arg, btcOpRecv, &context));
    op->state = state;
    childOp->arg = op;
    combinerPushOperation(childOp);
    return &op->root;
  }

  stream.seekSet(0);
  memcpy(command, header->command, 12);
  // payload bytes only, the way ioBtcRecv and the zmtp pair report it
  *bytesTransferred = header->length;
  return nullptr;
}

static asyncOpRoot *implBtcRecvProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  return implBtcRecv(reinterpret_cast<BTCSocket*>(object), context->CommandBuffer, *context->Stream, context->TransactionSize, flags, usTimeout, reinterpret_cast<btcRecvCb*>(callback), arg, &context->BytesTransferred);
}


static AsyncOpStatus startBtcSend(asyncOpRoot *opptr)
{
  btcOp *op = reinterpret_cast<btcOp*>(opptr);
  BTCSocket *socket = reinterpret_cast<BTCSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;

  uint8_t buffer[USERSPACE_BUFFER_SIZE];
  MessageHeader *header = reinterpret_cast<MessageHeader*>(buffer);

  while (!childOp) {
    switch (op->state) {
      case stInitialize : {
        buildMessageHeader(header, socket->magic, op->command, op->buffer, static_cast<uint32_t>(op->size));
        if (op->size+sizeof(MessageHeader) <= sizeof(buffer)) {
          op->state = stFinished;
          uint8_t *dataPtr = buffer + sizeof(MessageHeader);
          memcpy(dataPtr, op->buffer, op->size);
          childOp = implWrite(socket->plainSocket, buffer, op->size+sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        } else {
          op->state = stWriteData;
          childOp = implWrite(socket->plainSocket, buffer, sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, opptr, &bytes);
        }

        break;
      }

      case stWriteData : {
        op->state = stFinished;
        childOp = implWrite(socket->plainSocket, op->buffer, op->size, afWaitAll, 0, resumeRwCb, opptr, &bytes);
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

asyncOpRoot *implBtcSend(BTCSocket *socket,
                         const char *command,
                         void *data,
                         size_t size,
                         AsyncFlags flags,
                         uint64_t timeout,
                         btcSendCb callback,
                         void *arg,
                         size_t *bytesTransferred)
{
  asyncOpRoot *childOp = nullptr;
  size_t bytes;
  btcOpState state = stInitialize;

  uint8_t buffer[USERSPACE_BUFFER_SIZE];
  MessageHeader *header = reinterpret_cast<MessageHeader*>(buffer);

  buildMessageHeader(header, socket->magic, command, data, static_cast<uint32_t>(size));

  if (size+sizeof(MessageHeader) <= sizeof(buffer)) {
    state = stFinished;
    uint8_t *dataPtr = buffer + sizeof(MessageHeader);
    memcpy(dataPtr, data, size);
    childOp = implWrite(socket->plainSocket, buffer, size+sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, nullptr, &bytes);
  } else {
    state = stWriteData;
    childOp = implWrite(socket->plainSocket, buffer, sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, nullptr, &bytes);
    if (!childOp) {
      state = stFinished;
      childOp = implWrite(socket->plainSocket, data, size, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
    }
  }

  if (childOp) {
    Context context(startBtcSend, sendFinish, nullptr, data, size, nullptr, command);
    btcOp *op = reinterpret_cast<btcOp*>(newWriteAsyncOp(&socket->root, flags | afRunning, timeout, reinterpret_cast<void*>(callback), arg, btcOpSend, &context));
    op->state = state;
    childOp->arg = op;
    combinerPushOperation(childOp);
    return &op->root;
  }

  // payload bytes only, the way ioBtcSend and the zmtp pair report it
  *bytesTransferred = size;
  return nullptr;
}

static asyncOpRoot *implBtcSendProxy(aioObjectRoot *object, AsyncFlags flags, uint64_t usTimeout, void *callback, void *arg, void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  return implBtcSend(reinterpret_cast<BTCSocket*>(object), context->Command, context->Buffer, context->TransactionSize, flags, usTimeout, reinterpret_cast<btcSendCb*>(callback), arg, &context->BytesTransferred);
}


void btcSocketDestructor(aioObjectRoot *object)
{
  deleteAioObject(reinterpret_cast<BTCSocket*>(object)->plainSocket);
  objectFree(&objectPool, object, sizeof(BTCSocket));
}

aioObjectRoot *btcSocketHandle(BTCSocket *socket)
{
  return &socket->root;
}

BTCSocket *btcSocketNew(asyncBase *base, aioObject *plainSocket)
{
  if (!plainSocket)
    return nullptr;
  BTCSocket *socket = static_cast<BTCSocket*>(
    objectAlloc(&objectPool, sizeof(BTCSocket), 16));
  if (!socket)
    return nullptr;
  initObjectRoot(&socket->root, base, ioObjectUserDefined, btcSocketDestructor);

  socket->plainSocket = plainSocket;

  // Set up default magic for BTC mainnet
  socket->magic = 0xD9B4BEF9;
  return socket;
}

void btcSocketDelete(BTCSocket *socket)
{
  objectDelete(&socket->root);
}

aioObject *btcGetPlainSocket(BTCSocket *socket)
{
  return socket->plainSocket;
}

void btcSocketSetMagic(BTCSocket *socket, uint32_t magic)
{
  socket->magic = magic;
}


ssize_t aioBtcRecv(BTCSocket *socket, char command[12], xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout, btcRecvCb callback, void *arg)
{
  Context context(startBtcRecv, recvFinish, &stream, nullptr, sizeLimit, command, nullptr);
  auto makeResult = [](void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    context->Result = static_cast<ssize_t>(context->BytesTransferred);
  };
  auto initOp = [](asyncOpRoot*, void *) {};

  runAioOperation(&socket->root, newReadAsyncOp, implBtcRecvProxy, makeResult, initOp, flags, timeout, reinterpret_cast<void*>(callback), arg, btcOpRecv, &context);
  return context.Result;
}

ssize_t aioBtcSend(BTCSocket *socket, const char *command, void *data, size_t size, AsyncFlags flags, uint64_t timeout, btcSendCb callback, void *arg)
{
  Context context(startBtcSend, sendFinish, nullptr, data, size, nullptr, command);
  auto makeResult = [](void *contextPtr) {
    Context *context = static_cast<Context*>(contextPtr);
    context->Result = static_cast<ssize_t>(context->BytesTransferred);
  };
  auto initOp = [](asyncOpRoot*, void *) {};
  runAioOperation(&socket->root, newWriteAsyncOp, implBtcSendProxy, makeResult, initOp, flags, timeout, reinterpret_cast<void*>(callback), arg, btcOpSend, &context);
  return context.Result;
}


ssize_t ioBtcRecv(BTCSocket *socket, char command[12], xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout)
{
  Context context(startBtcRecv, 0, &stream, nullptr, sizeLimit, command, nullptr);
  auto initOp = [](asyncOpRoot*, void *) {};
  asyncOpRoot *op = runIoOperation(&socket->root, newReadAsyncOp, implBtcRecvProxy, initOp, flags, timeout, btcOpRecv, &context);

  if (op) {
    AsyncOpStatus status = opGetStatus(op);
    releaseAsyncOp(op);
    return status == aosSuccess ? static_cast<ssize_t>(stream.sizeOf()) : -status;
  } else {
    return static_cast<ssize_t>(stream.sizeOf());
  }
}

ssize_t ioBtcSend(BTCSocket *socket, const char *command, void *data, size_t size, AsyncFlags flags, uint64_t timeout)
{
  Context context(startBtcSend, 0, nullptr, data, size, nullptr, command);
  auto initOp = [](asyncOpRoot*, void *) {};
  asyncOpRoot *op = runIoOperation(&socket->root, newWriteAsyncOp, implBtcSendProxy, initOp, flags, timeout, btcOpSend, &context);

  if (op) {
    AsyncOpStatus status = opGetStatus(op);
    releaseAsyncOp(op);
    return status == aosSuccess ? static_cast<ssize_t>(size) : -status;
  } else {
    return static_cast<ssize_t>(size);
  }
}
