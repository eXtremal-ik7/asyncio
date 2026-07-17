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
// Pooled operations keep their scratch buffer up to this size (matches the
// transport's default read-ahead buffer); larger captures - block-sized
// payloads reach megabytes - are returned to the allocator on completion
constexpr size_t POOLED_BUFFER_SIZE_LIMIT=16384;

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
  uint8_t receiveBuffer[sizeof(MessageHeader)];
};

struct btcOp {
  asyncOpRoot root;
  btcOpState state;
  char *commandPtr;
  char command[12];
  xmstream *stream;
  void *buffer;
  size_t size;
  void *internalBuffer;
  size_t internalBufferSize;
};

namespace {
// The checksum runs twice per message (send and receive); a per-call
// EVP_MD_CTX_create/destroy pair costs a heap round trip each time.
// EVP_DigestInit fully reinitializes the context, so one context per thread
// is equivalent.
struct Sha256Ctx {
  EVP_MD_CTX *ctx = EVP_MD_CTX_create();
  ~Sha256Ctx() { EVP_MD_CTX_destroy(ctx); }
};
}

static uint32_t calculateCheckSum(void *data, size_t size)
{
  unsigned char hash[32];
  unsigned char hash2[32];

  static thread_local Sha256Ctx tls;
  EVP_DigestInit(tls.ctx, EVP_sha256());
  EVP_DigestUpdate(tls.ctx, data, size);
  EVP_DigestFinal(tls.ctx, hash, 0);
  EVP_DigestInit(tls.ctx, EVP_sha256());
  EVP_DigestUpdate(tls.ctx, hash, 32);
  EVP_DigestFinal(tls.ctx, hash2, 0);
  return *reinterpret_cast<uint32_t*>(hash2);
}

// A command is a NUL-padded field that carries no terminator when the name
// fills it completely, and its sources are both C strings and other command
// fields: strnlen bounds the read to the field size for the latter.
static void copyCommand(char *dst, size_t dstSize, const char *src)
{
  memset(dst, 0, dstSize);
  memcpy(dst, src, strnlen(src, dstSize));
}

static void buildMessageHeader(MessageHeader *out, uint32_t magic, const char *command, void *data, uint32_t size)
{
  out->magic = xhtole(magic);
  copyCommand(out->command, sizeof(out->command), command);
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
  if (op->internalBufferSize > POOLED_BUFFER_SIZE_LIMIT) {
    free(op->internalBuffer);
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }
}


// Common part of the operation constructors: pooled allocation (a fresh
// operation starts with an empty scratch buffer) and state setup
static btcOp *allocBtcOp(aioObjectRoot *object,
                         AsyncFlags flags,
                         uint64_t usTimeout,
                         void *callback,
                         void *arg,
                         int opCode,
                         const Context *context)
{
  btcOp *op = 0;
  if (asyncOpAlloc(object->header.base, sizeof(btcOp), flags & afRealtime, &opPool, &opTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = nullptr;
    op->internalBufferSize = 0;
  }

  initAsyncOpRoot(&op->root, context->StartProc, cancel, context->FinishProc, releaseProc, object, callback, arg, flags, opCode, usTimeout);
  op->state = stInitialize;
  return op;
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
  btcOp *op = allocBtcOp(object, flags, usTimeout, callback, arg, opCode, context);
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
  const Context *context = (const Context*)contextPtr;
  btcOp *op = allocBtcOp(object, flags, usTimeout, callback, arg, opCode, context);

  if (!(flags & afNoCopy)) {
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
    op->buffer = op->internalBuffer;
  } else {
    op->buffer = (void*)(uintptr_t)context->Buffer;
  }

  op->size = context->TransactionSize;
  op->stream = nullptr;
  copyCommand(op->command, sizeof(op->command), context->Command);
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

static asyncOpRoot *implBtcRecv(BTCSocket *socket,
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


// First step of a send, shared by the queued and the inline paths: build the
// header and either coalesce a small message into a single write or push the
// header alone. Sets *state (to stFinished or stWriteData) before the write,
// exactly as the callers did.
static asyncOpRoot *btcSendFirstWrite(BTCSocket *socket,
                                      uint8_t (&buffer)[USERSPACE_BUFFER_SIZE],
                                      const char *command,
                                      void *data,
                                      size_t size,
                                      void *resumeArg,
                                      btcOpState *state)
{
  size_t bytes;
  MessageHeader *header = reinterpret_cast<MessageHeader*>(buffer);
  buildMessageHeader(header, socket->magic, command, data, static_cast<uint32_t>(size));

  if (size+sizeof(MessageHeader) <= sizeof(buffer)) {
    *state = stFinished;
    uint8_t *dataPtr = buffer + sizeof(MessageHeader);
    memcpy(dataPtr, data, size);
    return implWrite(socket->plainSocket, buffer, size+sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, resumeArg, &bytes);
  } else {
    *state = stWriteData;
    return implWrite(socket->plainSocket, buffer, sizeof(MessageHeader), afWaitAll, 0, resumeRwCb, resumeArg, &bytes);
  }
}

static AsyncOpStatus startBtcSend(asyncOpRoot *opptr)
{
  btcOp *op = reinterpret_cast<btcOp*>(opptr);
  BTCSocket *socket = reinterpret_cast<BTCSocket*>(opptr->object);
  asyncOpRoot *childOp = nullptr;
  size_t bytes;

  uint8_t buffer[USERSPACE_BUFFER_SIZE];

  while (!childOp) {
    switch (op->state) {
      case stInitialize : {
        childOp = btcSendFirstWrite(socket, buffer, op->command, op->buffer, op->size, opptr, &op->state);
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

static asyncOpRoot *implBtcSend(BTCSocket *socket,
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

  childOp = btcSendFirstWrite(socket, buffer, command, data, size, nullptr, &state);
  if (!childOp && state == stWriteData) {
    // the header went out synchronously: catch up with the payload inline
    state = stFinished;
    childOp = implWrite(socket->plainSocket, data, size, afWaitAll, 0, resumeRwCb, nullptr, &bytes);
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


static void btcSocketDestructor(aioObjectRoot *object)
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


// Inline-completion glue for the aio/io entry points; the operation carries no
// inline state, so the seed hook is a no-op (the core invokes it
// unconditionally)
static void makeResult(void *contextPtr)
{
  Context *context = static_cast<Context*>(contextPtr);
  context->Result = static_cast<ssize_t>(context->BytesTransferred);
}

static void initOpStub(asyncOpRoot*, void*)
{
}

ssize_t aioBtcRecv(BTCSocket *socket, char command[12], xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout, btcRecvCb callback, void *arg)
{
  Context context(startBtcRecv, recvFinish, &stream, nullptr, sizeLimit, command, nullptr);
  runAioOperation(&socket->root, newReadAsyncOp, implBtcRecvProxy, makeResult, initOpStub, flags, timeout, reinterpret_cast<void*>(callback), arg, btcOpRecv, &context);
  return context.Result;
}

ssize_t aioBtcSend(BTCSocket *socket, const char *command, void *data, size_t size, AsyncFlags flags, uint64_t timeout, btcSendCb callback, void *arg)
{
  Context context(startBtcSend, sendFinish, nullptr, data, size, nullptr, command);
  runAioOperation(&socket->root, newWriteAsyncOp, implBtcSendProxy, makeResult, initOpStub, flags, timeout, reinterpret_cast<void*>(callback), arg, btcOpSend, &context);
  return context.Result;
}


ssize_t ioBtcRecv(BTCSocket *socket, char command[12], xmstream &stream, size_t sizeLimit, AsyncFlags flags, uint64_t timeout)
{
  Context context(startBtcRecv, 0, &stream, nullptr, sizeLimit, command, nullptr);
  asyncOpRoot *op = runIoOperation(&socket->root, newReadAsyncOp, implBtcRecvProxy, initOpStub, flags, timeout, btcOpRecv, &context);

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
  asyncOpRoot *op = runIoOperation(&socket->root, newWriteAsyncOp, implBtcSendProxy, initOpStub, flags, timeout, btcOpSend, &context);

  if (op) {
    AsyncOpStatus status = opGetStatus(op);
    releaseAsyncOp(op);
    return status == aosSuccess ? static_cast<ssize_t>(size) : -status;
  } else {
    return static_cast<ssize_t>(size);
  }
}
