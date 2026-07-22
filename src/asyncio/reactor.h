// Compact kernel handles, timer state and I/O executors shared by epoll and
// kqueue.
#ifndef __ASYNCIO_REACTOR_H_
#define __ASYNCIO_REACTOR_H_

#include "asyncioImpl.h"

#ifndef OS_WINDOWS
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// ---- handles --------------------------------------------------------------

enum {
  REACTOR_HANDLE_GENERATION_BITS = 22
};

// 42 pointer bits plus 22 low generation bits. ABA requires a full 2^22 wrap
// of one cell while its old kernel event remains unprocessed.
enum {
  REACTOR_HANDLE_GENERATION_SHIFT = 64 - REACTOR_HANDLE_GENERATION_BITS,
  REACTOR_HANDLE_POINTER_SHIFT = TAGGED_POINTER_DATA_SIZE
};

#define REACTOR_HANDLE_GENERATION_MASK ((1ULL << REACTOR_HANDLE_GENERATION_BITS) - 1)
#define REACTOR_HANDLE_POINTER_MASK ((1ULL << REACTOR_HANDLE_GENERATION_SHIFT) - 1)

static inline uint64_t kernelHandleEncode(objectHeader *header)
{
  uint64_t pointer = (uint64_t)header;
  assert(header && !(pointer & TAGGED_POINTER_DATA_MASK) && pointer < (1ULL << 48));
  uint64_t generation = objectHeaderGeneration(header);
  return (pointer >> REACTOR_HANDLE_POINTER_SHIFT) | ((generation & REACTOR_HANDLE_GENERATION_MASK) << REACTOR_HANDLE_GENERATION_SHIFT);
}

static inline objectHeader *kernelHandleDecode(uint64_t encoded, uint64_t *generation)
{
  objectHeader *decoded = (objectHeader*)((encoded & REACTOR_HANDLE_POINTER_MASK) << REACTOR_HANDLE_POINTER_SHIFT);
  uint64_t current = objectHeaderGeneration(decoded);
  // The live header supplies the upper generation bits; validation happens
  // in the type-specific claim.
  *generation = (current & ~REACTOR_HANDLE_GENERATION_MASK) | (encoded >> REACTOR_HANDLE_GENERATION_SHIFT);
  return decoded;
}

typedef enum TimerKind {
  tkUnknown = 0,
  tkOperation,
  tkUserEvent
} TimerKind;

struct aioTimer {
  // tag.low: published kqueue ident or epoll armed marker; zero rejects delivery.
  // tag.high: operation or timer generation.
  objectHeader header;
  union {
    struct {
      // Relaxed atomics; tag.high release/acquire versions this snapshot.
      uint64_t deadline;
      aioObjectRoot *object;
      uint64_t objectGeneration;
      asyncOpRoot *op;
    } operation;

    struct {
      // OWNER-only state; remaining == 0 means unlimited.
      aioTimerUserEventState state;
      uint64_t generation;
      aioUserEvent *userEvent;
    } event;
  };
  // timerfd on epoll, composite ident on kqueue.
  intptr_t fd;
};

typedef char aioTimerMustStayCompact[sizeof(aioTimer) <= 2 * CACHE_LINE_SIZE ? 1 : -1];
typedef char aioTimerUserEventStateMustFollowHeader[offsetof(aioTimer, event.state) == sizeof(objectHeader) ? 1 : -1];

static inline void timerInitialize(aioTimer *timer)
{
  __uint64_atomic_store(&timer->header.tag.low, 0, amoRelaxed);
  __uint64_atomic_store(&timer->header.tag.high, 0, amoRelaxed);
  objectHeaderSetType(&timer->header, ohtTimer);
  timer->header.timer.kind = tkUnknown;
  timer->header.timer.registered = 0;
  timer->header.timer.reserved = 0;
  __uint64_atomic_store(&timer->operation.deadline, 0, amoRelaxed);
  __pointer_atomic_store((void *volatile*)&timer->operation.object, 0, amoRelaxed);
  __uint64_atomic_store(&timer->operation.objectGeneration, 0, amoRelaxed);
  timer->operation.op = 0;
}

static inline void timerUnpublish(aioTimer *timer)
{
  __uint64_atomic_store(&timer->header.tag.low, 0, amoRelaxed);
}

static inline void timerPublishBegin(aioTimer *timer)
{
  __uint64_atomic_store(&timer->header.tag.low, 0, amoRelaxed);
}

static inline void timerPublishEnd(aioTimer *timer, uint64_t generation, uint64_t publication)
{
  __uint64_atomic_store(&timer->header.tag.high, generation, amoRelease);
  __uint64_atomic_store(&timer->header.tag.low, publication, amoRelease);
}

#ifndef OS_WINDOWS
// ---- I/O executors ----------------------------------------------------------
// kqueue and epoll drive nonblocking POSIX descriptors with identical
// executor bodies, kept here as the single copy both backends reference from
// their impl tables. Exactly one readiness backend is compiled into a build
// and only its TU takes these addresses; under Windows (where this header is
// pulled in by white-box tests) the section compiles away. Stream-write
// dispatch is the one backend-specific piece - BSD kernels suppress SIGPIPE
// per socket via SO_NOSIGPIPE while Linux needs an explicit
// send(MSG_NOSIGNAL) - so each backend keeps its own write executor built
// from guardedWrite and transferStatus below.

static inline int getFd(aioObject *object)
{
  switch (object->root.header.objectType) {
    case ioObjectDevice: return object->hDevice;
    case ioObjectSocket: return object->hSocket;
    default: return -1;
  }
}

static inline asyncOpRoot *newAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool)
{
  asyncOp *op = 0;
  if (asyncOpAlloc(base, sizeof(asyncOp), isRealTime, objectPool, objectTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }

  return &op->root;
}

// A readiness backend has nothing in flight inside the kernel to abort: a
// cancelled operation is releasable immediately.
static inline int cancelAsyncOp(asyncOpRoot *opptr)
{
  (void)opptr;
  return 1;
}

static inline AsyncOpStatus connectSyscall(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);
  if (op->state == 0) {
    op->state = 1;
    struct sockaddr_storage sa;
    socklen_t saLen = hostAddressToSockaddr(&op->host, &sa);
    int result = connect(fd, (struct sockaddr*)&sa, saLen);
    if (result == -1 && errno != EINPROGRESS)
      return aosUnknownError;
    else
      return aosPending;
  } else {
    int error;
    socklen_t size = sizeof(error);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size);
    return (error == 0) ? aosSuccess : aosUnknownError;
  }
}

static inline AsyncOpStatus acceptSyscall(asyncOpRoot *opptr)
{
  struct sockaddr_storage clientAddr;
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);
  socklen_t clientAddrSize = sizeof(clientAddr);
#ifdef SOCK_NONBLOCK
  op->acceptSocket = accept4(fd, (struct sockaddr*)&clientAddr, &clientAddrSize, SOCK_NONBLOCK);
#else
  op->acceptSocket = accept(fd, (struct sockaddr*)&clientAddr, &clientAddrSize);
#endif

  if (op->acceptSocket != -1) {
#ifndef SOCK_NONBLOCK
    int current = fcntl(op->acceptSocket, F_GETFL);
    fcntl(op->acceptSocket, F_SETFL, O_NONBLOCK | current);
#endif
    sockaddrToHostAddress(&clientAddr, &op->host);
    return aosSuccess;
  } else if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ECONNABORTED || errno == EPROTO || errno == EINTR) {
    // The connection can be gone from the backlog by the time accept runs
    // (stolen by another thread, aborted by the peer): wait for the next one.
    // Resource exhaustion (EMFILE/ENFILE/ENOBUFS) must NOT retry: the
    // backlog stays readable and the retry loop would spin hot
    return aosPending;
  } else {
    return aosUnknownError;
  }
}

// write() with SIGPIPE masked around the syscall for pipe descriptors on
// platforms without per-fd suppression; everything else takes the plain call.
static inline ssize_t guardedWrite(aioObject *object, int fd, const void *data, size_t size)
{
  ssize_t bytesWritten;
  if (object->needSigpipeGuard && !sigpipeIgnored) {
    struct SigpipeGuard guard;
    sigpipeGuardEnter(&guard);
    bytesWritten = write(fd, data, size);
    sigpipeGuardLeave(&guard, bytesWritten == -1 && errno == EPIPE);
  } else {
    bytesWritten = write(fd, data, size);
  }
  return bytesWritten;
}

// Common epilogue of the stream read/write executors: accumulate progress,
// stay pending while afWaitAll has not drained the transaction, map an
// end-of-stream zero to aosDisconnected and a failed syscall through
// socketStatusFromErrno (errno must still belong to that syscall).
static inline AsyncOpStatus transferStatus(asyncOp *op, ssize_t transferred)
{
  if (transferred > 0) {
    op->bytesTransferred += (size_t)transferred;
    if (op->root.flags & afWaitAll && op->bytesTransferred < op->transactionSize)
      return aosPending;
    else
      return aosSuccess;
  } else if (transferred == 0) {
    return op->transactionSize - op->bytesTransferred > 0 ? aosDisconnected : aosSuccess;
  } else {
    return socketStatusFromErrno(errno);
  }
}

static inline AsyncOpStatus readSyscall(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  aioObject *object = (aioObject*)op->root.object;
  struct ioBuffer *sb = &object->buffer;
  int fd = getFd(object);

  if (copyFromBuffer(op->buffer, &op->bytesTransferred, sb, op->transactionSize))
    return aosSuccess;
  // A partial hit from the read-ahead buffer completes a non-afWaitAll read
  // (shared contract with implRead and the iocp executor)
  if (op->bytesTransferred != 0 && !(opptr->flags & afWaitAll))
    return aosSuccess;

  if (op->transactionSize <= object->buffer.totalSize) {
    while (op->bytesTransferred < op->transactionSize) {
      ssize_t bytesRead = read(fd, sb->ptr, sb->totalSize);
      if (bytesRead == 0)
        return aosDisconnected;
      else if (bytesRead < 0)
        return socketStatusFromErrno(errno);
      sb->dataSize = (size_t)bytesRead;

      if (copyFromBuffer(op->buffer, &op->bytesTransferred, sb, op->transactionSize) || !(opptr->flags & afWaitAll))
        break;
      // A short read means the queue is drained: stay parked instead of
      // collecting the EAGAIN
      if ((size_t)bytesRead < sb->totalSize)
        return aosPending;
    }

    return aosSuccess;
  } else {
    ssize_t bytesRead = read(fd, (uint8_t*)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
    return transferStatus(op, bytesRead);
  }
}

static inline AsyncOpStatus readMsgSyscall(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);

  struct sockaddr_storage source;
  struct iovec iov;
  struct msghdr msg;
  memset(&msg, 0, sizeof(msg));
  iov.iov_base = op->buffer;
  iov.iov_len = op->transactionSize;
  msg.msg_name = &source;
  msg.msg_namelen = sizeof(source);
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  ssize_t result = recvmsg(fd, &msg, 0);
  if (result != -1) {
    sockaddrToHostAddress(&source, &op->host);
    op->bytesTransferred = (size_t)result;
    // MSG_TRUNC: the datagram did not fit, the kernel dropped its tail
    return (msg.msg_flags & MSG_TRUNC) ? aosBufferTooSmall : aosSuccess;
  } else {
    return socketStatusFromErrno(errno);
  }
}

static inline AsyncOpStatus writeMsgSyscall(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((aioObject*)op->root.object);

  struct sockaddr_storage remoteAddress;
  socklen_t addrLen = hostAddressToSockaddr(&op->host, &remoteAddress);
  ssize_t result = sendto(fd, op->buffer, op->transactionSize, 0, (struct sockaddr*)&remoteAddress, addrLen);
  if (result != -1) {
    return aosSuccess;
  }

  return socketStatusFromErrno(errno);
}

// ---- combiner pass ----------------------------------------------------------

typedef struct {
  uint32_t oldIoEvents;
  uint32_t ioEvents;
} CombinerPassEvents;

// The backend-independent middle of the readiness combiner pass, between the
// envelope decode and the backend's kernel rearm epilogue. The order is the
// protocol:
//  1. start the submitted node first: a start delivered together with an
//     event must enter its queue before a failed connect cancels the queues,
//     otherwise the operation would start on the dead socket afterwards;
//  2. by contract initialization precedes ordinary I/O, so any progress while
//     its slot is occupied belongs to it - drive it before the disconnect
//     sweep, so queued operations inherit a connect failure, not
//     aosDisconnected;
//  3. CANCEL/CANCELIO reap: a timeout/opCancel/cancelIo set the status and
//     asked for a scan; the CANCELIO position additionally bounds the bulk
//     cancelIo() sweep;
//  4. the error sweep (kernel read-side EOF/half-close arrives as
//     COMBINER_TAG_ERROR): a read side with nothing left buffered dies as
//     aosDisconnected. The write side is deliberately left alone: EOF only
//     says the peer stopped sending - after shutdown(SHUT_WR) it still reads
//     and a parked response must complete (IOCP parity); on a truly dead
//     connection the writes fail per-op with EPIPE/ECONNRESET;
//  5. execute whatever became startable.
// A dying object gets no fd readiness processing: its operations are being
// cancelled wholesale anyway, and its descriptor may already be closed; the
// error path ioctl and the caller's rearm syscall must not run on a reused
// fd - the returned ioEvents are already zeroed for it. fdObject is the
// object itself for fd-backed objects (READ/WRITE tag values deliberately
// match IO_EVENT_READ/WRITE), null otherwise.
static inline CombinerPassEvents reactorCombinerCore(aioObjectRoot *object, aioObject *fdObject, asyncOpRoot *op, uint32_t sig)
{
  uint32_t oldIoEvents = fdObject ? combinerActiveIoEvents(object) : 0;
  uint32_t progress = sig & COMBINER_TAG_PROGRESS_MASK;
  uint32_t ioEvents = fdObject ? progress | ((sig & COMBINER_TAG_ERROR) ? IO_EVENT_ERROR : 0) : 0;

  if (fdObject && __uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    ioEvents = 0;
    progress = 0;
  }

  uint32_t needStart = progress;

  if (op)
    startOperation(op, &needStart);

  if (progress && __uintptr_atomic_load(&object->initializationOp, amoRelaxed))
    processInitializationOp(object, &needStart);

  if (sig & (COMBINER_TAG_CANCEL | COMBINER_TAG_CANCELIO))
    reapObject(object, sig, &needStart);

  if (ioEvents & IO_EVENT_ERROR) {
    // FIONREAD failure (ENOTTY on an exotic device fd) counts as drained:
    // an error event with unknowable backlog must resolve to a deterministic
    // disconnect, not a read queue waiting forever
    int available = 0;
    int fd = getFd(fdObject);
    if (ioctl(fd, FIONREAD, &available) == -1)
      available = 0;
    if (available == 0)
      cancelOperationList(&object->readQueue, aosDisconnected);
  }

  if (needStart & IO_EVENT_READ)
    executeOperationList(&object->readQueue);
  if (needStart & IO_EVENT_WRITE)
    executeOperationList(&object->writeQueue);

  CombinerPassEvents result = { oldIoEvents, ioEvents };
  return result;
}
#endif // !OS_WINDOWS

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_REACTOR_H_
