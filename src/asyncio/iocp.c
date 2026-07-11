#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <winsock2.h>
#include <mswsock.h>
#include <windows.h>
#include "asyncioImpl.h"
#include "atomic.h"
#include <stdlib.h>
#include <time.h>

static ConcurrentQueue objectPool;

typedef struct iocpOp iocpOp;

typedef struct recvFromData {
  struct sockaddr_storage addr;
  INT size;
} recvFromData;

typedef struct iocpBase {
  asyncBase B;
  HANDLE completionPort;
  LPFN_CONNECTEX ConnectExPtr;
} iocpBase;

typedef struct iocpOp {
  asyncOp info;
  OVERLAPPED overlapped;
} iocpOp;

typedef struct aioTimer {
  asyncOpRoot *op;
  HANDLE hTimer;
  PTP_WAIT wait;
  volatile uintptr_t state;
} aioTimer;

#ifndef CREATE_WAITABLE_TIMER_HIGH_RESOLUTION
#define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x00000002
#endif

#define IOCP_TIMER_STOPPED 0
#define IOCP_TIMER_ARMED 1
#define IOCP_TIMER_CALLBACK 2

void combinerTaskHandler(aioObjectRoot* object, asyncOpRoot* op, uint32_t sig);
void iocpEnqueue(asyncBase *base, asyncOpRoot *op);
void postEmptyOperation(asyncBase *base);
void iocpNextFinishedOperation(asyncBase *base);
aioObject *iocpNewAioObject(asyncBase *base, IoObjectTy type, void *data);
asyncOpRoot *iocpNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool);
int iocpCancelAsyncOp(asyncOpRoot *opptr);
void iocpDeleteObject(aioObject *op);
void iocpInitializeTimer(asyncBase *base, asyncOpRoot *op);
void iocpStartTimer(asyncOpRoot *op);
void iocpStopTimer(asyncOpRoot *op);
void iocpDeleteTimer(asyncOpRoot *op);
void iocpActivate(aioUserEvent *event);
AsyncOpStatus iocpAsyncConnect(asyncOpRoot *op);
AsyncOpStatus iocpAsyncAccept(asyncOpRoot *op);
AsyncOpStatus iocpAsyncRead(asyncOpRoot *op);
AsyncOpStatus iocpAsyncWrite(asyncOpRoot *op);
AsyncOpStatus iocpAsyncReadMsg(asyncOpRoot *op);
AsyncOpStatus iocpAsyncWriteMsg(asyncOpRoot *op);

static struct asyncImpl iocpImpl = {
  combinerTaskHandler,
  iocpEnqueue,
  postEmptyOperation,
  iocpNextFinishedOperation,
  iocpNewAioObject,
  iocpNewAsyncOp,
  iocpCancelAsyncOp,
  iocpDeleteObject,
  iocpInitializeTimer,
  iocpStartTimer,
  iocpStopTimer,
  iocpDeleteTimer,
  iocpActivate,
  iocpAsyncConnect,
  iocpAsyncAccept,
  iocpAsyncRead,
  iocpAsyncWrite,
  iocpAsyncReadMsg,
  iocpAsyncWriteMsg
};

static aioObject *getObject(iocpOp *op)
{
  return (aioObject*)op->info.root.object;
}

static AsyncOpStatus iocpGetOverlappedResult(iocpOp *op)
{
  DWORD bytesTransferred;
  DWORD flags;
  BOOL result;
  aioObject *object = getObject(op);
  if (object->root.type == ioObjectSocket) {
    result = WSAGetOverlappedResult(object->hSocket, &op->overlapped, &bytesTransferred, FALSE, &flags);
    if (result == TRUE) {
      // Check for disconnect
      if ((op->info.root.opCode == actRead || op->info.root.opCode == actWrite) && bytesTransferred == 0 && op->info.transactionSize > 0) {
        return aosDisconnected;
      }
      return aosSuccess;
    } else {
      int error = WSAGetLastError();
      if (error == WSAEMSGSIZE)
        return aosBufferTooSmall;
      else if (error == WSAECONNRESET || error == ERROR_NETNAME_DELETED)
        return aosDisconnected;
      else if (error == ERROR_OPERATION_ABORTED)
        return aosCanceled;
      else
        return aosUnknownError;
    }
  }
  else {
    result = GetOverlappedResult(object->hDevice, &op->overlapped, &bytesTransferred, FALSE);
    return result == TRUE ? aosSuccess : aosUnknownError;
  }
}

static int iocpArmWaitableTimer(aioTimer *timer, uint64_t timeout)
{
  LARGE_INTEGER signalTime;
  signalTime.QuadPart = -(int64_t)(timeout * 10);
  SetThreadpoolWait(timer->wait, timer->hTimer, NULL);
  if (SetWaitableTimer(timer->hTimer, &signalTime, 0, NULL, NULL, FALSE))
    return 1;
  SetThreadpoolWait(timer->wait, NULL, NULL);
  return 0;
}

static void iocpDisarmWaitableTimer(aioTimer *timer)
{
  __uintptr_atomic_store(&timer->state, IOCP_TIMER_STOPPED, amoRelease);
  CancelWaitableTimer(timer->hTimer);
  SetThreadpoolWait(timer->wait, NULL, NULL);
  WaitForThreadpoolWaitCallbacks(timer->wait, TRUE);
}

static void iocpUserEventTimerCb(aioTimer *timer)
{
  int needReactivate = 1;
  aioUserEvent *event = (aioUserEvent*)timer->op;

  if (__uintptr_atomic_load(&timer->state, amoAcquire) != IOCP_TIMER_CALLBACK)
    return;

  if (eventTryActivate(event))
    iocpActivate(event);

  if (event->counter > 0) {
    if (--event->counter == 0)
      needReactivate = 0;
  }

  if (needReactivate) {
    if (__uintptr_atomic_compare_and_swap(&timer->state, IOCP_TIMER_CALLBACK, IOCP_TIMER_ARMED)) {
      if (!iocpArmWaitableTimer(timer, event->root.timeout)) {
        __uintptr_atomic_compare_and_swap(&timer->state, IOCP_TIMER_ARMED, IOCP_TIMER_STOPPED);
      } else if (__uintptr_atomic_load(&timer->state, amoAcquire) == IOCP_TIMER_STOPPED) {
        CancelWaitableTimer(timer->hTimer);
        SetThreadpoolWait(timer->wait, NULL, NULL);
      }
    }
  } else {
    __uintptr_atomic_compare_and_swap(&timer->state, IOCP_TIMER_CALLBACK, IOCP_TIMER_STOPPED);
  }
}


static void iocpIoFinishedTimerCb(aioTimer *timer)
{
  asyncOpRoot *op = timer->op;
  // The generation is read from the operation itself: unlike the reactor
  // backends nothing generation-like travels through the kernel here, and the
  // stop/delete rendezvous (WaitForThreadpoolWaitCallbacks) guarantees the
  // operation cannot be recycled while this callback runs - the ARMED->CALLBACK
  // CAS is the stale-fire gate, the status CAS below settles the race against
  // a concurrent completion
  uintptr_t generation = opGetGeneration(op);

  __uintptr_atomic_store(&timer->state, IOCP_TIMER_STOPPED, amoRelease);
  if (opSetStatus(op, generation, aosTimeout))
    combinerPushCounter(op->object, COMBINER_TAG_CANCEL);
}

static VOID CALLBACK iocpTimerCb(PTP_CALLBACK_INSTANCE instance, PVOID context, PTP_WAIT wait, TP_WAIT_RESULT waitResult)
{
  aioTimer *timer = (aioTimer*)context;
  __UNUSED(instance);
  __UNUSED(wait);
  __UNUSED(waitResult);

  if (!__uintptr_atomic_compare_and_swap(&timer->state, IOCP_TIMER_ARMED, IOCP_TIMER_CALLBACK))
    return;

  if (timer->op->opCode == actUserEvent)
    iocpUserEventTimerCb(timer);
  else
    iocpIoFinishedTimerCb(timer);
}

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig)
{
  uint32_t progress = sig & COMBINER_TAG_PROGRESS_MASK;
  // READ/WRITE tag values deliberately match IO_EVENT_READ/WRITE.
  uint32_t needStart = progress;

  // Start a submitted operation, then reconcile the signal for this node by
  // status. The proactor has no readiness side-channel: continue/finish/release
  // all arrive as PROGRESS_*, cancellation as CANCEL.
  if (op)
    startOperation(op, &needStart);

  if (progress && __uintptr_atomic_load(&object->initializationOp, amoRelaxed))
    processInitializationOp(object, &needStart);
  // CANCEL/CANCELIO: the CANCELIO position additionally drives the
  // CancelIoFlag sweep
  if (sig & (COMBINER_TAG_CANCEL | COMBINER_TAG_CANCELIO))
    reapObject(object, sig, &needStart);

  if (needStart & IO_EVENT_READ)
    executeOperationList(&object->readQueue);
  if (needStart & IO_EVENT_WRITE)
    executeOperationList(&object->writeQueue);
}


void iocpEnqueue(asyncBase *base, asyncOpRoot *op)
{
  PostQueuedCompletionStatus(((iocpBase*)base)->completionPort, 0, (ULONG_PTR)op, 0);
}


void postEmptyOperation(asyncBase *base)
{
  PostQueuedCompletionStatus(((iocpBase*)base)->completionPort, 0, 0, 0);
}


asyncBase *iocpNewAsyncBase()
{
  iocpBase *base = malloc(sizeof(iocpBase));
  if (base) {
    SOCKET tmpSocket;
    DWORD numBytes = 0;
    GUID guid = WSAID_CONNECTEX;

    base->completionPort =
      CreateIoCompletionPort(INVALID_HANDLE_VALUE, 0, 0, 0);
    base->ConnectExPtr = 0;
    tmpSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    WSAIoctl(tmpSocket,
      SIO_GET_EXTENSION_FUNCTION_POINTER,
      &guid,
      sizeof(guid),
      &base->ConnectExPtr,
      sizeof(base->ConnectExPtr),
      &numBytes,
      NULL,
      NULL);
    CloseHandle((HANDLE)tmpSocket);

    base->B.methodImpl = iocpImpl;
  }

  return (asyncBase*)base;
}


void iocpNextFinishedOperation(asyncBase *base)
{
  OVERLAPPED_ENTRY entries[128];
  const int maxEntriesNum = sizeof(entries) / sizeof(OVERLAPPED_ENTRY);
  iocpBase *localBase = (iocpBase*)base;
  messageLoopThreadId = __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, 1);

  while (1) {
    ULONG N, i;

    BOOL status = GetQueuedCompletionStatusEx(localBase->completionPort, entries, maxEntriesNum, &N, 500, FALSE);

    uint64_t currentTime = getMonotonicSeconds();
    unsigned loopThreadCount = __uint_atomic_load(&base->messageLoopThreadCounter, amoRelaxed);
    if (currentTime % loopThreadCount == messageLoopThreadId)
      processTimeoutQueue(base, currentTime);

    // ignore false status
    if (status == FALSE)
      continue;

    for (i = 0; i < N; i++) {
      OVERLAPPED_ENTRY *entry = &entries[i];
      if (entry->lpCompletionKey) {
        asyncOpRoot *op = (asyncOpRoot*)entry->lpCompletionKey;
        if (op->opCode == actUserEvent) {
          aioUserEvent *event = (aioUserEvent*)op;
          eventDeactivate(event);
          op->finishMethod(op);
          eventDecrementReference(event, 1);
        } else {
          currentFinishedSync = 0;
          if (op->flags & afCoroutine) {
            coroutineCall((coroutineTy*)op->finishMethod);
          } else {
            aioObjectRoot* object = op->object;
            if (op->callback)
              op->finishMethod(op);
            concurrentQueuePush(op->objectPool, op);
            objectDecrementReference(object, 1);
          }
        }
      } else if (entry->lpOverlapped) {
        iocpOp *op = (iocpOp*)(((uint8_t*)entry->lpOverlapped) - offsetof(struct iocpOp, overlapped));
        AsyncOpStatus result = iocpGetOverlappedResult(op);
        if (result == aosSuccess) {
          aioObject *object = (aioObject*)op->info.root.object;
          int isBuffered = op->info.root.opCode == actRead && op->info.transactionSize < object->buffer.totalSize;
          if (!isBuffered)
            op->info.bytesTransferred += entry->dwNumberOfBytesTransferred;
          else
            object->buffer.dataSize = entry->dwNumberOfBytesTransferred;
          if (op->info.root.opCode == actAccept) {
            const size_t addrSize = sizeof(struct sockaddr_storage) + 16;
            struct sockaddr *localAddr = 0;
            struct sockaddr *remoteAddr = 0;
            INT localAddrLength;
            INT remoteAddrLength;
            GetAcceptExSockaddrs(op->info.internalBuffer,
              entry->dwNumberOfBytesTransferred,
              addrSize,
              addrSize,
              &localAddr, &localAddrLength,
              &remoteAddr, &remoteAddrLength);
            if (localAddr && remoteAddr) {
              sockaddrToHostAddress((struct sockaddr_storage*)remoteAddr, &op->info.host);
            } else {
              result = aosUnknownError;
            }
          } else if (op->info.root.opCode == actRead || op->info.root.opCode == actWrite) {
            if (isBuffered || ((op->info.root.flags & afWaitAll) && op->info.bytesTransferred < op->info.transactionSize)) {
              combinerPushProgress(&op->info.root);
              continue;
            }
          } else if (op->info.root.opCode == actReadMsg) {
            struct recvFromData *rf = op->info.internalBuffer;
            sockaddrToHostAddress(&rf->addr, &op->info.host);
          } else if (op->info.root.opCode == actConnect) {
            // Put the socket into the regular connected state, otherwise
            // getpeername/shutdown on it stay broken after ConnectEx
            setsockopt(object->hSocket, SOL_SOCKET, SO_UPDATE_CONNECT_CONTEXT, NULL, 0);
          }
        } else if (result == aosBufferTooSmall && op->info.root.opCode == actReadMsg) {
          // WSAEMSGSIZE: the buffer holds the first part of the datagram, the
          // tail is dropped; the kernel still reports the source address
          struct recvFromData *rf = op->info.internalBuffer;
          op->info.bytesTransferred = entry->dwNumberOfBytesTransferred;
          sockaddrToHostAddress(&rf->addr, &op->info.host);
        } else if (result == aosDisconnected && op->info.root.opCode == actAccept) {
          // A connection that died in the backlog (RST) completes the parked
          // AcceptEx with ERROR_NETNAME_DELETED; posix kernels hand out the
          // dead socket successfully and let the application find out on the
          // first read. Don't fail the accept operation over a remote-side
          // event: drop the corpse and re-post AcceptEx for the next
          // connection (same re-drive as partial read/write above)
          closesocket(op->info.acceptSocket);
          op->info.acceptSocket = INVALID_SOCKET;
          combinerPushProgress(&op->info.root);
          continue;
        }

        opSetStatus(&op->info.root, opGetGeneration(&op->info.root), result);
        combinerPushProgress(&op->info.root);
      } else {
        unsigned threadsRunning = __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, -1) - 1;
        if (threadsRunning)
          PostQueuedCompletionStatus(((iocpBase*)base)->completionPort, 0, 0, 0);
        return;
      }
    }
  }
}


aioObject *iocpNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  iocpBase *localBase = (iocpBase*)base;
  aioObject* object = 0;
  if (!objectPoolGet(&objectPool, (void**)&object, sizeof(aioObject))) {
    object = alignedMalloc(sizeof(aioObject), TAGGED_POINTER_ALIGNMENT);
    object->buffer.ptr = 0;
    object->buffer.totalSize = 0;
  }

  initObjectRoot(&object->root, base, type, (aioObjectDestructor*)iocpDeleteObject);
  switch (type) {
    case ioObjectDevice:
      object->hDevice = *(iodevTy *)data;
      CreateIoCompletionPort(object->hDevice, localBase->completionPort, 0, 1);
      break;
    case ioObjectSocket:
      object->hSocket = *(socketTy *)data;
      CreateIoCompletionPort((HANDLE)object->hSocket, localBase->completionPort, 0, 1);
      break;
    default:
      break;
  }

  object->buffer.offset = 0;
  object->buffer.dataSize = 0;
  return object;
}


asyncOpRoot *iocpNewAsyncOp(asyncBase* base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool)
{
  iocpOp *op = 0;
  if (asyncOpAlloc(base, sizeof(iocpOp), isRealTime, objectPool, objectTimerPool, (asyncOpRoot**)&op)) {
    op->info.internalBuffer = 0;
    op->info.internalBufferSize = 0;
  }

  memset(&op->overlapped, 0, sizeof(op->overlapped));
  return &op->info.root;
}

int iocpCancelAsyncOp(asyncOpRoot *opptr)
{
  aioObject *object = (aioObject*)opptr->object;
  iocpOp *op = (iocpOp*)opptr;
  switch (object->root.type) {
    case ioObjectDevice:
      CancelIoEx(object->hDevice, &op->overlapped);
      break;
    case ioObjectSocket:
      CancelIoEx((HANDLE)object->hSocket, &op->overlapped);
      break;
    default:
      break;
  }

  return 0;
}

void iocpDeleteObject(aioObject *object)
{
  switch (object->root.type) {
    case ioObjectDevice:
      CloseHandle(object->hDevice);
      break;
    case ioObjectSocket:
      closesocket(object->hSocket);
      break;
    default:
      break;
  }

  objectPoolPut(&objectPool, object, sizeof(aioObject));
}

void iocpInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  __UNUSED(base);
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  timer->op = op;
  timer->state = IOCP_TIMER_STOPPED;
  timer->hTimer = CreateWaitableTimerExW(NULL, NULL, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
  if (!timer->hTimer)
    timer->hTimer = CreateWaitableTimer(NULL, FALSE, NULL);
  timer->wait = timer->hTimer ? CreateThreadpoolWait(iocpTimerCb, timer, NULL) : NULL;
  op->timerId = timer;
}

void iocpStartTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  timer->op = op;
  if (__uintptr_atomic_load(&timer->state, amoAcquire) != IOCP_TIMER_STOPPED)
    iocpDisarmWaitableTimer(timer);
  __uintptr_atomic_store(&timer->state, IOCP_TIMER_ARMED, amoRelease);
  if (!timer->hTimer || !timer->wait || !iocpArmWaitableTimer(timer, op->timeout)) {
    __uintptr_atomic_store(&timer->state, IOCP_TIMER_STOPPED, amoRelease);
    if (op->opCode == actUserEvent) {
      aioUserEvent *event = (aioUserEvent*)op;
      if (eventTryActivate(event))
        iocpActivate(event);
    } else if (opSetStatus(op, opGetGeneration(op), aosUnknownError)) {
      combinerPushCounter(op->object, COMBINER_TAG_CANCEL);
    }
  }
}


void iocpStopTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (timer->hTimer && timer->wait)
    iocpDisarmWaitableTimer(timer);
}

void iocpDeleteTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (timer->hTimer && timer->wait)
    iocpDisarmWaitableTimer(timer);
  if (timer->wait)
    CloseThreadpoolWait(timer->wait);
  if (timer->hTimer)
    CloseHandle(timer->hTimer);
  free(timer);
}

void iocpActivate(aioUserEvent *event)
{
  iocpEnqueue(event->base, &event->root);
}


AsyncOpStatus iocpAsyncConnect(asyncOpRoot *opptr)
{
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);
  iocpBase *localBase = (iocpBase*)object->root.base;

  struct sockaddr_storage sa;
  socklen_t saLen = hostAddressToSockaddr(&op->info.host, &sa);
  memset(&op->overlapped, 0, sizeof(op->overlapped));
  // ConnectEx is BOOL: nonzero = synchronous success with the completion
  // packet still queued to the port - park and let it finish there. Zero
  // with an error other than WSA_IO_PENDING means nothing was submitted:
  // parking such an operation leaves a zombie no completion or CancelIoEx
  // packet can ever finish
  int result = localBase->ConnectExPtr(object->hSocket, (const struct sockaddr*)&sa, saLen, NULL, 0, NULL, &op->overlapped);
  if (result != 0)
    return aosPending;
  return WSAGetLastError() == WSA_IO_PENDING ? aosPending : aosUnknownError;
}


AsyncOpStatus iocpAsyncAccept(asyncOpRoot *opptr)
{
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);

  const size_t addrSize = sizeof(struct sockaddr_storage) + 16;
  const size_t acceptResultSize = 2 * addrSize;
  if (op->info.internalBuffer == 0) {
    op->info.internalBuffer = malloc(acceptResultSize);
    op->info.internalBufferSize = acceptResultSize;
  } else if (op->info.internalBufferSize < acceptResultSize) {
    op->info.internalBuffer = realloc(op->info.internalBuffer, acceptResultSize);
    op->info.internalBufferSize = acceptResultSize;
  }

  struct sockaddr_storage listenAddr;
  int listenAddrLen = sizeof(listenAddr);
  getsockname(object->hSocket, (struct sockaddr*)&listenAddr, &listenAddrLen);

  u_long arg = 1;
  op->info.acceptSocket = WSASocket(listenAddr.ss_family, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
  ioctlsocket(op->info.acceptSocket, FIONBIO, &arg);

  memset(&op->overlapped, 0, sizeof(op->overlapped));
  int result = AcceptEx(object->hSocket,
                        op->info.acceptSocket,
                        op->info.internalBuffer,
                        0,
                        addrSize,
                        addrSize,
                        NULL,
                        &op->overlapped);

  // AcceptEx is BOOL like ConnectEx: nonzero = synchronous success (packet
  // still queued), zero + WSA_IO_PENDING = in flight, anything else was
  // never submitted and must fail here instead of parking a zombie
  if (result != 0)
    return aosPending;
  return WSAGetLastError() == WSA_IO_PENDING ? aosPending : aosUnknownError;
}


AsyncOpStatus iocpAsyncRead(asyncOpRoot *opptr)
{
  WSABUF wsabuf;
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);
  struct ioBuffer *sb = &object->buffer;
  DWORD flags = 0;

  if (copyFromBuffer(op->info.buffer, &op->info.bytesTransferred, sb, op->info.transactionSize))
    return aosSuccess;

  if (op->info.transactionSize <= object->buffer.totalSize) {
    memset(&op->overlapped, 0, sizeof(op->overlapped));
    if (object->root.type == ioObjectDevice) {
      // TODO: check totalSize > 4Gb
      int result = ReadFile(object->hDevice, sb->ptr, (DWORD)sb->totalSize, 0, &op->overlapped);
      if (result == TRUE || GetLastError() == WSA_IO_PENDING)
        return aosPending;
      else
        return aosUnknownError;
    } else {
      wsabuf.buf = sb->ptr;
      wsabuf.len = (ULONG)sb->totalSize;
      int result = WSARecv(object->hSocket, &wsabuf, 1, NULL, &flags, &op->overlapped, NULL);
      if (result == 0 || WSAGetLastError() == WSA_IO_PENDING)
        return aosPending;
      else
        return aosUnknownError;
    }
  } else {
    wsabuf.buf = (CHAR*)op->info.buffer + op->info.bytesTransferred;
    wsabuf.len = (ULONG)(op->info.transactionSize - op->info.bytesTransferred);
    memset(&op->overlapped, 0, sizeof(op->overlapped));
    if (object->root.type == ioObjectDevice) {
      // TODO: check totalSize > 4Gb
      int result = ReadFile(object->hDevice, (CHAR*)op->info.buffer + op->info.bytesTransferred, (DWORD)(op->info.transactionSize - op->info.bytesTransferred), 0, &op->overlapped);
      if (result == TRUE || GetLastError() == WSA_IO_PENDING)
        return aosPending;
      else
        return aosUnknownError;
    } else {
      int result = WSARecv(object->hSocket, &wsabuf, 1, NULL, &flags, &op->overlapped, NULL);
      if (result == 0 || WSAGetLastError() == WSA_IO_PENDING)
        return aosPending;
      else
        return aosUnknownError;
    }

  }
}


AsyncOpStatus iocpAsyncWrite(asyncOpRoot *opptr)
{
  WSABUF wsabuf;
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);
  // TODO: correct processing >4Gb data blocks
  memset(&op->overlapped, 0, sizeof(op->overlapped));
  if (object->root.type == ioObjectDevice) {
    // TODO: check totalSize > 4Gb
    BOOL result = WriteFile(object->hDevice, (CHAR*)op->info.buffer + op->info.bytesTransferred, (DWORD)(op->info.transactionSize - op->info.bytesTransferred), 0, &op->overlapped);
    if (result == TRUE || GetLastError() == WSA_IO_PENDING)
      return aosPending;
    else
      return aosUnknownError;
  } else {
    wsabuf.buf = (CHAR*)op->info.buffer + op->info.bytesTransferred;
    wsabuf.len = (ULONG)(op->info.transactionSize - op->info.bytesTransferred);
    int result = WSASend(object->hSocket, &wsabuf, 1, NULL, 0, &op->overlapped, NULL);
    if (result == 0 || WSAGetLastError() == WSA_IO_PENDING)
      return aosPending;
    else
      return aosUnknownError;
  }
}


AsyncOpStatus iocpAsyncReadMsg(asyncOpRoot *opptr)
{
  WSABUF wsabuf;
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);

  const size_t acceptResultSize = sizeof(recvFromData);
  if (op->info.internalBuffer == 0) {
    op->info.internalBuffer = malloc(acceptResultSize);
    op->info.internalBufferSize = acceptResultSize;
  }
  else if (op->info.internalBufferSize < acceptResultSize) {
    op->info.internalBuffer = realloc(op->info.internalBuffer, acceptResultSize);
    op->info.internalBufferSize = acceptResultSize;
  }

  recvFromData *rf = op->info.internalBuffer;
  rf->size = sizeof(rf->addr);
  DWORD flags = 0;
  // TODO: correct processing >4Gb data blocks
  wsabuf.buf = op->info.buffer;
  wsabuf.len = (ULONG)op->info.transactionSize;

  memset(&op->overlapped, 0, sizeof(op->overlapped));
  int result = WSARecvFrom(object->hSocket, &wsabuf, 1, NULL, &flags, (SOCKADDR*)&rf->addr, &rf->size, &op->overlapped, NULL);
  if (result == 0 || WSAGetLastError() == WSA_IO_PENDING) {
    return aosPending;
  } else if (WSAGetLastError() == WSAEMSGSIZE) {
    // The datagram is consumed and cut down to the buffer size right away;
    // no completion packet follows an immediate failure
    op->info.bytesTransferred = op->info.transactionSize;
    sockaddrToHostAddress(&rf->addr, &op->info.host);
    return aosBufferTooSmall;
  } else {
    return aosUnknownError;
  }
}


AsyncOpStatus iocpAsyncWriteMsg(asyncOpRoot *opptr)
{
  WSABUF wsabuf;
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);

  struct sockaddr_storage remoteAddress;
  socklen_t addrLen = hostAddressToSockaddr(&op->info.host, &remoteAddress);
  // TODO: correct processing >4Gb data blocks
  wsabuf.buf = op->info.buffer;
  wsabuf.len = (ULONG)op->info.transactionSize;
  memset(&op->overlapped, 0, sizeof(op->overlapped));
  int result = WSASendTo(object->hSocket, &wsabuf, 1, NULL, 0, (struct sockaddr*)&remoteAddress, addrLen, &op->overlapped, NULL);
  if (result == 0 || WSAGetLastError() == WSA_IO_PENDING) {
    return aosPending;
  } else {
    return aosUnknownError;
  }
}
