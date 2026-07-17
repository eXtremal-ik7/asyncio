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
  objectHeader header;
  union {
    aioObjectRoot *object;
    aioTimerUserEventState userEvent;
  };
  void *target;
  uint64_t objectGeneration;
  HANDLE hTimer;
  PTP_WAIT wait;
  volatile uint64_t eventGeneration;
} aioTimer;

typedef char iocpTimerUserEventStateMustFollowHeader[offsetof(aioTimer, userEvent) == sizeof(objectHeader) ? 1 : -1];

#ifndef CREATE_WAITABLE_TIMER_HIGH_RESOLUTION
#define CREATE_WAITABLE_TIMER_HIGH_RESOLUTION 0x00000002
#endif

#define IOCP_TIMER_STOPPED 0
#define IOCP_TIMER_ARMED 1
#define IOCP_TIMER_CALLBACK 2
#define IOCP_TIMER_KIND_OPERATION 0
#define IOCP_TIMER_KIND_USER_EVENT 1
#define IOCP_USER_EVENT_PACKET 0xA10E0001u

static void iocpInitializeTimerState(aioTimer *timer, asyncBase *base, void *target, unsigned kind)
{
  __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_STOPPED, amoRelaxed);
  __uint64_atomic_store(&timer->header.tag.high, 0, amoRelaxed);
  objectHeaderSetType(&timer->header, ohtTimer);
  timer->header.timer.kind = (uint8_t)kind;
  timer->header.timer.registered = 0;
  timer->header.timer.reserved = 0;
  timer->header.base = base;
  timer->target = target;
  timer->object = 0;
  timer->objectGeneration = 0;
  __uint64_atomic_store(&timer->eventGeneration, 0, amoRelaxed);
}

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig);
void iocpEnqueue(asyncBase *base, asyncOpRoot *op);
void postEmptyOperation(asyncBase *base);
void iocpWakeupLoop(asyncBase *base);
void iocpNextFinishedOperation(asyncBase *base);
aioObject *iocpNewAioObject(asyncBase *base, IoObjectTy type, void *data);
asyncOpRoot *iocpNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool);
int iocpCancelAsyncOp(asyncOpRoot *opptr);
void iocpDeleteObject(aioObject *op);
void iocpInitializeTimer(asyncBase *base, asyncOpRoot *op);
void iocpStartTimer(asyncOpRoot *op);
void iocpStopTimer(asyncOpRoot *op);
int iocpInitializeUserEvent(aioUserEvent *event);
int iocpActivate(aioUserEvent *event);
int iocpUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period);
uint64_t iocpConsumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period);
void iocpReleaseUserEvent(aioUserEvent *event);
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
  iocpInitializeTimer,
  iocpStartTimer,
  iocpStopTimer,
  iocpInitializeUserEvent,
  iocpActivate,
  iocpAsyncConnect,
  iocpAsyncAccept,
  iocpAsyncRead,
  iocpAsyncWrite,
  iocpAsyncReadMsg,
  iocpAsyncWriteMsg,
  iocpWakeupLoop,
  iocpUpdateEventTimer,
  iocpConsumeEventTimerTick,
  iocpReleaseUserEvent
};

static aioObject *getObject(iocpOp *op)
{
  return (aioObject*)op->info.root.object;
}

// A socket call that failed to even start its overlapped operation: map the
// immediate WSA error. WSAENOTCONN (e.g. WSARecv/WSASend before the socket is
// connected) normalizes to aosNotConnected, mirroring EPIPE/ENOTCONN on POSIX;
// anything else is opaque. WSA_IO_PENDING (the op is in flight) is handled by
// the caller before it reaches here.
static AsyncOpStatus iocpInlineErrorStatus(void)
{
  return WSAGetLastError() == WSAENOTCONN ? aosNotConnected : aosUnknownError;
}

static AsyncOpStatus iocpGetOverlappedResult(iocpOp *op)
{
  DWORD bytesTransferred;
  DWORD flags;
  BOOL result;
  aioObject *object = getObject(op);
  if (object->root.header.objectType == ioObjectSocket) {
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
      else if (error == WSAENOTCONN)
        return aosNotConnected;
      else if (error == ERROR_OPERATION_ABORTED)
        return aosCanceled;
      else
        return aosUnknownError;
    }
  } else {
    result = GetOverlappedResult(object->hDevice, &op->overlapped, &bytesTransferred, FALSE);
    return result == TRUE ? aosSuccess : aosUnknownError;
  }
}

static int iocpArmWaitableTimer(aioTimer *timer, uint64_t timeout)
{
  LARGE_INTEGER signalTime;
  signalTime.QuadPart = -(int64_t)(timeout * 10);
  // SetWaitableTimer resets an already-signaled timer. Registering the wait
  // afterwards cannot miss an early expiration because the handle remains
  // signaled until the waiter consumes it; it also avoids registering the
  // previous arm's still-signaled state before the reset.
  if (!SetWaitableTimer(timer->hTimer, &signalTime, 0, NULL, NULL, FALSE))
    return 0;
  SetThreadpoolWait(timer->wait, timer->hTimer, NULL);
  return 1;
}

static void iocpDisarmWaitableTimer(aioTimer *timer)
{
  // STOPPED is only a gate; callback payload travels in local variables, and
  // WaitForThreadpoolWaitCallbacks is the lifetime barrier. No data is
  // released through this word.
  __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_STOPPED, amoRelaxed);
  CancelWaitableTimer(timer->hTimer);
  SetThreadpoolWait(timer->wait, NULL, NULL);
  WaitForThreadpoolWaitCallbacks(timer->wait, TRUE);
}

static void iocpUserEventTimerCb(aioTimer *timer, PTP_CALLBACK_INSTANCE instance)
{
  aioUserEvent *event = (aioUserEvent*)timer->target;
  // iocpTimerCb's successful acquire CAS already observes the generation
  // published before ARMED. Copy it before publishing STOPPED/control; a
  // later rearm may then update the field while this callback only carries
  // the old value in its local.
  uint32_t timerGeneration = (uint32_t)objectHeaderGeneration(&timer->header);
  uint64_t eventGeneration = __uint64_atomic_load(&timer->eventGeneration, amoRelaxed);
  __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_STOPPED, amoRelaxed);
  // The common kernel owner may run a callback which performs final Delete.
  // Disassociation keeps that release's threadpool-wait rendezvous from
  // waiting on itself. Every timer field needed below has already been copied;
  // a successful event claim protects any backend rearm done by the owner.
  DisassociateCurrentThreadFromCallback(instance);
  eventTimerSignal(event, timerGeneration, eventGeneration, 1);
}

static void iocpIoFinishedTimerCb(aioTimer *timer)
{
  asyncOpRoot *op = (asyncOpRoot*)timer->target;
  uint64_t generation = objectHeaderGeneration(&timer->header);
  aioObjectRoot *object = timer->object;
  uint64_t objectGeneration = timer->objectGeneration;

  __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_STOPPED, amoRelaxed);
  (void)opCancel(op, generation, aosTimeout, object, objectGeneration);
}

static VOID CALLBACK iocpTimerCb(PTP_CALLBACK_INSTANCE instance, PVOID context, PTP_WAIT wait, TP_WAIT_RESULT waitResult)
{
  aioTimer *timer = (aioTimer*)context;
  __UNUSED(wait);
  __UNUSED(waitResult);

  if (!__uint64_atomic_compare_and_swap(&timer->header.tag.low, IOCP_TIMER_ARMED, IOCP_TIMER_CALLBACK, amoAcquire))
    return;

  if (timer->header.timer.kind == IOCP_TIMER_KIND_USER_EVENT)
    iocpUserEventTimerCb(timer, instance);
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

void iocpWakeupLoop(asyncBase *base)
{
  // Pure kick of one sleeper: key 0 + overlapped 0 is the quit marker, the
  // byte count 1 tells the loop's marker branch apart from it
  PostQueuedCompletionStatus(((iocpBase*)base)->completionPort, 1, 0, 0);
}

asyncBase *iocpNewAsyncBase()
{
  iocpBase *base = malloc(sizeof(iocpBase));
  if (base) {
    SOCKET tmpSocket;
    DWORD numBytes = 0;
    GUID guid = WSAID_CONNECTEX;

    base->completionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, 0, 0, 0);
    if (!base->completionPort) {
      // Without the completion port the message loop cannot run: fail
      // creation, like the reactor backends do on descriptor exhaustion
      free(base);
      return 0;
    }
    base->ConnectExPtr = 0;
    tmpSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (tmpSocket == INVALID_SOCKET ||
        WSAIoctl(tmpSocket,
                 SIO_GET_EXTENSION_FUNCTION_POINTER,
                 &guid,
                 sizeof(guid),
                 &base->ConnectExPtr,
                 sizeof(base->ConnectExPtr),
                 &numBytes,
                 NULL,
                 NULL) == SOCKET_ERROR ||
        !base->ConnectExPtr) {
      if (tmpSocket != INVALID_SOCKET)
        closesocket(tmpSocket);
      CloseHandle(base->completionPort);
      free(base);
      return 0;
    }
    closesocket(tmpSocket);

    base->B.methodImpl = iocpImpl;
  }

  return (asyncBase*)base;
}

void iocpNextFinishedOperation(asyncBase *base)
{
  OVERLAPPED_ENTRY entries[128];
  const int maxEntriesNum = sizeof(entries) / sizeof(OVERLAPPED_ENTRY);
  iocpBase *localBase = (iocpBase*)base;
  if (!loopThreadEnter(base))
    return;

  while (1) {
    ULONG N, i;

    // An idle base gets UINT32_MAX from timerLoopPrepareSleep, which is
    // exactly INFINITE: the port then blocks until a completion, a posted
    // kick or the quit packet.
    BOOL status = GetQueuedCompletionStatusEx(localBase->completionPort,
                                              entries,
                                              maxEntriesNum,
                                              &N,
                                              timerLoopPrepareSleep(base, messageLoopThreadId, getMonotonicTicks(), 500),
                                              FALSE);
    timerLoopCancelSleep(base, messageLoopThreadId);

    // Unconditional sweep (the modulo election is gone): an idle pass costs
    // one relaxed load, and the wakeup handshake relies on whichever thread
    // the kick lands on doing the sweep itself
    processTimeoutQueue(base, getMonotonicTicks());

    // ignore false status
    if (status == FALSE)
      continue;

    for (i = 0; i < N; i++) {
      OVERLAPPED_ENTRY *entry = &entries[i];
      if (entry->lpCompletionKey) {
        if (entry->dwNumberOfBytesTransferred == IOCP_USER_EVENT_PACKET) {
          aioUserEvent *event = (aioUserEvent*)entry->lpCompletionKey;
          // A manually posted packet round-trips both payload fields verbatim;
          // this numeric OVERLAPPED value is never dereferenced.
          uint64_t fullGeneration = (uint64_t)(uintptr_t)entry->lpOverlapped;
          if (!eventTimerTryClaimReference(event, fullGeneration))
            continue;
          currentFinishedSync = 0;
          eventManualReady(event);
          eventDecrementReference(event, 1);
          continue;
        }
        asyncOpRoot *op = (asyncOpRoot*)entry->lpCompletionKey;
        if (eventIsQueueTask(op)) {
          eventExecuteQueuedTask(op);
        } else {
          currentFinishedSync = 0;
          if (op->flags & afCoroutine) {
            coroutineCall((coroutineTy*)op->finishMethod);
          } else {
            if (op->callback)
              op->finishMethod(op);
            releaseAsyncOp(op);
          }
        }
      } else if (entry->lpOverlapped) {
        iocpOp *op = (iocpOp*)(((uint8_t*)entry->lpOverlapped) - offsetof(struct iocpOp, overlapped));
        AsyncOpStatus result = iocpGetOverlappedResult(op);
        if (result == aosSuccess) {
          aioObject *object = (aioObject*)op->info.root.object;
          // Must mirror the submit-side test in iocpAsyncRead exactly: a read
          // of precisely the buffer capacity is posted into the object buffer,
          // and treating it as unbuffered here would account the bytes against
          // a user buffer that never received them.
          int isBuffered = op->info.root.opCode == actRead && op->info.transactionSize <= object->buffer.totalSize;
          if (!isBuffered)
            op->info.bytesTransferred += entry->dwNumberOfBytesTransferred;
          else
            object->buffer.dataSize = entry->dwNumberOfBytesTransferred;
          if (op->info.root.opCode == actAccept) {
            const DWORD addrSize = sizeof(struct sockaddr_storage) + 16;
            struct sockaddr *localAddr = 0;
            struct sockaddr *remoteAddr = 0;
            INT localAddrLength;
            INT remoteAddrLength;
            GetAcceptExSockaddrs(op->info.internalBuffer,
                                 entry->dwNumberOfBytesTransferred,
                                 addrSize,
                                 addrSize,
                                 &localAddr,
                                 &localAddrLength,
                                 &remoteAddr,
                                 &remoteAddrLength);
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
      } else if (entry->dwNumberOfBytesTransferred) {
        // Wakeup kick from a timer producer: the wait already returned and
        // the sweep at the loop top delivers, nothing to do with the entry
      } else {
        unsigned threadsRunning = loopThreadExit(base);
        if (threadsRunning)
          postEmptyOperation(base);
        return;
      }
    }
  }
}

aioObject *iocpNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  iocpBase *localBase = (iocpBase*)base;
  aioObject *object = (aioObject*)objectAlloc(&objectPool, sizeof(aioObject), 16);
  if (!object)
    return 0;

  HANDLE associated = NULL;
  switch (type) {
    case ioObjectDevice:
      object->hDevice = *(iodevTy*)data;
      associated = CreateIoCompletionPort(object->hDevice, localBase->completionPort, 0, 1);
      break;
    case ioObjectSocket:
      object->hSocket = *(socketTy*)data;
      associated = CreateIoCompletionPort((HANDLE)object->hSocket, localBase->completionPort, 0, 1);
      break;
    default: associated = localBase->completionPort; break;
  }
  if (!associated) {
    objectFree(&objectPool, object, sizeof(aioObject));
    return 0;
  }

  initObjectRoot(&object->root, base, type, (aioObjectDestructor*)iocpDeleteObject);

  object->buffer.offset = 0;
  object->buffer.dataSize = 0;
  ioBufferEnsureCapacity(&object->buffer, DEFAULT_SOCKET_BUFFER_SIZE);
  return object;
}

asyncOpRoot *iocpNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool)
{
  iocpOp *op = 0;
  if (asyncOpAlloc(base, sizeof(iocpOp), isRealTime, objectPool, objectTimerPool, (asyncOpRoot**)&op)) {
    op->info.internalBuffer = 0;
    op->info.internalBufferSize = 0;
  }

  // overlapped stays uninitialized here: every submit path clears it right
  // before handing it to the kernel, and nothing reads it earlier.
  return &op->info.root;
}

int iocpCancelAsyncOp(asyncOpRoot *opptr)
{
  aioObject *object = (aioObject*)opptr->object;
  iocpOp *op = (iocpOp*)opptr;
  switch (object->root.header.objectType) {
    case ioObjectDevice: CancelIoEx(object->hDevice, &op->overlapped); break;
    case ioObjectSocket: CancelIoEx((HANDLE)object->hSocket, &op->overlapped); break;
    default: break;
  }

  return 0;
}

void iocpDeleteObject(aioObject *object)
{
  switch (object->root.header.objectType) {
    case ioObjectDevice: CloseHandle(object->hDevice); break;
    case ioObjectSocket: closesocket(object->hSocket); break;
    default: break;
  }

  objectFree(&objectPool, object, sizeof(aioObject));
}

// Allocates and fully constructs a backend timer cell: a high-resolution
// waitable timer with a plain fallback, plus its threadpool wait. Returns
// NULL with nothing left behind when any stage fails, so the caller can
// simply retry the whole construction on a later arm.
static aioTimer *iocpNewTimerCell(asyncBase *base, void *target, unsigned kind)
{
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  if (!timer)
    return 0;
  iocpInitializeTimerState(timer, base, target, kind);
  timer->hTimer = CreateWaitableTimerExW(NULL, NULL, CREATE_WAITABLE_TIMER_HIGH_RESOLUTION, TIMER_ALL_ACCESS);
  if (!timer->hTimer)
    timer->hTimer = CreateWaitableTimer(NULL, FALSE, NULL);
  timer->wait = timer->hTimer ? CreateThreadpoolWait(iocpTimerCb, timer, NULL) : NULL;
  if (!timer->wait) {
    // A published timer always carries both handles; a partial cell is freed
    // here so the next arm retries the whole construction.
    if (timer->hTimer)
      CloseHandle(timer->hTimer);
    alignedFree(timer);
    return 0;
  }
  return timer;
}

void iocpInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  op->timerId = iocpNewTimerCell(base, op, IOCP_TIMER_KIND_OPERATION);
}

void iocpStartTimer(asyncOpRoot *op)
{
  // The paired cell is created once per pooled slot; a constructor-time
  // allocation failure must not disable this slot's timeouts forever.
  aioTimer *timer = (aioTimer*)opEnsureTimerCell(op);
  if (!timer) {
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
    return;
  }
  timer->target = op;
  timer->object = op->object;
  timer->header.base = op->object->header.base;
  timer->objectGeneration = objectHeaderGeneration(&op->object->header);
  __uint64_atomic_store(&timer->header.tag.high, opGetGeneration(op), amoRelaxed);
  if (__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) != IOCP_TIMER_STOPPED)
    iocpDisarmWaitableTimer(timer);
  __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_ARMED, amoRelease);
  if (!iocpArmWaitableTimer(timer, op->timeout)) {
    __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_STOPPED, amoRelaxed);
    (void)opCancel(op, objectHeaderGeneration(&timer->header), aosUnknownError, timer->object, timer->objectGeneration);
  }
}

void iocpStopTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (timer)
    iocpDisarmWaitableTimer(timer);
}

int iocpActivate(aioUserEvent *event)
{
  // Manual packets provide two machine-word payloads, so IOCP does not need
  // the reactor's compact pointer/generation encoding.
  return PostQueuedCompletionStatus(((iocpBase*)event->header.base)->completionPort,
                                    IOCP_USER_EVENT_PACKET,
                                    (ULONG_PTR)event,
                                    (LPOVERLAPPED)(uintptr_t)eventHandleGeneration(event)) != FALSE;
}

int iocpInitializeUserEvent(aioUserEvent *event)
{
  (void)event;
  return 1;
}

static aioTimer *iocpEnsureEventTimer(aioUserEvent *event)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  if (timer)
    return timer;
  timer = iocpNewTimerCell(event->header.base, event, IOCP_TIMER_KIND_USER_EVENT);
  if (!timer)
    return 0;
  eventTimerStore(event, timer, amoRelaxed);
  return timer;
}

static int iocpArmEventTimer(aioUserEvent *event, aioTimer *timer, uint32_t generation, uint64_t period)
{
  __uint64_atomic_store(&timer->header.tag.high, generation, amoRelaxed);
  __uint64_atomic_store(&timer->eventGeneration, eventHandleGeneration(event), amoRelaxed);
  __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_ARMED, amoRelease);
  if (iocpArmWaitableTimer(timer, period))
    return 1;
  __uint64_atomic_store(&timer->header.tag.low, IOCP_TIMER_STOPPED, amoRelaxed);
  return 0;
}

int iocpUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  switch (update) {
    case etuStop:
      assert(timer && "Stopping a user-event timer which was never armed");
      // A user-event timer callback changes CALLBACK to STOPPED before it
      // enters the common tick/owner path. Waiting for that same callback
      // here would deadlock on a finite timer's last tick. The callback has
      // already copied every timer payload it uses, and final destruction
      // still performs the unconditional callback rendezvous below.
      if (__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) != IOCP_TIMER_STOPPED)
        iocpDisarmWaitableTimer(timer);
      return 1;

    case etuStart:
      if (!timer) {
        timer = iocpEnsureEventTimer(event);
        if (!timer)
          return 0;
      }
      assert(__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) == IOCP_TIMER_STOPPED &&
             "Arming a user-event timer before its predecessor reached STOPPED");
      return iocpArmEventTimer(event, timer, generation, period);
  }
  return 0;
}

uint64_t iocpConsumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  aioTimerUserEventState *state = eventTimerState(timer);
  uint128 control = __uint128_atomic_load_relaxed(&event->timerControl);
  if (eventTimerControlGeneration(control) == generation && eventTimerControlPeriod(control) && !(control.low & EVENT_TIMER_TERMINAL) &&
      (!eventTimerControlIsFinite(control) || state->remaining > published)) {
    if (!iocpArmEventTimer(event, timer, generation, period))
      state->armed = 0;
  }
  return published;
}

void iocpReleaseUserEvent(aioUserEvent *event)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  if (timer) {
    iocpDisarmWaitableTimer(timer);
    CloseThreadpoolWait(timer->wait);
    CloseHandle(timer->hTimer);
    eventTimerStore(event, 0, amoRelaxed);
    alignedFree(timer);
  }
  objectFree(&event->header.base->eventPool, event, sizeof(aioUserEvent));
}

AsyncOpStatus iocpAsyncConnect(asyncOpRoot *opptr)
{
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);
  iocpBase *localBase = (iocpBase*)object->root.header.base;

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

  const DWORD addrSize = sizeof(struct sockaddr_storage) + 16;
  const size_t acceptResultSize = 2 * (size_t)addrSize;
  asyncOpEnsureInternalBuffer(&op->info.internalBuffer, &op->info.internalBufferSize, acceptResultSize);

  struct sockaddr_storage listenAddr;
  int listenAddrLen = sizeof(listenAddr);
  getsockname(object->hSocket, (struct sockaddr*)&listenAddr, &listenAddrLen);

  u_long arg = 1;
  op->info.acceptSocket = WSASocket(listenAddr.ss_family, SOCK_STREAM, IPPROTO_TCP, NULL, 0, WSA_FLAG_OVERLAPPED);
  ioctlsocket(op->info.acceptSocket, FIONBIO, &arg);

  memset(&op->overlapped, 0, sizeof(op->overlapped));
  int result = AcceptEx(object->hSocket, op->info.acceptSocket, op->info.internalBuffer, 0, addrSize, addrSize, NULL, &op->overlapped);

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
  // A partial hit from the read-ahead buffer completes a non-afWaitAll read,
  // matching the reactor backends: parking here would hold already delivered
  // bytes hostage to future traffic.
  if (op->info.bytesTransferred != 0 && !(opptr->flags & afWaitAll))
    return aosSuccess;

  if (op->info.transactionSize <= object->buffer.totalSize) {
    memset(&op->overlapped, 0, sizeof(op->overlapped));
    if (object->root.header.objectType == ioObjectDevice) {
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
        return iocpInlineErrorStatus();
    }
  } else {
    wsabuf.buf = (CHAR*)op->info.buffer + op->info.bytesTransferred;
    wsabuf.len = (ULONG)(op->info.transactionSize - op->info.bytesTransferred);
    memset(&op->overlapped, 0, sizeof(op->overlapped));
    if (object->root.header.objectType == ioObjectDevice) {
      // TODO: check totalSize > 4Gb
      int result = ReadFile(object->hDevice,
                            (CHAR*)op->info.buffer + op->info.bytesTransferred,
                            (DWORD)(op->info.transactionSize - op->info.bytesTransferred),
                            0,
                            &op->overlapped);
      if (result == TRUE || GetLastError() == WSA_IO_PENDING)
        return aosPending;
      else
        return aosUnknownError;
    } else {
      int result = WSARecv(object->hSocket, &wsabuf, 1, NULL, &flags, &op->overlapped, NULL);
      if (result == 0 || WSAGetLastError() == WSA_IO_PENDING)
        return aosPending;
      else
        return iocpInlineErrorStatus();
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
  if (object->root.header.objectType == ioObjectDevice) {
    // TODO: check totalSize > 4Gb
    BOOL result = WriteFile(object->hDevice,
                            (CHAR*)op->info.buffer + op->info.bytesTransferred,
                            (DWORD)(op->info.transactionSize - op->info.bytesTransferred),
                            0,
                            &op->overlapped);
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
      return iocpInlineErrorStatus();
  }
}

AsyncOpStatus iocpAsyncReadMsg(asyncOpRoot *opptr)
{
  WSABUF wsabuf;
  iocpOp *op = (iocpOp*)opptr;
  aioObject *object = getObject(op);

  const size_t acceptResultSize = sizeof(recvFromData);
  asyncOpEnsureInternalBuffer(&op->info.internalBuffer, &op->info.internalBufferSize, acceptResultSize);

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
    return iocpInlineErrorStatus();
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
    return iocpInlineErrorStatus();
  }
}
