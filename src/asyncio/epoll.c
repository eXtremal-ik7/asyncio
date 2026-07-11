#include "asyncioImpl.h"
#include "reactorTimer.h"
#include "asyncio/coroutine.h"
#include "atomic.h"

#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <sys/timerfd.h>

static ConcurrentQueue objectPool;

#define MAX_EVENTS 256

__NO_PADDING_BEGIN
typedef struct epollBase {
  asyncBase B;
  int epollFd;
  int eventFd;
  aioObject *eventObject;
} epollBase;

typedef struct EPollObject {
  aioObject Object;
  // Whether the fd is currently in the epoll set. Registration is lazy:
  // EPOLLERR/EPOLLHUP ignore the requested mask, so an idle object with a
  // pending error condition would wake every epoll_wait if its fd stayed
  // in the set with an empty mask. Only touched under the object combiner.
  uint32_t Registered;
} EPollObject;
__NO_PADDING_END

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig);
void epollEnqueue(asyncBase *base, asyncOpRoot *op);
void epollPostEmptyOperation(asyncBase *base);
void epollNextFinishedOperation(asyncBase *base);
aioObject *epollNewAioObject(asyncBase *base, IoObjectTy type, void *data);
asyncOpRoot *epollNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool);
int epollCancelAsyncOp(asyncOpRoot *opptr);
void epollDeleteObject(aioObject *object);
void epollInitializeTimer(asyncBase *base, asyncOpRoot *op);
void epollStartTimer(asyncOpRoot *op);
void epollStopTimer(asyncOpRoot *op);
void epollDeleteTimer(asyncOpRoot *op);
void epollActivate(aioUserEvent *op);
AsyncOpStatus epollAsyncConnect(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncAccept(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncRead(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncWrite(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncReadMsg(asyncOpRoot *op);
AsyncOpStatus epollAsyncWriteMsg(asyncOpRoot *op);

static struct asyncImpl epollImpl = {
  combinerTaskHandler,
  epollEnqueue,
  epollPostEmptyOperation,
  epollNextFinishedOperation,
  epollNewAioObject,
  epollNewAsyncOp,
  epollCancelAsyncOp,
  epollDeleteObject,
  epollInitializeTimer,
  epollStartTimer,
  epollStopTimer,
  epollDeleteTimer,
  epollActivate,
  epollAsyncConnect,
  epollAsyncAccept,
  epollAsyncRead,
  epollAsyncWrite,
  epollAsyncReadMsg,
  epollAsyncWriteMsg
};

static void epollControl(int epollFd, int action, uint32_t events, int fd, void *ptr)
{
  struct epoll_event ev;
  ev.events = events;
  ev.data.ptr = ptr;
  if (epoll_ctl(epollFd,
                action,
                fd,
                &ev) == -1)
    fprintf(stderr, "epoll_ctl error, errno: %s\n", strerror(errno));
}

static int getFd(EPollObject *object)
{
  switch (object->Object.root.type) {
    case ioObjectDevice :
      return object->Object.hDevice;
    case ioObjectSocket :
      return object->Object.hSocket;
    default :
      return -1;
  }
}

static uint32_t epollEvents(uint32_t ioEvents)
{
  uint32_t events = 0;
  if (ioEvents & IO_EVENT_READ)
    events |= EPOLLIN;
  if (ioEvents & IO_EVENT_WRITE)
    events |= EPOLLOUT;
  return events;
}

asyncBase *epollNewAsyncBase()
{
  epollBase *base = malloc(sizeof(epollBase));
  if (base) {
    base->eventFd = eventfd(0, EFD_NONBLOCK);
    base->B.methodImpl = epollImpl;
    base->epollFd = epoll_create(MAX_EVENTS);
    if (base->epollFd == -1) {
      fprintf(stderr, " * epollNewAsyncBase: epoll_create failed\n");
    }

    base->eventObject = epollNewAioObject(&base->B, ioObjectDevice, &base->eventFd);

    // The event fd bypasses the combiner re-arm logic (the message loop
    // re-arms it directly), so register it here explicitly
    epollControl(base->epollFd, EPOLL_CTL_ADD, EPOLLIN | EPOLLONESHOT, base->eventFd, base->eventObject);
    ((EPollObject*)base->eventObject)->Registered = 1;
  }

  return (asyncBase *)base;
}

void epollEnqueue(asyncBase *base, asyncOpRoot *op)
{
  epollBase *localBase = (epollBase*)base;
  concurrentQueuePush(&base->globalQueue, op);
  eventfd_write(localBase->eventFd, 1);
}

void epollPostEmptyOperation(asyncBase *base)
{
  epollEnqueue(base, 0);
}

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, uint32_t sig)
{
  EPollObject *fdObject = (object->type == ioObjectDevice || object->type == ioObjectSocket) ? (EPollObject*)object : 0;
  uint32_t oldIoEvents = fdObject ? combinerActiveIoEvents(object) : 0;
  uint32_t progress = sig & COMBINER_TAG_PROGRESS_MASK;
  uint32_t ioEvents = fdObject
    ? progress | ((sig & COMBINER_TAG_ERROR) ? IO_EVENT_ERROR : 0)
    : 0;

  // A dying object gets no fd readiness processing: its operations are being
  // cancelled wholesale anyway, and for a stale batch entry landing here
  // within the grace period the descriptor is already closed - the error
  // path ioctl and the rearm epoll_ctl would run on a dead or reused fd
  if (fdObject && __uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    ioEvents = 0;
    progress = 0;
  }

  // READ/WRITE tag values deliberately match IO_EVENT_READ/WRITE.
  uint32_t needStart = progress;

  // Start a submitted operation before completing initialization from the
  // accumulated event: a start delivered together with the event must
  // enter its queue first, otherwise a failed connect cancels the queues
  // without it and the operation would start on the dead socket afterwards
  if (op)
    startOperation(op, &needStart);

  // By contract initialization precedes ordinary I/O, so any progress while
  // its slot is occupied belongs to it. Drive it before the disconnect sweep
  // so queued operations inherit a connect failure, not aosDisconnected.
  if (progress && __uintptr_atomic_load(&object->initializationOp, amoRelaxed))
    processInitializationOp(object, &needStart);

  // CANCEL: a timeout/opCancel/cancelIo set the status and asked for a scan
  if (sig & COMBINER_TAG_CANCEL)
    reapObject(object, &needStart);

  if (ioEvents & IO_EVENT_ERROR) {
    // EPOLLRDHUP mapped to TAG_ERROR, cancel all operations with aosDisconnected status
    int available;
    int fd = getFd(fdObject);
    ioctl(fd, FIONREAD, &available);
    if (available == 0)
      cancelOperationList(&object->readQueue, aosDisconnected);
    cancelOperationList(&object->writeQueue, aosDisconnected);
  }

  if (needStart & IO_EVENT_READ)
    executeOperationList(&object->readQueue);
  if (needStart & IO_EVENT_WRITE)
    executeOperationList(&object->writeQueue);

  if (fdObject && !__uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    int fd = getFd(fdObject);
    epollBase *base = (epollBase*)object->base;
    uint32_t currentEvents = epollEvents(oldIoEvents);
    uint32_t newEvents;

    // Calculate the current mask because no fd->mask map is kept. Any delivered
    // event consumes the EPOLLONESHOT shot, whatever direction it carries.
    if (ioEvents)
      currentEvents = 0;

    newEvents = epollEvents(combinerActiveIoEvents(object));

    if (newEvents) {
      if (!fdObject->Registered) {
        epollControl(base->epollFd, EPOLL_CTL_ADD, newEvents | EPOLLONESHOT | EPOLLRDHUP, fd, object);
        fdObject->Registered = 1;
      } else if (currentEvents != newEvents) {
        epollControl(base->epollFd, EPOLL_CTL_MOD, newEvents | EPOLLONESHOT | EPOLLRDHUP, fd, object);
      }
    } else if (currentEvents) {
      // No operation left on an armed fd: remove it from the set instead of
      // keeping it with an empty mask - EPOLLERR/EPOLLHUP ignore the mask and
      // would wake every epoll_wait for as long as the error condition holds.
      // An fd disarmed by a delivered EPOLLONESHOT shot stays fully silent,
      // so that state is left in the set as is
      epollControl(base->epollFd, EPOLL_CTL_DEL, 0, fd, object);
      fdObject->Registered = 0;
    }
  }
}

void epollNextFinishedOperation(asyncBase *base)
{
  int nfds, n;
  struct epoll_event events[MAX_EVENTS];
  epollBase *localBase = (epollBase *)base;
  messageLoopThreadId = __sync_fetch_and_add(&base->messageLoopThreadCounter, 1);
  graceThreadEnter(base);

  while (1) {
    do {
      if (!executeGlobalQueue(base)) {
        // Found quit marker: stamp the grace slot out and drain, strictly
        // before the loop thread counter drops
        graceThreadExit(base);
        unsigned threadsRunning = __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, 0u-1) - 1;
        if (threadsRunning)
          epollEnqueue(base, 0);
        return;
      }

      graceQuiesce(base);
      nfds = epoll_wait(localBase->epollFd, events, MAX_EVENTS, 500);
      uint64_t currentTime = getMonotonicSeconds();
      unsigned loopThreadCount = __uint_atomic_load(&base->messageLoopThreadCounter, amoRelaxed);
      if (currentTime % loopThreadCount == messageLoopThreadId)
        processTimeoutQueue(base, currentTime);
    } while (nfds <= 0 && errno == EINTR);

    for (n = 0; n < nfds; n++) {
      uintptr_t timerId;
      aioObjectRoot *object;
      __tagged_pointer_decode(events[n].data.ptr, (void**)&object, &timerId);
      if (object == &localBase->eventObject->root) {
        eventfd_t eventValue;
        eventfd_read(localBase->eventFd, &eventValue);
        epollControl(localBase->epollFd, EPOLL_CTL_MOD, EPOLLIN | EPOLLONESHOT, localBase->eventFd, object);
      } else if (timerId & udataTimer) {
        uint64_t data;
        aioTimer *timer = (aioTimer*)object;
        if (read((int)timer->fd, &data, sizeof(data))) {
          uintptr_t armedGeneration = 0;
          switch (reactorTimerDecodeEvent(timer, timerId, &armedGeneration)) {
            case rteIgnore:
              // stale doorbell, timer disarmed
              break;

            case rteUserEvent: {
              aioUserEvent *event = (aioUserEvent*)timer->op;
              if (eventTryActivate(event)) {
                if (event->counter > 0 && --event->counter == 0) {
                  epollStopTimer(&event->root);
                } else {
                  // We need rearm epoll for timer
                  epollControl(localBase->epollFd,
                               EPOLL_CTL_MOD,
                               EPOLLIN | EPOLLONESHOT,
                               (int)timer->fd,
                               __tagged_pointer_make(timer, udataTimer | udataUserEvent));
                }

                eventDeactivate(event);
                event->root.finishMethod(&event->root);
                eventDecrementReference(event, 1);
              }
              break;
            }

            case rteExpireOperation:
              opCancel(timer->op, armedGeneration, aosTimeout);
              break;
          }
        }
      } else {
        uint32_t eventMask = 0;
        if (events[n].events & EPOLLIN)
          eventMask |= IO_EVENT_READ;
        if (events[n].events & EPOLLOUT)
          eventMask |= IO_EVENT_WRITE;
        if (events[n].events & EPOLLRDHUP)
          eventMask |= IO_EVENT_ERROR;
        // EPOLLERR/EPOLLHUP are reported regardless of the requested mask and
        // consume the EPOLLONESHOT shot like any other event. Wake both queues
        // so parked operations retry their syscall and report the real errno.
        // IO_EVENT_ERROR does not fit here: its contract is "peer closed"
        // (EPOLLRDHUP) and it would misreport e.g. ECONNREFUSED as aosDisconnected
        if (events[n].events & (EPOLLERR | EPOLLHUP))
          eventMask |= IO_EVENT_READ | IO_EVENT_WRITE;

        if (eventMask) {
          uint32_t bits = 0;
          if (eventMask & IO_EVENT_READ)  bits |= COMBINER_TAG_PROGRESS_READ;
          if (eventMask & IO_EVENT_WRITE) bits |= COMBINER_TAG_PROGRESS_WRITE;
          // EPOLLRDHUP alone carries no direction. ERROR is passed in Head with
          // the readiness bits, so no object-side mailbox is needed.
          if (eventMask & IO_EVENT_ERROR)
            bits |= COMBINER_TAG_ERROR | COMBINER_TAG_PROGRESS_READ | COMBINER_TAG_PROGRESS_WRITE;
          combinerPushCounter(object, bits);
        }
      }
    }
  }
}


aioObject *epollNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  EPollObject *object = 0;
  if (!objectPoolGet(&objectPool, (void**)&object, sizeof(EPollObject))) {
    object = alignedMalloc(sizeof(EPollObject), TAGGED_POINTER_ALIGNMENT);
    object->Object.buffer.ptr = 0;
    object->Object.buffer.totalSize = 0;
  }

  initObjectRoot(&object->Object.root, base, type, (aioObjectDestructor*)epollDeleteObject);
  switch (type) {
    case ioObjectDevice :
      object->Object.hDevice = *(iodevTy *)data;
      break;
    case ioObjectSocket :
      object->Object.hSocket = *(socketTy *)data;
      break;
    default :
      break;
  }

  object->Registered = 0;
  object->Object.buffer.offset = 0;
  object->Object.buffer.dataSize = 0;
  return &object->Object;
}

asyncOpRoot *epollNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool)
{
  asyncOp *op = 0;
  if (asyncOpAlloc(base, sizeof(asyncOp), isRealTime, objectPool, objectTimerPool, (asyncOpRoot**)&op)) {
    op->internalBuffer = 0;
    op->internalBufferSize = 0;
  }

  return &op->root;
}

int epollCancelAsyncOp(asyncOpRoot *opptr)
{
  __UNUSED(opptr);
  return 1;
}

// Memory half of the object destructor, runs via graceRetire once no loop
// thread can hold the object in an already-harvested event batch
static void epollReleaseObject(aioObject *object)
{
  objectPoolPut(&objectPool, object, sizeof(EPollObject));
}

void epollDeleteObject(aioObject *object)
{
  epollBase *localBase = (epollBase*)object->root.base;
  int registered = ((EPollObject*)object)->Registered;
  switch (object->root.type) {
    case ioObjectDevice :
      if (registered)
        epollControl(localBase->epollFd, EPOLL_CTL_DEL, 0, object->hDevice, 0);
      close(object->hDevice);
      object->hDevice = -1;
      break;
    case ioObjectSocket :
      if (registered)
        epollControl(localBase->epollFd, EPOLL_CTL_DEL, 0, object->hSocket, 0);
      close(object->hSocket);
      object->hSocket = -1;
      break;
    default :
      break;
  }

  // The descriptor is gone from the epoll set: batches harvested from now on
  // cannot reference the object, batches already in loop thread buffers gate
  // the memory release through the grace period
  graceRetire(object->root.base, &object->root, (aioObjectDestructor*)epollReleaseObject);
}

void epollInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  epollBase *localBase = (epollBase*)base;
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  timer->root.base = base;
  timer->root.type = ioObjectTimer;
  timer->tag = 0;
  timer->fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
  timer->op = op;
  epollControl(localBase->epollFd, EPOLL_CTL_ADD, 0, (int)timer->fd, timer);
  op->timerId = timer;
}

void epollStartTimer(asyncOpRoot *op)
{
  struct itimerspec its;
  int periodic = op->opCode == actUserEvent;
  its.it_value.tv_sec = op->timeout / 1000000;
  its.it_value.tv_nsec = (op->timeout % 1000000) * 1000;
  its.it_interval.tv_sec = periodic ? its.it_value.tv_sec : 0;
  its.it_interval.tv_nsec = periodic ? its.it_value.tv_nsec : 0;

  aioTimer *timer = (aioTimer*)op->timerId;
  void *udata = reactorTimerArm(timer, op);
  timerfd_settime((int)timer->fd, 0, &its, 0);
  epollControl(((epollBase*)timer->root.base)->epollFd,
               EPOLL_CTL_MOD,
               EPOLLIN | EPOLLONESHOT,
               (int)timer->fd,
               udata);
}


void epollStopTimer(asyncOpRoot *op)
{
  uint64_t data;
  struct itimerspec its;
  memset(&its, 0, sizeof(its));
  aioTimer *timer = (aioTimer*)op->timerId;
  reactorTimerDisarm(timer);
  timerfd_settime((int)timer->fd, 0, &its, 0);
  epollControl(((epollBase*)timer->root.base)->epollFd, EPOLL_CTL_MOD, 0, (int)timer->fd, &timer->root);
  while (read((int)timer->fd, &data, sizeof(data)) > 0)
    continue;
}

// Memory half only: the timer pointer is published to the kernel as epoll_data
// and may still sit in another loop thread's harvested batch, so the release
// goes through the grace period like any other kernel-published object
static void epollReleaseTimer(aioObjectRoot *object)
{
  free(object);
}

void epollDeleteTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  close((int)timer->fd);
  graceRetire(timer->root.base, &timer->root, epollReleaseTimer);
}

void epollActivate(aioUserEvent *op)
{
  epollEnqueue(op->base, &op->root);
}


AsyncOpStatus epollAsyncConnect(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((EPollObject*)op->root.object);
  if (op->state == 0) {
    op->state = 1;
    struct sockaddr_storage sa;
    socklen_t saLen = hostAddressToSockaddr(&op->host, &sa);
    int result = connect(fd, (struct sockaddr *)&sa, saLen);
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


AsyncOpStatus epollAsyncAccept(asyncOpRoot *opptr)
{
  struct sockaddr_storage clientAddr;
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((EPollObject*)op->root.object);
  socklen_t clientAddrSize = sizeof(clientAddr);
  op->acceptSocket =
    accept(fd, (struct sockaddr *)&clientAddr, &clientAddrSize);

  if (op->acceptSocket != -1) {
    int current = fcntl(op->acceptSocket, F_GETFL);
    fcntl(op->acceptSocket, F_SETFL, O_NONBLOCK | current);
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


AsyncOpStatus epollAsyncRead(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  EPollObject *object = (EPollObject*)op->root.object;
  struct ioBuffer *sb = &object->Object.buffer;
  int fd = getFd(object);

  if (copyFromBuffer(op->buffer, &op->bytesTransferred, sb, op->transactionSize))
    return aosSuccess;

  if (op->transactionSize <= object->Object.buffer.totalSize) {
    while (op->bytesTransferred < op->transactionSize) {
      ssize_t bytesRead = read(fd, sb->ptr, sb->totalSize);
      if (bytesRead == 0)
        return aosDisconnected;
      else if (bytesRead < 0)
        return errno == EAGAIN ? aosPending : aosUnknownError;
      sb->dataSize = (size_t)bytesRead;

      if (copyFromBuffer(op->buffer, &op->bytesTransferred, sb, op->transactionSize) || !(opptr->flags & afWaitAll))
        break;
    }

    return aosSuccess;
  } else {
    ssize_t bytesRead = read(fd,
                             (uint8_t *)op->buffer + op->bytesTransferred,
                             op->transactionSize - op->bytesTransferred);

    if (bytesRead > 0) {
      op->bytesTransferred += (size_t)bytesRead;
      if (op->root.flags & afWaitAll && op->bytesTransferred < op->transactionSize)
        return aosPending;
      else
        return aosSuccess;
    } else if (bytesRead == 0) {
      return op->transactionSize - op->bytesTransferred > 0 ? aosDisconnected : aosSuccess;
    } else {
      return errno == EAGAIN ? aosPending : aosUnknownError;
    }
  }
}


AsyncOpStatus epollAsyncWrite(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  EPollObject *object = (EPollObject*)op->root.object;
  int fd = getFd(object);

  ssize_t bytesWritten;
  if (object->Object.root.type == ioObjectSocket) {
    bytesWritten = send(fd, (uint8_t *)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred, MSG_NOSIGNAL);
  } else if (object->Object.needSigpipeGuard && !sigpipeIgnored) {
    struct SigpipeGuard guard;
    sigpipeGuardEnter(&guard);
    bytesWritten = write(fd, (uint8_t *)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
    sigpipeGuardLeave(&guard, bytesWritten == -1 && errno == EPIPE);
  } else {
    bytesWritten = write(fd, (uint8_t *)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
  }
  if (bytesWritten > 0) {
    op->bytesTransferred += (size_t)bytesWritten;
    if (op->root.flags & afWaitAll && op->bytesTransferred < op->transactionSize)
      return aosPending;
    else
      return aosSuccess;
  } else if (bytesWritten == 0) {
    return op->transactionSize - op->bytesTransferred > 0 ? aosDisconnected : aosSuccess;
  } else {
    return errno == EAGAIN ? aosPending : aosUnknownError;
  }
}


AsyncOpStatus epollAsyncReadMsg(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((EPollObject*)op->root.object);

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
    return errno == EAGAIN ? aosPending : aosUnknownError;
  }
}


AsyncOpStatus epollAsyncWriteMsg(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  int fd = getFd((EPollObject*)op->root.object);

  struct sockaddr_storage remoteAddress;
  socklen_t addrLen = hostAddressToSockaddr(&op->host, &remoteAddress);
  ssize_t result = sendto(fd, op->buffer, op->transactionSize, 0, (struct sockaddr *)&remoteAddress, addrLen);
  if (result != -1) {
    return aosSuccess;
  }

  return errno == EAGAIN ? aosPending : aosUnknownError;
}
