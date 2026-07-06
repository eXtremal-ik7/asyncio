#include "asyncioImpl.h"
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
  uint32_t IoEvents;
} EPollObject;

typedef struct aioTimer {
  aioObjectRoot root;
  int fd;
  asyncOpRoot *op;
} aioTimer;
__NO_PADDING_END

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, AsyncOpActionTy opMethod);
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

    epollControl(base->epollFd, EPOLL_CTL_MOD, EPOLLIN | EPOLLONESHOT, base->eventFd, base->eventObject);
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

void combinerTaskHandler(aioObjectRoot *object, asyncOpRoot *op, AsyncOpActionTy opMethod)
{
  EPollObject *fdObject = (object->type == ioObjectDevice || object->type == ioObjectSocket) ? (EPollObject*)object : 0;
  uint32_t ioEvents = fdObject ? fdObject->IoEvents : 0;

  int hasReadOp = object->readQueue.head != 0;
  int hasWriteOp = object->writeQueue.head != 0;
  if (ioEvents & IO_EVENT_ERROR) {
    // EPOLLRDHUP mapped to TAG_ERROR, cancel all operations with aosDisconnected status
    int available;
    int fd = getFd(fdObject);
    ioctl(fd, FIONREAD, &available);
    if (available == 0)
      cancelOperationList(&object->readQueue, aosDisconnected);
    cancelOperationList(&object->writeQueue, aosDisconnected);
  }

  uint32_t needStart = ioEvents;
  if (op)
    processAction(op, opMethod, &needStart);
  if (needStart & IO_EVENT_READ)
    executeOperationList(&object->readQueue);
  if (needStart & IO_EVENT_WRITE)
    executeOperationList(&object->writeQueue);

  if (fdObject) {
    int fd = getFd(fdObject);
    epollBase *base = (epollBase*)object->base;
    uint32_t currentEvents = 0;
    uint32_t newEvents = 0;

    // "Calculate" current epoll_ctl mask because we don't have map<fd, oldMask>
    // EPOLLIN/EPOLLOUT now enabled if read/write queue was not empty and file descriptor not deactivated by EPOLLONESHOT flag
    int fdDeactivated = (ioEvents & (IO_EVENT_READ | IO_EVENT_WRITE)) != 0;

    if (hasReadOp)
      currentEvents |= EPOLLIN;
    if (hasWriteOp)
      currentEvents |= EPOLLOUT;
    if (fdDeactivated)
      currentEvents = 0;

    if (object->readQueue.head)
      newEvents |= EPOLLIN;
    if (object->writeQueue.head)
      newEvents |= EPOLLOUT;

    if (ioEvents)
      fdObject->IoEvents = 0;
    if (currentEvents != newEvents) {
      epollControl(base->epollFd, EPOLL_CTL_MOD, newEvents ? newEvents | EPOLLONESHOT | EPOLLRDHUP : 0, fd, object);
    }
  }
}

void epollNextFinishedOperation(asyncBase *base)
{
  int nfds, n;
  struct epoll_event events[MAX_EVENTS];
  epollBase *localBase = (epollBase *)base;
  messageLoopThreadId = __sync_fetch_and_add(&base->messageLoopThreadCounter, 1);

  while (1) {
    do {
      if (!executeGlobalQueue(base)) {
        // Found quit marker
        unsigned threadsRunning = __uint_atomic_fetch_and_add(&base->messageLoopThreadCounter, 0u-1) - 1;
        if (threadsRunning)
          epollEnqueue(base, 0);
        return;
      }

      nfds = epoll_wait(localBase->epollFd, events, MAX_EVENTS, 500);
      time_t currentTime = time(0);
      if (currentTime % base->messageLoopThreadCounter == messageLoopThreadId)
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
      } else if (object->type == ioObjectTimer) {
        uint64_t data;
        aioTimer *timer = (aioTimer*)object;
        if (read(timer->fd, &data, sizeof(data))) {
          asyncOpRoot *op = timer->op;
          if (op->opCode == actUserEvent) {
            aioUserEvent *event = (aioUserEvent*)op;
            if (eventTryActivate(event)) {
              // TODO: compare timer and event tag
              if (event->counter > 0 && --event->counter == 0) {
                epollStopTimer(op);
              } else {
                // We need rearm epoll for timer
                epollControl(localBase->epollFd,
                             EPOLL_CTL_MOD,
                             EPOLLIN | EPOLLONESHOT,
                             timer->fd,
                             __tagged_pointer_make(timer, opGetGeneration(op)));
              }

              eventDeactivate(event);
              op->finishMethod(op);
              eventDecrementReference(event, 1);
            }
          } else {
            opCancel(op, opEncodeTag(op, timerId), aosTimeout);
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

        if (eventMask) {
          ((EPollObject*)object)->IoEvents = eventMask;
          combinerPushCounter(object, COMBINER_TAG_ACCESS);
        }
      }
    }
  }
}


aioObject *epollNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  epollBase *localBase = (epollBase*)base;
  EPollObject *object = 0;
  if (!concurrentQueuePop(&objectPool, (void**)&object)) {
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

  object->IoEvents = 0;
  object->Object.buffer.offset = 0;
  object->Object.buffer.dataSize = 0;
  epollControl(localBase->epollFd, EPOLL_CTL_ADD, 0, getFd(object), object);
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

void epollDeleteObject(aioObject *object)
{
  epollBase *localBase = (epollBase*)object->root.base;
  switch (object->root.type) {
    case ioObjectDevice :
      epollControl(localBase->epollFd, EPOLL_CTL_DEL, 0, object->hDevice, 0);
      close(object->hDevice);
      object->hDevice = -1;
      break;
    case ioObjectSocket :
      epollControl(localBase->epollFd, EPOLL_CTL_DEL, 0, object->hSocket, 0);
      close(object->hSocket);
      object->hSocket = -1;
      break;
    default :
      break;
  }

  concurrentQueuePush(&objectPool, object);
}

void epollInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  epollBase *localBase = (epollBase*)base;
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  timer->root.base = base;
  timer->root.type = ioObjectTimer;
  timer->fd = timerfd_create(CLOCK_REALTIME, TFD_NONBLOCK);
  timer->op = op;
  epollControl(localBase->epollFd, EPOLL_CTL_ADD, 0, timer->fd, timer);
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
  timerfd_settime(timer->fd, 0, &its, 0);
  epollControl(((epollBase*)timer->root.base)->epollFd,
               EPOLL_CTL_MOD,
               EPOLLIN | EPOLLONESHOT,
               timer->fd,
               __tagged_pointer_make(timer, opGetGeneration(op)));
}


void epollStopTimer(asyncOpRoot *op)
{
  uint64_t data;
  struct itimerspec its;
  memset(&its, 0, sizeof(its));
  aioTimer *timer = (aioTimer*)op->timerId;
  timerfd_settime(timer->fd, 0, &its, 0);
  epollControl(((epollBase*)timer->root.base)->epollFd, EPOLL_CTL_MOD, 0, timer->fd, &timer->root);
  while (read(timer->fd, &data, sizeof(data)) > 0)
    continue;
}

void epollDeleteTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  close(timer->fd);
  free(timer);
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
  socklen_t addrlen = sizeof(source);
  ssize_t result = recvfrom(fd, op->buffer, op->transactionSize, 0, (struct sockaddr*)&source, &addrlen);
  if (result != -1) {
    sockaddrToHostAddress(&source, &op->host);
    op->bytesTransferred = (size_t)result;
    return aosSuccess;
  } else {
    if (errno == EAGAIN)
      return aosPending;
    if (errno == ENOMEM)
      return aosBufferTooSmall;
    else
      return aosUnknownError;
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
