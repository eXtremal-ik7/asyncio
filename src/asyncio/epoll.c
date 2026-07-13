#include "asyncioImpl.h"
#include "reactorTimer.h"
#include "asyncio/coroutine.h"
#include "atomic.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <malloc.h>
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
void epollWakeupLoop(asyncBase *base);
void epollNextFinishedOperation(asyncBase *base);
aioObject *epollNewAioObject(asyncBase *base, IoObjectTy type, void *data);
asyncOpRoot *epollNewAsyncOp(asyncBase *base, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool);
int epollCancelAsyncOp(asyncOpRoot *opptr);
void epollDeleteObject(aioObject *object);
void epollInitializeTimer(asyncBase *base, asyncOpRoot *op);
void epollStartTimer(asyncOpRoot *op);
void epollStopTimer(asyncOpRoot *op);
int epollActivate(aioUserEvent *op);
int epollUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uintptr_t generation, uint64_t period);
void epollReleaseUserEvent(aioUserEvent *event);
AsyncOpStatus epollAsyncConnect(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncAccept(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncRead(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncWrite(asyncOpRoot *opptr);
AsyncOpStatus epollAsyncReadMsg(asyncOpRoot *op);
AsyncOpStatus epollAsyncWriteMsg(asyncOpRoot *op);
static int epollMonotonicNow(uintptr_t *nowNs);
static int epollTimerPublish(aioTimer *timer, asyncBase *target, void *udata);

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
  epollActivate,
  epollAsyncConnect,
  epollAsyncAccept,
  epollAsyncRead,
  epollAsyncWrite,
  epollAsyncReadMsg,
  epollAsyncWriteMsg,
  epollWakeupLoop,
  epollUpdateEventTimer,
  epollReleaseUserEvent
};

static int epollControl(int epollFd, int action, uint32_t events, int fd, void *ptr)
{
  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.events = events;
  ev.data.ptr = ptr;
  int result;
  do {
    result = epoll_ctl(epollFd, action, fd, &ev);
  } while (result == -1 && errno == EINTR);
  return result;
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
    if (base->eventFd == -1 || base->epollFd == -1) {
      // Descriptor exhaustion. A base without its epoll set or its doorbell
      // eventfd cannot run a message loop: fail creation instead of returning
      // an object that spins on EBADF
      if (base->eventFd != -1)
        close(base->eventFd);
      if (base->epollFd != -1)
        close(base->epollFd);
      free(base);
      return 0;
    }

    base->eventObject = epollNewAioObject(&base->B, ioObjectDevice, &base->eventFd);
    if (!base->eventObject) {
      close(base->eventFd);
      close(base->epollFd);
      free(base);
      return 0;
    }

    // The event fd bypasses the combiner re-arm logic (the message loop
    // re-arms it directly), so register it here explicitly
    if (epollControl(base->epollFd,
                     EPOLL_CTL_ADD,
                     EPOLLIN | EPOLLONESHOT,
                     base->eventFd,
                     (void*)(uintptr_t)objectHandleEncodeKnown(
                       &base->eventObject->root)) == -1) {
      close(base->eventFd);
      base->eventObject->hDevice = -1;
      objectFree(&objectPool, base->eventObject, sizeof(EPollObject));
      close(base->epollFd);
      free(base);
      return 0;
    }
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

void epollWakeupLoop(asyncBase *base)
{
  // Pure kick: no queue node (an empty node is the quit marker). The eventfd
  // is EPOLLIN|EPOLLONESHOT-registered, one sleeper wakes, reads it out and
  // re-arms; with nobody sleeping the next epoll_wait returns once immediately
  eventfd_write(((epollBase*)base)->eventFd, 1);
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
  // cancelled wholesale anyway, and its descriptor may already be closed;
  // the error path ioctl and a rearm epoll_ctl must not run on a reused fd.
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

  // CANCEL/CANCELIO: a timeout/opCancel/cancelIo set the status and asked for
  // a scan; the CANCELIO position additionally drives the CancelIoFlag sweep
  if (sig & (COMBINER_TAG_CANCEL | COMBINER_TAG_CANCELIO))
    reapObject(object, sig, &needStart);

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
        epollControl(base->epollFd,
                     EPOLL_CTL_ADD,
                     newEvents | EPOLLONESHOT | EPOLLRDHUP,
                     fd,
                     (void*)(uintptr_t)objectHandleEncodeKnown(object));
        fdObject->Registered = 1;
      } else if (currentEvents != newEvents) {
        epollControl(base->epollFd,
                     EPOLL_CTL_MOD,
                     newEvents | EPOLLONESHOT | EPOLLRDHUP,
                     fd,
                     (void*)(uintptr_t)objectHandleEncodeKnown(object));
      }
    } else if (currentEvents) {
      // No operation left on an armed fd: remove it from the set instead of
      // keeping it with an empty mask - EPOLLERR/EPOLLHUP ignore the mask and
      // would wake every epoll_wait for as long as the error condition holds.
      // An fd disarmed by a delivered EPOLLONESHOT shot stays fully silent,
      // so that state is left in the set as is
      epollControl(base->epollFd, EPOLL_CTL_DEL, 0, fd, 0);
      fdObject->Registered = 0;
    }
  }
}

void epollNextFinishedOperation(asyncBase *base)
{
  int nfds, n;
  struct epoll_event events[MAX_EVENTS];
  epollBase *localBase = (epollBase *)base;
  if (!loopThreadEnter(base))
    return;

  while (1) {
    do {
      if (!executeGlobalQueue(base)) {
        unsigned threadsRunning = loopThreadExit(base);
        if (threadsRunning)
          epollEnqueue(base, 0);
        return;
      }

      // UINT32_MAX = wait with no timeout: an idle base blocks until queue
      // traffic, a timer-arm kick or kernel readiness supplies a doorbell.
      uint32_t sleepMs = timerLoopPrepareSleep(base, messageLoopThreadId, getMonotonicTicks(), 500);
      nfds = epoll_wait(localBase->epollFd, events, MAX_EVENTS,
                        sleepMs == UINT32_MAX ? -1 : (int)sleepMs);
      timerLoopCancelSleep(base, messageLoopThreadId);
      // Unconditional sweep (the modulo election is gone): an idle pass costs
      // one relaxed load, and the wakeup handshake relies on whichever thread
      // the kick lands on doing the sweep itself
      processTimeoutQueue(base, getMonotonicTicks());
    } while (nfds <= 0 && errno == EINTR);

    for (n = 0; n < nfds; n++) {
      uint64_t envelope = (uint64_t)(uintptr_t)events[n].data.ptr;
      if (envelope & udataTimer) {
        uintptr_t timerId;
        aioObjectRoot *decoded;
        __tagged_pointer_decode(events[n].data.ptr, (void**)&decoded, &timerId);
        aioTimer *timer = (aioTimer*)decoded;
        if (timerId & udataUserEvent) {
          // Do not read timerfd here. A harvested envelope can overlap a
          // restart, and reading the shared fd in the loop would be able to
          // consume the next arm's expiration. Publish only a generation-
          // tagged probe; the event timer applier serializes read/settime/
          // disarm and re-arms the consumed EPOLLONESHOT shot.
          uintptr_t armedGeneration;
          uintptr_t armedIncarnation;
          if (reactorTimerDecodeCountProbe(timer,
                                           &armedGeneration,
                                           &armedIncarnation))
            eventTimerSignalProbe((aioUserEvent*)timer->op,
                                  armedGeneration,
                                  armedIncarnation);
          continue;
        }
        uintptr_t armedGeneration = 0;
        uintptr_t now;
        if (!epollMonotonicNow(&now)) {
          // The one-shot was consumed and without the matching monotonic
          // reading the backend cannot prove a timeout. Fail the CURRENT arm
          // terminally instead of silently stranding it behind a failed MOD.
          // UINTPTR_MAX selects any live deadline while the side-field triple
          // still rejects a concurrent rearm; opCancel then validates the
          // captured object generation before entering its combiner.
          aioObjectRoot *armedObject = 0;
          uintptr_t armedObjectGeneration = 0;
          if (reactorTimerDecodeDeadlineHandle(timer,
                                               timerId,
                                               UINTPTR_MAX,
                                               &armedGeneration,
                                               &armedObject,
                                               &armedObjectGeneration) ==
                rteExpireOperation &&
              !opCancel(timer->op,
                        armedGeneration,
                        aosUnknownError,
                        armedObject,
                        armedObjectGeneration))
            recordStaleHandleDrop(base);
          continue;
        }
        aioObjectRoot *armedObject = 0;
        uintptr_t armedObjectGeneration = 0;
        switch (reactorTimerDecodeDeadlineHandle(timer,
                                                 timerId,
                                                 now,
                                                 &armedGeneration,
                                                 &armedObject,
                                                 &armedObjectGeneration)) {
          case rteIgnore:
            // Stale doorbell: timer disarmed, or the current absolute deadline
            // is still ahead. Nothing reads the shared timerfd.
            break;

          case rteUserEvent: {
            aioUserEvent *event = (aioUserEvent*)timer->op;
            // Deadline timers are operation-only; keep the exhaustive enum
            // branch defensive, but incarnation zero makes it a no-op.
            eventTimerSignalTick(event, armedGeneration, 0);
            break;
          }

          case rteExpireOperation:
            if (!opCancel(timer->op,
                          armedGeneration,
                          aosTimeout,
                          armedObject,
                          armedObjectGeneration))
              recordStaleHandleDrop(base);
            break;
        }
      } else {
        aioObjectRoot *object;
        uintptr_t carriedGen;
        if (!objectHandleDecode(envelope, &object, &carriedGen))
          continue;
        if (object == &localBase->eventObject->root) {
          eventfd_t eventValue;
          eventfd_read(localBase->eventFd, &eventValue);
          epollControl(localBase->epollFd,
                       EPOLL_CTL_MOD,
                       EPOLLIN | EPOLLONESHOT,
                       localBase->eventFd,
                       (void*)(uintptr_t)envelope);
          continue;
        }
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
          if (!combinerPushValidated(object,
                                     carriedGen,
                                     bits))
            recordStaleHandleDrop(base);
        }
      }
    }
  }
}


aioObject *epollNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  EPollObject *object = (EPollObject*)objectAlloc(&objectPool,
                                                  sizeof(EPollObject),
                                                  TAGGED_POINTER_ALIGNMENT);
  if (!object)
    return 0;
  uint64_t encoded;
  if (!objectHandleTryEncode(&object->Object.root, &encoded)) {
    // Every pooled fd cell was encodable when first published, keeps the same
    // address, and generation-range exhaustion is tombstoned instead of pooled.
    // Failure therefore proves this is a fresh, unpublished allocation.
    alignedFree(object);
    return 0;
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

// Return the generation-headed cell immediately. Harvested events validate
// only Head and cannot enter the combiner after the generation bump.
static void epollReleaseObject(aioObject *object)
{
  if (__uintptr_atomic_load(&object->root.Head.gen, amoRelaxed) >
      objectHandleGenMask()) {
    free(object->buffer.ptr);
    object->buffer.ptr = 0;
    object->buffer.totalSize = 0;
    ASAN_POISON_MEMORY_REGION((uint8_t*)object + sizeof(AsyncObjectHead),
                              sizeof(EPollObject) - sizeof(AsyncObjectHead));
    return; // permanent tombstone: exhausted generation is never published
  }
  objectFree(&objectPool, object, sizeof(EPollObject));
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

  // Already-harvested epoll entries carry the old generation and may touch
  // only Head. The rest of the cell can return to its type-stable pool now.
  epollReleaseObject(object);
}

void epollInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  op->timerId = 0;
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  if (!timer)
    return;
  timer->base = base;
  reactorTimerInitializeSharedState(timer);
  timer->fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (timer->fd == -1) {
    alignedFree(timer);
    return;
  }
  timer->op = op;
  timer->registeredBase = 0;
  timer->kind = rtkUnknown;
  timer->broken = 0;
  // Registration is lazy. asyncOpAlloc reaches this constructor before
  // initAsyncOpRoot has initialized opCode, so reading it here is invalid;
  // the first arm binds the immutable timer kind and publishes an active ADD.
  op->timerId = timer;
}

static int epollMonotonicDeadline(uint64_t timeout,
                                  struct itimerspec *its,
                                  uintptr_t *deadline)
{
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1 || now.tv_sec < 0)
    return 0;
  if ((uint64_t)now.tv_sec > UINTPTR_MAX / 1000000000ULL ||
      timeout > UINTPTR_MAX / 1000ULL)
    return 0;
  uintptr_t nowNs = (uintptr_t)now.tv_sec * 1000000000ULL +
                    (uintptr_t)now.tv_nsec;
  uintptr_t delta = (uintptr_t)(timeout * 1000ULL);
  if (nowNs > UINTPTR_MAX - delta)
    return 0;
  *deadline = nowNs + delta;
  memset(its, 0, sizeof(*its));
  its->it_value.tv_sec = (time_t)(*deadline / 1000000000ULL);
  its->it_value.tv_nsec = (long)(*deadline % 1000000000ULL);
  return 1;
}

static int epollMonotonicNow(uintptr_t *nowNs)
{
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1 || now.tv_sec < 0 ||
      (uint64_t)now.tv_sec > UINTPTR_MAX / 1000000000ULL)
    return 0;
  *nowNs = (uintptr_t)now.tv_sec * 1000000000ULL + (uintptr_t)now.tv_nsec;
  return 1;
}

static int epollTimerSettime(aioTimer *timer, int flags, const struct itimerspec *its)
{
  int result;
  do {
    result = timerfd_settime((int)timer->fd, flags, its, 0);
  } while (result == -1 && errno == EINTR);
  return result == 0;
}

static int epollTimerEnsureFd(aioTimer *timer)
{
  if (timer->fd != -1)
    return 1;
  timer->fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  return timer->fd != -1;
}

static int epollTimerPublish(aioTimer *timer, asyncBase *target, void *udata)
{
  asyncBase *old = timer->registeredBase;
  epollBase *targetBase = (epollBase*)target;
  int action = old == target && old ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
  int result = epollControl(targetBase->epollFd,
                            action,
                            EPOLLIN | EPOLLONESHOT,
                            (int)timer->fd,
                            udata);
  if (result == -1 && action == EPOLL_CTL_MOD && errno == ENOENT)
    result = epollControl(targetBase->epollFd,
                          EPOLL_CTL_ADD,
                          EPOLLIN | EPOLLONESHOT,
                          (int)timer->fd,
                          udata);
  else if (result == -1 && action == EPOLL_CTL_ADD && errno == EEXIST)
    result = epollControl(targetBase->epollFd,
                          EPOLL_CTL_MOD,
                          EPOLLIN | EPOLLONESHOT,
                          (int)timer->fd,
                          udata);
  if (result == -1)
    return 0;

  timer->registeredBase = target;
  timer->base = target;
  if (old && old != target)
    (void)epollControl(((epollBase*)old)->epollFd,
                       EPOLL_CTL_DEL,
                       0,
                       (int)timer->fd,
                       0);
  return 1;
}

static void epollTimerRollbackArm(aioTimer *timer)
{
  struct itimerspec disarm;
  memset(&disarm, 0, sizeof(disarm));
  reactorTimerDisarm(timer);
  if (timer->fd != -1)
    (void)epollTimerSettime(timer, 0, &disarm);
}

void epollStartTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (!timer) {
    // The paired cell is created once per pooled slot; a constructor-time
    // allocation failure must not disable this slot's timeouts forever.
    epollInitializeTimer(op->object->base, op);
    timer = (aioTimer*)op->timerId;
  }
  if (!timer || !reactorTimerBindKind(timer, rtkOperation)) {
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
    return;
  }

  struct itimerspec its;
  uintptr_t deadline;
  if (!epollMonotonicDeadline(op->timeout, &its, &deadline) ||
      !epollTimerSettime(timer, TFD_TIMER_ABSTIME, &its)) {
    epollTimerRollbackArm(timer);
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
    return;
  }

  void *udata = reactorTimerArmDeadlineGeneration(timer,
                                                  op,
                                                  opGetGeneration(op),
                                                  deadline);
  if (!epollTimerPublish(timer, op->object->base, udata)) {
    epollTimerRollbackArm(timer);
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
  }
}

static int epollStartTimerGeneration(asyncOpRoot *op,
                                     uintptr_t generation,
                                     uint64_t period)
{
  struct itimerspec its;
  memset(&its, 0, sizeof(its));
  its.it_value.tv_sec = period / 1000000;
  its.it_value.tv_nsec = (period % 1000000) * 1000;
  its.it_interval = its.it_value;

  aioTimer *timer = (aioTimer*)op->timerId;
  if (!timer || !reactorTimerBindKind(timer, rtkUserEvent) ||
      !epollTimerEnsureFd(timer) ||
      !epollTimerSettime(timer, 0, &its)) {
    if (timer)
      epollTimerRollbackArm(timer);
    return 0;
  }
  void *udata = reactorTimerArmCountGeneration(timer, op, generation);
  if (!epollTimerPublish(timer, ((aioUserEvent*)op)->base, udata)) {
    epollTimerRollbackArm(timer);
    return 0;
  }
  return 1;
}

static int epollStopTimerInternal(asyncOpRoot *op)
{
  struct itimerspec its;
  memset(&its, 0, sizeof(its));
  aioTimer *timer = (aioTimer*)op->timerId;
  if (!timer)
    return 1;
  reactorTimerDisarm(timer);
  // settime starts a new timerfd epoch and clears the expiration count. The
  // delivered EPOLLONESHOT shot may stay registered but cannot wake again
  // until the next arm's checked MOD/ADD; no drain or empty-mask MOD needed.
  return epollTimerSettime(timer, 0, &its);
}

void epollStopTimer(asyncOpRoot *op)
{
  (void)epollStopTimerInternal(op);
}

int epollActivate(aioUserEvent *op)
{
  epollEnqueue(op->base, &op->root);
  return 1;
}

int epollUpdateEventTimer(aioUserEvent *event,
                          EventTimerUpdate update,
                          uintptr_t generation,
                          uint64_t period)
{
  aioTimer *timer = (aioTimer*)event->root.timerId;
  switch (update) {
    case etuStart:
      return epollStartTimerGeneration(&event->root, generation, period);
    case etuStop:
      return epollStopTimerInternal(&event->root);
    case etuRearm:
      return timer && epollTimerPublish(timer,
                                        event->base,
                                        reactorTimerUdata(timer));
    case etuConsume: {
      uint64_t expirations;
      ssize_t bytes;
      do {
        bytes = read((int)timer->fd, &expirations, sizeof(expirations));
      } while (bytes < 0 && errno == EINTR);
      return bytes == (ssize_t)sizeof(expirations) && expirations != 0 ? 2 : 1;
    }
  }
  return 1;
}

void epollReleaseUserEvent(aioUserEvent *event)
{
  aioTimer *timer = (aioTimer*)event->root.timerId;
  if (!timer) {
    if (__uintptr_atomic_load(&event->incarnation, amoRelaxed) != 0)
      concurrentQueuePush(event->root.objectPool, event);
    return;
  }
  assert(__uintptr_atomic_load(&timer->tag, amoAcquire) == 0 &&
         "Recycling an armed user-event timer");
  if (timer->registeredBase && timer->fd != -1)
    (void)epollControl(((epollBase*)timer->registeredBase)->epollFd,
                       EPOLL_CTL_DEL,
                       0,
                       (int)timer->fd,
                       0);
  if (timer->fd != -1)
    close((int)timer->fd);
  timer->fd = -1;
  timer->registeredBase = 0;
  timer->base = event->base;
  // The timer cell is the physical lifetime anchor for stale epoll batches;
  // keep event<->timer immutable and return them to the pool as one unit.
  if (__uintptr_atomic_load(&event->incarnation, amoRelaxed) != 0)
    concurrentQueuePush(event->root.objectPool, event);
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
