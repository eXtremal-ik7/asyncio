#include "asyncioImpl.h"
#include "reactor.h"
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
void epollDeleteObject(aioObject *object);
void epollInitializeTimer(asyncBase *base, asyncOpRoot *op);
void epollStartTimer(asyncOpRoot *op);
void epollStopTimer(asyncOpRoot *op);
int epollInitializeUserEvent(aioUserEvent *event);
int epollActivate(aioUserEvent *op);
int epollUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period);
uint64_t epollConsumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period);
void epollReleaseUserEvent(aioUserEvent *event);
AsyncOpStatus epollAsyncWrite(asyncOpRoot *opptr);
static int epollMonotonicNow(uint64_t *nowNs);
static int epollTimerPublish(aioTimer *timer, asyncBase *target, uint64_t envelope, uint32_t events);

static struct asyncImpl epollImpl = {
  combinerTaskHandler,
  epollEnqueue,
  epollPostEmptyOperation,
  epollNextFinishedOperation,
  epollNewAioObject,
  newAsyncOp,
  cancelAsyncOp,
  epollInitializeTimer,
  epollStartTimer,
  epollStopTimer,
  epollInitializeUserEvent,
  epollActivate,
  connectSyscall,
  acceptSyscall,
  readSyscall,
  epollAsyncWrite,
  readMsgSyscall,
  writeMsgSyscall,
  epollWakeupLoop,
  epollUpdateEventTimer,
  epollConsumeEventTimerTick,
  epollReleaseUserEvent
};

static int epollControl(int epollFd, int action, uint32_t events, int fd, uint64_t envelope)
{
  struct epoll_event ev;
  memset(&ev, 0, sizeof(ev));
  ev.events = events;
  ev.data.u64 = envelope;
  int result;
  do {
    result = epoll_ctl(epollFd, action, fd, &ev);
  } while (result == -1 && errno == EINTR);
  return result;
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
                     kernelHandleEncode(&base->eventObject->root.header)) == -1) {
      close(base->eventFd);
      base->eventObject->hDevice = -1;
      objectFree(&objectPool, base->eventObject, sizeof(EPollObject));
      close(base->epollFd);
      free(base);
      return 0;
    }
    ((EPollObject*)base->eventObject)->Registered = 1;
  }

  return (asyncBase*)base;
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
  EPollObject *fdObject =
      (object->header.objectType == ioObjectDevice || object->header.objectType == ioObjectSocket) ? (EPollObject*)object : 0;
  CombinerPassEvents pass = reactorCombinerCore(object, fdObject ? &fdObject->Object : 0, op, sig);

  if (fdObject && !__uint_atomic_load(&object->DeletePending, amoRelaxed)) {
    int fd = getFd(&fdObject->Object);
    epollBase *base = (epollBase*)object->header.base;
    uint32_t currentEvents = epollEvents(pass.oldIoEvents);
    uint32_t newEvents;

    // Calculate the current mask because no fd->mask map is kept. Any delivered
    // event consumes the EPOLLONESHOT shot, whatever direction it carries.
    if (pass.ioEvents)
      currentEvents = 0;

    newEvents = epollEvents(combinerActiveIoEvents(object));

    if (newEvents) {
      if (!fdObject->Registered) {
        epollControl(base->epollFd,
                     EPOLL_CTL_ADD,
                     newEvents | EPOLLONESHOT | EPOLLRDHUP,
                     fd,
                     kernelHandleEncode(&fdObject->Object.root.header));
        fdObject->Registered = 1;
      } else if (currentEvents != newEvents) {
        epollControl(base->epollFd,
                     EPOLL_CTL_MOD,
                     newEvents | EPOLLONESHOT | EPOLLRDHUP,
                     fd,
                     kernelHandleEncode(&fdObject->Object.root.header));
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
  epollBase *localBase = (epollBase*)base;
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
      nfds = epoll_wait(localBase->epollFd, events, MAX_EVENTS, sleepMs == UINT32_MAX ? -1 : (int)sleepMs);
      timerLoopCancelSleep(base, messageLoopThreadId);
      // Unconditional sweep (the modulo election is gone): an idle pass costs
      // one relaxed load, and the wakeup handshake relies on whichever thread
      // the kick lands on doing the sweep itself
      processTimeoutQueue(base, getMonotonicTicks());
    } while (nfds <= 0 && errno == EINTR);

    for (n = 0; n < nfds; n++) {
      uint64_t generation;
      uint64_t envelope = events[n].data.u64;
      objectHeader *header = kernelHandleDecode(envelope, &generation);

      switch (objectHeaderGetType(header)) {
        case ohtUserEvent: {
          aioUserEvent *event = (aioUserEvent*)header;
          if (!eventTimerTryClaimReference(event, generation))
            break;
          eventfd_t eventValue;
          int result;
          do {
            result = eventfd_read((int)event->activationId, &eventValue);
          } while (result == -1 && errno == EINTR);
          if (result == 0 || errno == EAGAIN)
            eventManualReady(event);
          eventDecrementReference(event, 1);
          break;
        }

        case ohtTimer: {
          aioTimer *timer = (aioTimer*)header;
          if (timer->header.timer.kind == tkUserEvent) {
            // Publish one provisional tick. The owner replaces it with the
            // accumulated timerfd count after it has serialized against
            // start/stop.
            uint64_t timerGeneration = __uint64_atomic_load(&timer->header.tag.high, amoAcquire);
            int isStale = timerGeneration != generation;
            uint64_t eventGeneration = __uint64_atomic_load(&timer->event.generation, amoAcquire);
            isStale |= !__uint64_atomic_load(&timer->header.tag.low, amoAcquire);
            isStale |= objectHeaderGeneration(&timer->header) != timerGeneration;
            if (!isStale)
              eventTimerSignal(timer->event.userEvent, timerGeneration, eventGeneration, 1);
            break;
          }

          uint64_t now;
          AsyncOpStatus status = aosTimeout;
          if (!epollMonotonicNow(&now)) {
            now = UINT64_MAX;
            status = aosUnknownError;
          }

          int isStale = !__uint64_atomic_load(&timer->header.tag.low, amoAcquire);
          uint64_t timerGeneration = __uint64_atomic_load(&timer->header.tag.high, amoAcquire);
          isStale |= timerGeneration != generation;
          uint64_t deadline = __uint64_atomic_load(&timer->operation.deadline, amoRelaxed);
          isStale |= now < deadline;
          if (!isStale) {
            aioObjectRoot *object = (aioObjectRoot*)__pointer_atomic_load((void *volatile*)&timer->operation.object, amoRelaxed);
            uint64_t objectGeneration = __uint64_atomic_load(&timer->operation.objectGeneration, amoRelaxed);
            (void)opCancel(timer->operation.op, timerGeneration, status, object, objectGeneration);
          }
          break;
        }

        case ohtObject: {
          aioObject *object = (aioObject*)header;
          if (object == localBase->eventObject) {
            eventfd_t eventValue;
            eventfd_read(localBase->eventFd, &eventValue);
            epollControl(localBase->epollFd, EPOLL_CTL_MOD, EPOLLIN | EPOLLONESHOT, localBase->eventFd, envelope);
            break;
          }

          // Encode kernel readiness straight into combiner tag bits (their
          // values deliberately match IO_EVENT_*): EPOLLERR/EPOLLHUP wake
          // both directions, EPOLLRDHUP additionally raises the error sweep.
          uint32_t bits = 0;
          if (events[n].events & (EPOLLIN | EPOLLERR | EPOLLHUP))
            bits |= COMBINER_TAG_PROGRESS_READ;
          if (events[n].events & (EPOLLOUT | EPOLLERR | EPOLLHUP))
            bits |= COMBINER_TAG_PROGRESS_WRITE;
          if (events[n].events & EPOLLRDHUP)
            bits |= COMBINER_TAG_ERROR | COMBINER_TAG_PROGRESS_READ | COMBINER_TAG_PROGRESS_WRITE;
          if (bits)
            (void)combinerPushValidated(&object->root, generation, bits);
          break;
        }
      }
    }
  }
}

aioObject *epollNewAioObject(asyncBase *base, IoObjectTy type, void *data)
{
  EPollObject *object = (EPollObject*)objectAlloc(&objectPool, sizeof(EPollObject), TAGGED_POINTER_ALIGNMENT);
  if (!object)
    return 0;

  initObjectRoot(&object->Object.root, base, type, (aioObjectDestructor*)epollDeleteObject);
  switch (type) {
    case ioObjectDevice: object->Object.hDevice = *(iodevTy*)data; break;
    case ioObjectSocket: object->Object.hSocket = *(socketTy*)data; break;
    default: break;
  }

  object->Registered = 0;
  object->Object.buffer.offset = 0;
  object->Object.buffer.dataSize = 0;
  return &object->Object;
}

void epollDeleteObject(aioObject *object)
{
  epollBase *localBase = (epollBase*)object->root.header.base;
  int registered = ((EPollObject*)object)->Registered;
  switch (object->root.header.objectType) {
    case ioObjectDevice:
      if (registered)
        epollControl(localBase->epollFd, EPOLL_CTL_DEL, 0, object->hDevice, 0);
      close(object->hDevice);
      object->hDevice = -1;
      break;
    case ioObjectSocket:
      if (registered)
        epollControl(localBase->epollFd, EPOLL_CTL_DEL, 0, object->hSocket, 0);
      close(object->hSocket);
      object->hSocket = -1;
      break;
    default: break;
  }

  objectFree(&objectPool, object, sizeof(EPollObject));
}

void epollInitializeTimer(asyncBase *base, asyncOpRoot *op)
{
  op->timerId = 0;
  aioTimer *timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
  if (!timer)
    return;
  timerInitialize(timer);
  timer->header.base = base;
  timer->fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
  if (timer->fd == -1) {
    alignedFree(timer);
    return;
  }
  timer->operation.op = op;
  timer->header.timer.kind = tkOperation;
  // Registration stays lazy until the first active arm.
  op->timerId = timer;
}

static int epollMonotonicDeadline(uint64_t timeout, struct itimerspec *its, uint64_t *deadline)
{
  uint64_t nowNs;
  if (!epollMonotonicNow(&nowNs) || timeout > UINT64_MAX / 1000ULL)
    return 0;
  uint64_t delta = timeout * 1000ULL;
  if (nowNs > UINT64_MAX - delta)
    return 0;
  *deadline = nowNs + delta;
  memset(its, 0, sizeof(*its));
  its->it_value.tv_sec = (time_t)(*deadline / 1000000000ULL);
  its->it_value.tv_nsec = (long)(*deadline % 1000000000ULL);
  return 1;
}

static int epollMonotonicNow(uint64_t *nowNs)
{
  struct timespec now;
  if (clock_gettime(CLOCK_MONOTONIC, &now) == -1 || now.tv_sec < 0 || (uint64_t)now.tv_sec > UINT64_MAX / 1000000000ULL)
    return 0;
  *nowNs = (uint64_t)now.tv_sec * 1000000000ULL + (uint64_t)now.tv_nsec;
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

static int epollTimerPublish(aioTimer *timer, asyncBase *target, uint64_t envelope, uint32_t events)
{
  asyncBase *old = timer->header.timer.registered ? timer->header.base : 0;
  epollBase *targetBase = (epollBase*)target;
  int action = old == target && old ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
  int result = epollControl(targetBase->epollFd, action, events, (int)timer->fd, envelope);
  if (result == -1 && action == EPOLL_CTL_MOD && errno == ENOENT)
    result = epollControl(targetBase->epollFd, EPOLL_CTL_ADD, events, (int)timer->fd, envelope);
  else if (result == -1 && action == EPOLL_CTL_ADD && errno == EEXIST)
    result = epollControl(targetBase->epollFd, EPOLL_CTL_MOD, events, (int)timer->fd, envelope);
  if (result == -1)
    return 0;

  timer->header.base = target;
  timer->header.timer.registered = 1;
  if (old && old != target)
    (void)epollControl(((epollBase*)old)->epollFd, EPOLL_CTL_DEL, 0, (int)timer->fd, 0);
  return 1;
}

static void epollTimerRollbackArm(aioTimer *timer)
{
  struct itimerspec disarm;
  memset(&disarm, 0, sizeof(disarm));
  timerUnpublish(timer);
  if (timer->fd != -1)
    (void)epollTimerSettime(timer, 0, &disarm);
}

void epollStartTimer(asyncOpRoot *op)
{
  // The paired cell is created once per pooled slot; a constructor-time
  // allocation failure must not disable this slot's timeouts forever.
  aioTimer *timer = (aioTimer*)opEnsureTimerCell(op);
  if (!timer) {
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
    return;
  }
  struct itimerspec its;
  uint64_t deadline;
  if (!epollMonotonicDeadline(op->timeout, &its, &deadline) || !epollTimerSettime(timer, TFD_TIMER_ABSTIME, &its)) {
    epollTimerRollbackArm(timer);
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
    return;
  }

  aioObjectRoot *object = op->object;
  uint64_t generation = opGetGeneration(op);

  timerPublishBegin(timer);
    __uint64_atomic_store(&timer->operation.deadline, deadline, amoRelaxed);
    __pointer_atomic_store((void *volatile*)&timer->operation.object, object, amoRelaxed);
    __uint64_atomic_store(&timer->operation.objectGeneration, objectHeaderGeneration(&object->header), amoRelaxed);
  timerPublishEnd(timer, generation, 1);

  uint64_t envelope = kernelHandleEncode(&timer->header);
  if (!epollTimerPublish(timer, object->header.base, envelope, EPOLLIN | EPOLLONESHOT)) {
    epollTimerRollbackArm(timer);
    (void)opSetStatus(op, opGetGeneration(op), aosUnknownError);
  }
}

static int epollStartTimerGeneration(aioUserEvent *event, aioTimer *timer, uint32_t generation, uint64_t period)
{
  struct itimerspec its;
  memset(&its, 0, sizeof(its));
  its.it_value.tv_sec = period / 1000000;
  its.it_value.tv_nsec = (period % 1000000) * 1000;
  its.it_interval = its.it_value;

  if (!epollTimerEnsureFd(timer) || !epollTimerSettime(timer, 0, &its)) {
    epollTimerRollbackArm(timer);
    return 0;
  }

  timerPublishBegin(timer);
    __uint64_atomic_store(&timer->event.generation, eventHandleGeneration(event), amoRelease);
  timerPublishEnd(timer, generation, 1);

  uint64_t envelope = kernelHandleEncode(&timer->header);
  if (!epollTimerPublish(timer, event->header.base, envelope, EPOLLIN | EPOLLET)) {
    epollTimerRollbackArm(timer);
    return 0;
  }
  return 1;
}

static int epollDisarmTimer(aioTimer *timer)
{
  struct itimerspec its;
  memset(&its, 0, sizeof(its));
  timerUnpublish(timer);
  // settime starts a new timerfd epoch and clears the expiration count.
  return epollTimerSettime(timer, 0, &its);
}

void epollStopTimer(asyncOpRoot *op)
{
  aioTimer *timer = (aioTimer*)op->timerId;
  if (timer)
    (void)epollDisarmTimer(timer);
}

int epollInitializeUserEvent(aioUserEvent *event)
{
  uint64_t encoded = kernelHandleEncode(&event->header);
  int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd == -1)
    return 0;
  event->activationId = fd;
  if (epollControl(((epollBase*)event->header.base)->epollFd, EPOLL_CTL_ADD, EPOLLIN | EPOLLET, fd, encoded) == 0)
    return 1;
  close(fd);
  event->activationId = UINT64_MAX;
  return 0;
}

int epollActivate(aioUserEvent *event)
{
  eventfd_t value = 1;
  int result;
  do {
    result = eventfd_write((int)event->activationId, value);
  } while (result == -1 && errno == EINTR);
  // EAGAIN means the eventfd counter is saturated and therefore already
  // readable; the manual signalState, not the kernel counter, carries exact
  // multiplicity.
  return result == 0 || errno == EAGAIN;
}

int epollUpdateEventTimer(aioUserEvent *event, EventTimerUpdate update, uint32_t generation, uint64_t period)
{
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  switch (update) {
    case etuStart:
      if (!timer) {
        timer = alignedMalloc(sizeof(aioTimer), TAGGED_POINTER_ALIGNMENT);
        if (!timer)
          return 0;
        timerInitialize(timer);
        timer->header.base = event->header.base;
        timer->fd = -1;
        timer->event.userEvent = event;
        timer->header.timer.kind = tkUserEvent;
        eventTimerStore(event, timer, amoRelaxed);
      }
      return epollStartTimerGeneration(event, timer, generation, period);
    case etuStop: {
      assert(timer && "Stopping a user-event timer which was never armed");
      return epollDisarmTimer(timer);
    }
  }
  return 1;
}

uint64_t epollConsumeEventTimerTick(aioUserEvent *event, uint64_t published, uint32_t generation, uint64_t period)
{
  __UNUSED(generation);
  __UNUSED(period);
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  uint64_t expirations;
  ssize_t bytes;
  do {
    bytes = read((int)timer->fd, &expirations, sizeof(expirations));
  } while (bytes < 0 && errno == EINTR);
  // A competing harvested readiness may find the descriptor drained. Its
  // provisional control-word tick remains the conservative delivery count.
  return bytes == (ssize_t)sizeof(expirations) ? expirations : published;
}

void epollReleaseUserEvent(aioUserEvent *event)
{
  if (event->activationId != UINT64_MAX) {
    (void)epollControl(((epollBase*)event->header.base)->epollFd, EPOLL_CTL_DEL, 0, (int)event->activationId, 0);
    close((int)event->activationId);
    event->activationId = UINT64_MAX;
  }
  aioTimer *timer = eventTimerLoad(event, amoRelaxed);
  if (timer) {
    assert(__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) == 0 && "Recycling an armed user-event timer");
    if (timer->header.timer.registered && timer->fd != -1)
      (void)epollControl(((epollBase*)timer->header.base)->epollFd, EPOLL_CTL_DEL, 0, (int)timer->fd, 0);
    if (timer->fd != -1)
      close((int)timer->fd);
    timer->fd = -1;
    timer->header.timer.registered = 0;
    timer->header.base = event->header.base;
  }
  // The timer cell is the physical lifetime anchor for stale epoll batches;
  // keep event<->timer immutable and return them to the pool as one unit.
  objectFree(&event->header.base->eventPool, event, sizeof(aioUserEvent));
}

AsyncOpStatus epollAsyncWrite(asyncOpRoot *opptr)
{
  asyncOp *op = (asyncOp*)opptr;
  aioObject *object = (aioObject*)op->root.object;
  int fd = getFd(object);

  // Linux has no per-descriptor SIGPIPE suppression, so sockets take an
  // explicit MSG_NOSIGNAL send; pipes go through the masking write helper.
  ssize_t bytesWritten;
  if (object->root.header.objectType == ioObjectSocket)
    bytesWritten = send(fd, (uint8_t*)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred, MSG_NOSIGNAL);
  else
    bytesWritten = guardedWrite(object, fd, (uint8_t*)op->buffer + op->bytesTransferred, op->transactionSize - op->bytesTransferred);
  return transferStatus(op, bytesWritten);
}
