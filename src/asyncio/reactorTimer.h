// Shared timer protocol of the readiness backends (epoll/kqueue): the kernel
// publication of an arming and the decode of a delivered timer event. Both
// sides of the kernel round-trip live in one place so the two backends cannot
// drift apart, and the protocol stays exhaustively unit-testable - the same
// reasoning as the combinerSelect* decision kernels in asyncioImpl.h. The
// kernel syscalls themselves (kevent/timerfd+epoll_ctl) stay in the backends.
// Windows/IOCP is not a reactor and keeps its own waitable-timer scheme.
#ifndef __ASYNCIO_REACTORTIMER_H_
#define __ASYNCIO_REACTORTIMER_H_

#include "asyncioImpl.h"

#ifdef __cplusplus
extern "C" {
#endif

// Discriminator bits carried in the udata pointer tag. The event decode path
// runs concurrently with the owning thread initializing the pointed-to object
// (the kernel round-trip is not an ordering edge), so the branch decision must
// not read the object's memory: fd objects carry no bits, timers are marked
// explicitly, user-event timers additionally so
enum {
  udataTimer = 1,
  udataUserEvent = 2
};

typedef struct aioTimer {
  aioObjectRoot root;
  // Full generation of the armed operation; 0 = disarmed. Written with release
  // at arm time, read with acquire at event delivery: the kernel round-trip
  // carries no memory ordering, this field is the synchronization edge. A stale
  // event delivered after stop/rearm reads either 0 or a generation that loses
  // the status CAS in opCancel().
  uintptr_t tag;
  // A timerfd on epoll, a virtual timer id (kevent ident) on kqueue
  intptr_t fd;
  asyncOpRoot *op;
} aioTimer;

// What a delivered kernel timer event means for the backend loop.
typedef enum ReactorTimerEventAction {
  rteIgnore = 0,      // stale doorbell: nothing may be touched
  rteExpireOperation, // realtime timeout: opCancel(timer->op, generation, aosTimeout)
  rteUserEvent        // a user-event timer tick: activate and deliver
} ReactorTimerEventAction;

// Publish an arming: the timer's tag synchronizes the operation fields to the
// delivery side; the returned pointer is the udata to register with the kernel
// for this arming - the only payload the delivered event carries back.
static inline void *reactorTimerArm(aioTimer *timer, asyncOpRoot *op)
{
  int periodic = op->opCode == actUserEvent;
  __uintptr_atomic_store(&timer->tag, opGetGeneration(op), amoRelease);
  return __tagged_pointer_make(timer, udataTimer | (periodic ? udataUserEvent : 0));
}

static inline void reactorTimerDisarm(aioTimer *timer)
{
  __uintptr_atomic_store(&timer->tag, 0, amoRelaxed);
}

// Decode a delivered timer event. udataBits are the discriminator bits that
// travelled with the event through the kernel; on rteExpireOperation
// *armedGeneration receives the generation whose timeout this event carries,
// to be rejected by the status CAS if the operation moved on. The acquire
// load pairs with the release store in reactorTimerArm and is the only
// ordering edge with the arming thread; timer->op and the fields behind it
// may only be read after it. The user-event bit rides in the udata tag:
// op->opCode belongs to the operation and cannot be read before its
// generation is validated by the status CAS.
static inline ReactorTimerEventAction reactorTimerDecodeEvent(aioTimer *timer,
                                                              uintptr_t udataBits,
                                                              uintptr_t *armedGeneration)
{
  uintptr_t armedTag = __uintptr_atomic_load(&timer->tag, amoAcquire);
  if (armedTag == 0)
    return rteIgnore;
  if (udataBits & udataUserEvent)
    return rteUserEvent;
  *armedGeneration = armedTag;
  return rteExpireOperation;
}

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_REACTORTIMER_H_
