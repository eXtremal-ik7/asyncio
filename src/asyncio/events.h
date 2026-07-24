#ifndef __ASYNCIO_EVENTS_H_
#define __ASYNCIO_EVENTS_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "lifetime.h"

#if defined(OS_32)
#define TAG_EVENT_DELETE STATIC_CAST(uintptr_t, 0x80000000U)
#define TAG_EVENT_WAITER_COMMITTED STATIC_CAST(uintptr_t, 0x40000000U)
#define TAG_EVENT_REF_MASK STATIC_CAST(uintptr_t, 0x3FFFFFFFU)
#elif defined(OS_64)
#define TAG_EVENT_DELETE STATIC_CAST(uintptr_t, 0x8000000000000000ULL)
#define TAG_EVENT_WAITER_COMMITTED STATIC_CAST(uintptr_t, 0x4000000000000000ULL)
#define TAG_EVENT_REF_MASK STATIC_CAST(uintptr_t, 0x3FFFFFFFFFFFFFFFULL)
#else
#error Configution incomplete
#endif

typedef enum EventTimerUpdate {
  etuStart,
  etuStop
} EventTimerUpdate;

typedef struct aioTimer aioTimer;

typedef struct aioTimerUserEventState {
  uint32_t remaining;
  uint32_t armed;
  uint64_t period;
} aioTimerUserEventState;

struct aioUserEvent {
  // Kernel-visible prefix. tag.low carries references and terminal/rendezvous
  // bits; tag.high is the monotonic incarnation.
  objectHeader header;

  void *callback;
  void *arg;
  volatile uintptr_t signalState;
  uint64_t activationId;

  // The user-event timer control word and coroutine waiter are independent
  // DWCAS protocols. See events.c for their state transitions.
  volatile uint128 timerControl;
  volatile uint128 waiter;
  aioTimer *volatile timer;

  userEventDestructorCb *destructorCb;
  void *destructorCbArg;
};

typedef char aioUserEventMustFitTwoCacheLines[sizeof(struct aioUserEvent) <= 2 * CACHE_LINE_SIZE ? 1 : -1];
typedef char aioUserEventTimerStateStartsOnSecondCacheLine[offsetof(struct aioUserEvent, timerControl) == CACHE_LINE_SIZE ? 1 : -1];
typedef char aioUserEventWaiterIsDwcasAligned[offsetof(struct aioUserEvent, waiter) % 16 == 0 ? 1 : -1];

static inline uint64_t eventHandleGeneration(aioUserEvent *event)
{
  return objectHeaderGeneration(&event->header);
}

static inline aioTimer *eventTimerLoad(aioUserEvent *event, AtomicMemoryOrder order)
{
  return (aioTimer*)__pointer_atomic_load((void* volatile*)&event->timer, order);
}

static inline void eventTimerStore(aioUserEvent *event, aioTimer *timer, AtomicMemoryOrder order)
{
  __pointer_atomic_store((void* volatile*)&event->timer, timer, order);
}

static inline aioTimerUserEventState *eventTimerState(aioTimer *timer)
{
  return (aioTimerUserEventState*)((uint8_t*)timer + sizeof(objectHeader));
}

#define EVENT_TIMER_OWNER (1ULL << 63)
#define EVENT_TIMER_COUNTER_SHIFT 32

static inline uint64_t eventTimerControlPeriod(uint128 control)
{
  return control.low;
}

static inline uint32_t eventTimerControlCounter(uint128 control)
{
  return (uint32_t)((control.high >> EVENT_TIMER_COUNTER_SHIFT) & 0x7FFFFFFF);
}

static inline uint64_t eventTimerControlPendingTicks(uint128 control)
{
  return control.low;
}

static inline uint32_t eventTimerControlGeneration(uint128 control)
{
  return (uint32_t)control.high;
}

static inline uint32_t eventTimerGeneration(aioUserEvent *event)
{
  return (uint32_t)__uint64_atomic_load(&event->timerControl.high, amoRelaxed);
}

enum EventWaiterTag {
  ewtCredits = 0,
  ewtUser = 1,
  ewtSleep = 2,
  ewtDeleted = 3,
  ewtMask = 3,
  ewtCreditUnit = 4
};

static inline int eventReferenceIsDeleting(aioUserEvent *event)
{
  return (__uint64_atomic_load(&event->header.tag.low, amoRelaxed) & TAG_EVENT_DELETE) != 0;
}

static inline int eventTryClaimReference(aioUserEvent *event, uint64_t eventGeneration, int allowDeletedWaiter)
{
  uint128 expected = {
    __uint64_atomic_load(&event->header.tag.low, amoRelaxed),
    eventGeneration
  };
  for (;;) {
    if (expected.high != eventGeneration || !(expected.low & TAG_EVENT_REF_MASK))
      return 0;
    if ((expected.low & TAG_EVENT_DELETE) &&
        (!allowDeletedWaiter || !(expected.low & TAG_EVENT_WAITER_COMMITTED)))
      return 0;
    uint128 desired = {expected.low + 1, expected.high};
    if (__uint128_atomic_compare_and_swap(&event->header.tag, &expected, desired))
      return 1;
  }
}

static inline int eventTimerTryClaimReference(aioUserEvent *event, uint64_t eventGeneration)
{
  return eventTryClaimReference(event, eventGeneration, 0);
}

static inline int eventManualTryClaimReference(aioUserEvent *event, uint64_t eventGeneration)
{
  return eventTryClaimReference(event, eventGeneration, 1);
}

void eventTimerSignal(aioUserEvent *event, uint32_t timerGeneration, uint64_t eventGeneration, uint64_t tickCount);
void eventManualReady(aioUserEvent *event);

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_EVENTS_H_
