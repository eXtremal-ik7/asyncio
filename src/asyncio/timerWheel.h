#ifndef __ASYNCIO_TIMERWHEEL_H_
#define __ASYNCIO_TIMERWHEEL_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "lifetime.h"
#include <stdint.h>

#define TIMER_TICK_MICROSECONDS 125000
#ifndef TIMER_WHEEL_LEVEL_BITS
#define TIMER_WHEEL_LEVEL_BITS 10
#endif
#define TIMER_WHEEL_SLOTS (1u << TIMER_WHEEL_LEVEL_BITS)
#ifndef TIMER_WHEEL_LEVELS
#define TIMER_WHEEL_LEVELS 4
#endif

typedef struct timerWheel {
  uint128 slots[TIMER_WHEEL_LEVELS][TIMER_WHEEL_SLOTS];
  uintptr_t occupancy[TIMER_WHEEL_LEVELS][TIMER_WHEEL_SLOTS / 64];
} timerWheel;

typedef struct TimerLoopState {
  volatile uintptr_t wakeTick;
  volatile uintptr_t preclearSequence;
  uint8_t pad[CACHE_LINE_SIZE - 2 * sizeof(uintptr_t)];
} TimerLoopState;

static const uintptr_t timerSleepEternal = UINTPTR_MAX - 1;
static const uintptr_t timerPreclearOverflowEntry = ((uintptr_t)1 << 32) + 1;
static const uintptr_t timerPreclearOverflowActiveMask = ((uintptr_t)1 << 32) - 1;

typedef struct asyncOpListLink {
  asyncOpRoot *op;
  uintptr_t generation;
  aioObjectRoot *object;
  uintptr_t objectGeneration;
  struct asyncOpListLink *next;
  uint64_t deadlineTick;
} asyncOpListLink;

void timerWheelInit(asyncBase *base, uint64_t currentTick);
void timerWheelTeardown(asyncBase *base);
int timerWheelInsert(asyncBase *base, asyncOpListLink *link, uint64_t cursor);
asyncOpListLink *timerWheelDetach(asyncBase *base, TimerLoopState *state, unsigned level, uint64_t windowStart);
void timerWheelProcessDetached(asyncBase *base, asyncOpListLink *link, uint64_t windowStart);
void timerWheelSweepTick(asyncBase *base, TimerLoopState *state, uint64_t tick);
void addToTimeoutQueue(asyncBase *base, asyncOpRoot *op);
void processTimeoutQueue(asyncBase *base, TimerLoopState *state, uint64_t currentTick);
uint64_t timerLoopPrepareSleep(asyncBase *base, TimerLoopState *state, uint64_t currentTick);
void timerLoopCancelSleep(TimerLoopState *state);
uint64_t getMonotonicTicks(void);

static inline uint64_t timerDeadlineTick(uint64_t absoluteMicroseconds)
{
  return absoluteMicroseconds / TIMER_TICK_MICROSECONDS + (absoluteMicroseconds % TIMER_TICK_MICROSECONDS != 0);
}

static inline uint32_t timerSleepMilliseconds(uint64_t wakeTick)
{
  if (wakeTick == UINT64_MAX)
    return UINT32_MAX;
  uint64_t currentTick = getMonotonicTicks();
  if (wakeTick <= currentTick)
    return 0;
  const uint32_t tickMs = TIMER_TICK_MICROSECONDS / 1000;
  const uint64_t tickLimit = 0x7FFFFFFFu / tickMs;
  uint64_t sleepTicks = wakeTick - currentTick;
  if (sleepTicks > tickLimit)
    sleepTicks = tickLimit;
  return (uint32_t)(sleepTicks * tickMs);
}

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_TIMERWHEEL_H_
