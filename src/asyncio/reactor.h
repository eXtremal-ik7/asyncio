// Compact kernel handles and timer state shared by epoll and kqueue.
#ifndef __ASYNCIO_REACTOR_H_
#define __ASYNCIO_REACTOR_H_

#include "asyncioImpl.h"

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
  timer->header.timer.broken = 0;
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

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_REACTOR_H_
