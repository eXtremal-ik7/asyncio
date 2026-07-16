// Shared protocols of the readiness backends (epoll/kqueue): compact kernel
// handles and reactor timers. Both sides of each kernel round-trip live in one
// place so the backends cannot drift apart, while their syscalls
// (kevent/timerfd+epoll_ctl) stay in the backend files. Windows/IOCP is not a
// reactor and uses its completion packet fields directly.
//
// Compact handles carry an aligned objectHeader pointer and a generation
// fragment. The decoder compares that fragment with tag.high and returns the
// common header plus the full generation; the immutable header type selects
// the concrete validation protocol. The timer half solves a different
// problem: an event can
// sit in a harvested batch while its operation completes and the same physical
// timer is rearmed.
// kqueue carries a never-reused full-width ident (creation prefix + arm
// sequence), so an old envelope is rejected exactly. epoll has only the
// pointer: operation timers therefore snapshot the current absolute
// CLOCK_MONOTONIC deadline and may adopt it only after that deadline has
// genuinely passed. Periodic user events let their sole owner serialize
// timerfd reads with start/stop, while generation and event incarnation
// prevent publication into a different logical schedule.
// Timer transitions are pure value logic over aioTimer fields and are always
// compiled on every platform; each backend calls its own flavor, and unit
// tests exercise both anywhere.
#ifndef __ASYNCIO_REACTOR_H_
#define __ASYNCIO_REACTOR_H_

#include "asyncioImpl.h"

#ifdef __cplusplus
extern "C" {
#endif

// ---- compact common-header handles ---------------------------------------

enum {
  REACTOR_HANDLE_GENERATION_BITS = 22
};

// Every target is 64-byte aligned, so a 48-bit userspace pointer needs 42
// bits. The remaining 22 carry tag.high modulo 2^22. An ABA therefore needs
// the same physical cell to advance through a complete compact-generation
// cycle while an already harvested kernel event remains stalled. The type is
// intentionally not duplicated in the envelope: it lives immediately after
// the aligned tag in objectHeader and is immutable for the physical cell.
enum {
  REACTOR_HANDLE_GENERATION_SHIFT = 64 - REACTOR_HANDLE_GENERATION_BITS,
  REACTOR_HANDLE_POINTER_SHIFT = TAGGED_POINTER_DATA_SIZE
};

#define REACTOR_HANDLE_GENERATION_MASK ((1ULL << REACTOR_HANDLE_GENERATION_BITS) - 1)
#define REACTOR_HANDLE_POINTER_MASK ((1ULL << REACTOR_HANDLE_GENERATION_SHIFT) - 1)

static inline uint64_t reactorHandleEncode(objectHeader *header)
{
  uint64_t pointer = (uint64_t)header;
  assert(header && !(pointer & TAGGED_POINTER_DATA_MASK) && pointer < (1ULL << 48));
  uint64_t generation = objectHeaderGeneration(header);
  return (pointer >> REACTOR_HANDLE_POINTER_SHIFT) | ((generation & REACTOR_HANDLE_GENERATION_MASK) << REACTOR_HANDLE_GENERATION_SHIFT);
}

static inline objectHeader *reactorHandleDecode(uint64_t encoded, uint64_t *generation)
{
  objectHeader *decoded = (objectHeader*)((encoded & REACTOR_HANDLE_POINTER_MASK) << REACTOR_HANDLE_POINTER_SHIFT);
  uint64_t current = objectHeaderGeneration(decoded);
  // The envelope supplies the low 22 bits while the still-readable header
  // supplies the epoch prefix. Equality with the current full generation is
  // decided only by the type-specific claim; a stale compact value therefore
  // remains a harmless unequal candidate without a decode-time branch.
  *generation = (current & ~REACTOR_HANDLE_GENERATION_MASK) | (encoded >> REACTOR_HANDLE_GENERATION_SHIFT);
  return decoded;
}

typedef enum ReactorTimerKind {
  rtkUnknown = 0,
  rtkOperation,
  rtkUserEvent
} ReactorTimerKind;

// kqueue ident split: high half is the timer's permanent base (creation
// counter), low half is the per-timer arm sequence. Op pools are
// process-global statics, so a recycled slot - and its timer - can serve any
// base: the creation counter must be process-global too, or two bases could
// hand out colliding idents. On 32-bit BSD targets the halves shrink to
// 16 bits each - outside the supported platform set, noted for honesty.
enum {
  rtIdentSeqBits = (int)(sizeof(uint64_t) * 4)
};

struct aioTimer {
  // Common kernel-visible prefix. tag.low is the release/acquire publication
  // word of the current arming; 0 = the
  // disarmed sentinel, and an armed store can never equal it:
  //  - kqueue: the full composite ident of this arming (seq part skips 0);
  //  - epoll: 1, with tag.high carrying the generation.
  // tag.high is the full armed operation/schedule generation carried compactly
  // by the common reactor envelope. The final release store to tag.low is the
  // publication edge for target and the side fields below.
  objectHeader header;
  union {
    // epoll operation timers: absolute CLOCK_MONOTONIC deadline in
    // nanoseconds. This side field follows the release/acquire snapshot
    // protocol below.
    uint64_t armedDeadline;

    // User-event OWNER-only applied state. A zero remaining count denotes an
    // unlimited timer while armed; positive public counters fit in uint32_t.
    aioTimerUserEventState userEvent;
  };
  // Operation timers only: owner handle captured before kernel publication.
  // Once opSetStatus wins, operation storage may recycle and these fields are
  // the only legal route to the object combiner. User-event timers leave them
  // unused and use their own incarnation claim.
  uint64_t armedObject;
  uint64_t armedObjectGeneration;
  // User-event timers only: logical lifetime identity of the event. The
  // timer cell itself survives event reuse, so every delivered envelope must
  // carry this side value into the event's two-phase reference claim.
  uint64_t armedEventGeneration;
  // epoll: the timerfd. kqueue: the full composite ident of the current
  // arming (kevent ident for EV_ADD/EV_DELETE); plain field, only the arming
  // side reads and writes it - arm/stop of one timer are serialized by op
  // slot ownership, and the happens-before between incarnations rides the
  // pool push/pop of the recycle, like every plain field of the op itself.
  intptr_t fd;
  // Immutable logical target while this cell is paired with storage. The
  // header timer kind determines whether it is an asyncOpRoot or an
  // aioUserEvent; keeping it typeless avoids forcing user events to embed a
  // fake asyncOpRoot.
  void *target;
};

typedef char aioTimerMustStayCompact[sizeof(aioTimer) <= 2 * CACHE_LINE_SIZE ? 1 : -1];
typedef char aioTimerUserEventStateMustFollowHeader[offsetof(aioTimer, userEvent) == sizeof(objectHeader) ? 1 : -1];

static inline void reactorTimerInitializeSharedState(aioTimer *timer)
{
  __uint64_atomic_store(&timer->header.tag.low, 0, amoRelaxed);
  __uint64_atomic_store(&timer->header.tag.high, 0, amoRelaxed);
  objectHeaderSetType(&timer->header, ohtTimer);
  timer->header.timer.kind = rtkUnknown;
  timer->header.timer.broken = 0;
  timer->header.timer.registered = 0;
  timer->header.timer.reserved = 0;
  __uint64_atomic_store(&timer->armedDeadline, 0, amoRelaxed);
  __uint64_atomic_store(&timer->armedObject, 0, amoRelaxed);
  __uint64_atomic_store(&timer->armedObjectGeneration, 0, amoRelaxed);
  __uint64_atomic_store(&timer->armedEventGeneration, 0, amoRelaxed);
}

// What a delivered kernel timer event means for the backend loop.
typedef enum ReactorTimerEventAction {
  rteIgnore = 0,      // stale doorbell: nothing may be touched
  rteExpireOperation, // realtime timeout: opCancel(timer->target, generation, aosTimeout)
  rteUserEvent        // a user-event timer tick: activate and deliver
} ReactorTimerEventAction;

// Bind the concrete timer protocol before the common header is encoded into
// kernel payload. The immutable kind must agree at every later arm.
static inline int reactorTimerBindKind(aioTimer *timer, ReactorTimerKind kind)
{
  if (timer->header.timer.kind == rtkUnknown)
    timer->header.timer.kind = (uint8_t)kind;
  return timer->header.timer.kind == (uint8_t)kind;
}

static inline void *reactorTimerUdata(aioTimer *timer)
{
  return (void*)(uintptr_t)reactorHandleEncode(&timer->header);
}

static inline void reactorTimerDisarm(aioTimer *timer)
{
  __uint64_atomic_store(&timer->header.tag.low, 0, amoRelaxed);
}

// ---- kqueue flavor: per-arming ident delivered inside the event ----------

// Publish an arming. Computes the next composite ident into timer->fd (the
// caller registers EV_ADD with it), captures the operation's generation and
// releases both through the tag. The kernel keeps the ident in the knote and
// returns it in the delivered event: the arming's identity travels full-width
// through the kernel, nothing is read back from live mutable state.
static inline void *reactorTimerArmIdentTargetGeneration(aioTimer *timer, void *target, ReactorTimerKind kind, uint64_t generation)
{
  // The backend binds the immutable kind while preparing the timer. Repeating
  // reactorTimerBindKind here added a branch/load to every arm in release
  // builds without protecting any additional transition.
  assert(timer->header.timer.kind == (uint8_t)kind);
  timer->target = target;
  uint64_t seqMask = (1ULL << rtIdentSeqBits) - 1;
  uint64_t base = (uint64_t)timer->fd & ~seqMask;
  uint64_t seq = ((uint64_t)timer->fd + 1) & seqMask;
  // The backend rotates the physical timer cell before this point. Reusing 1
  // after wrap would let an indefinitely delayed kevent validate against a
  // new arming of the same cell.
  assert(seq != 0 && "kqueue timer ident sequence exhausted without rotation");
  reactorTimerDisarm(timer);
  timer->fd = (intptr_t)(base | seq);
  if (kind == rtkOperation) {
    asyncOpRoot *op = (asyncOpRoot*)target;
    aioObjectRoot *object = op->object;
    __uint64_atomic_store(&timer->armedObject, (uint64_t)object, amoRelease);
    __uint64_atomic_store(&timer->armedObjectGeneration, objectHeaderGeneration(&object->header), amoRelease);
  } else {
    aioUserEvent *event = (aioUserEvent*)target;
    __uint64_atomic_store(&timer->armedEventGeneration, eventHandleGeneration(event), amoRelease);
  }
  // Release, not relaxed: the decode's seqlock triple acquires this store,
  // and the resulting synchronizes-with edge is what forces a reader that
  // observed a NEWER generation to also observe a newer tag (the previous
  // stop's tag store travels ahead of this one through the recycle's pool
  // release/acquire chain) - see reactorTimerDecodeIdent.
  __uint64_atomic_store(&timer->header.tag.high, generation, amoRelease);
  __uint64_atomic_store(&timer->header.tag.low, (uint64_t)timer->fd, amoRelease);
  return reactorTimerUdata(timer);
}

static inline void *reactorTimerArmIdentGeneration(aioTimer *timer, asyncOpRoot *op, uint64_t generation)
{
  return reactorTimerArmIdentTargetGeneration(timer, op, rtkOperation, generation);
}

static inline void *reactorTimerArmEventIdentGeneration(aioTimer *timer, aioUserEvent *event, uint64_t generation)
{
  return reactorTimerArmIdentTargetGeneration(timer, event, rtkUserEvent, generation);
}

static inline void *reactorTimerArmIdent(aioTimer *timer, asyncOpRoot *op)
{
  return reactorTimerArmIdentGeneration(timer, op, opGetGeneration(op));
}

// Decode a delivered kqueue timer event. eventIdent is the knote ident that
// travelled with the event; a mismatch with the published tag means the
// doorbell belongs to a previous arming of this timer (stop/recycle/rearm
// happened while it sat in a harvested batch) and nothing may be touched.
//
// The generation is read under a seqlock triple. Without it the decode,
// having validated ident N, could read armedGeneration of arming N+1: the
// arm's two stores release in order, but a reader that missed the second
// may still see the first. Acquiring a NEWER generation synchronizes with
// its arm store, which the previous stop's tag<-0 happens-before (pool
// push/pop of the recycle), so the re-read tag is then guaranteed to differ
// and the triple rejects. The only recycle path without a stop's tag<-0 is
// expiry-by-timeout - driven by this very doorbell, which is unique per
// arming (EV_ONESHOT), so it cannot race itself.
static inline ReactorTimerEventAction reactorTimerDecodeIdentHandle(aioTimer *timer,
                                                                    uint64_t envelopeGeneration,
                                                                    uint64_t eventIdent,
                                                                    uint64_t *armedGeneration,
                                                                    aioObjectRoot**armedObject,
                                                                    uint64_t *armedObjectGeneration,
                                                                    uint64_t *armedEventGeneration)
{
  uint64_t armed = __uint64_atomic_load(&timer->header.tag.low, amoAcquire);
  if (armed == 0 || armed != eventIdent)
    return rteIgnore;
  uint64_t generation = __uint64_atomic_load(&timer->header.tag.high, amoAcquire);
  if (generation != envelopeGeneration)
    return rteIgnore;
  aioObjectRoot *object = 0;
  uint64_t objectGeneration = 0;
  uint64_t eventGeneration = 0;
  if (timer->header.timer.kind == rtkUserEvent) {
    eventGeneration = __uint64_atomic_load(&timer->armedEventGeneration, amoAcquire);
  } else {
    object = (aioObjectRoot*)__uint64_atomic_load(&timer->armedObject, amoAcquire);
    objectGeneration = __uint64_atomic_load(&timer->armedObjectGeneration, amoAcquire);
  }
  // If generation came from a newer arm, its acquire pulls the intervening
  // tag<-0 into happens-before; per-location coherence then makes a relaxed
  // recheck sufficient to reject the old ident. If it came from this arm, the
  // first tag acquire already published all of its fields.
  if (__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) != armed || objectHeaderGeneration(&timer->header) != generation)
    return rteIgnore;
  *armedGeneration = generation;
  *armedObject = object;
  *armedObjectGeneration = objectGeneration;
  *armedEventGeneration = eventGeneration;
  return timer->header.timer.kind == rtkUserEvent ? rteUserEvent : rteExpireOperation;
}

static inline ReactorTimerEventAction reactorTimerDecodeIdent(aioTimer *timer,
                                                              uint64_t envelopeGeneration,
                                                              uint64_t eventIdent,
                                                              uint64_t *armedGeneration)
{
  aioObjectRoot *object;
  uint64_t objectGeneration;
  uint64_t eventGeneration;
  return reactorTimerDecodeIdentHandle(timer, envelopeGeneration, eventIdent, armedGeneration, &object, &objectGeneration, &eventGeneration);
}

// ---- epoll user-event flavor: serialized timerfd counts ------------------

// Publish a periodic user-event arming. timerfd_settime runs before this call
// and checked ADD/MOD runs after it. A loop delivery snapshots this arm; the
// event owner remains the sole reader of timerfd and the sole start/stop
// writer, so no harvested loop thread can drain a newer arm.
static inline void *reactorTimerArmEventCountGeneration(aioTimer *timer, aioUserEvent *event, uint64_t generation)
{
  assert(timer->header.timer.kind == rtkUserEvent);
  timer->target = event;
  reactorTimerDisarm(timer);
  __uint64_atomic_store(&timer->armedEventGeneration, eventHandleGeneration(event), amoRelease);
  __uint64_atomic_store(&timer->header.tag.high, generation, amoRelease);
  __uint64_atomic_store(&timer->header.tag.low, 1, amoRelease);
  return reactorTimerUdata(timer);
}

// Snapshot the two fields needed by an epoll user-event readiness event. A newer arm
// may publish its incarnation before its final tag; acquiring that side value
// also observes the intervening tag=0, so the relaxed recheck rejects the
// mixed snapshot by per-location coherence.
static inline int reactorTimerDecodeCount(aioTimer *timer,
                                         uint64_t envelopeGeneration,
                                         uint64_t *armedGeneration,
                                         uint64_t *armedEventGeneration)
{
  uint64_t armed = __uint64_atomic_load(&timer->header.tag.low, amoAcquire);
  if (armed == 0)
    return 0;
  uint64_t generation = __uint64_atomic_load(&timer->header.tag.high, amoAcquire);
  if (generation != envelopeGeneration)
    return 0;
  uint64_t eventGeneration = __uint64_atomic_load(&timer->armedEventGeneration, amoAcquire);
  if (__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) != armed || objectHeaderGeneration(&timer->header) != generation)
    return 0;
  *armedGeneration = generation;
  *armedEventGeneration = eventGeneration;
  return 1;
}

// epoll operation timer publication. Every arm explicitly crosses tag==0;
// the release side store plus the final release tag let an old harvested
// envelope take a coherent snapshot even while another thread rearms this
// physical timer cell.
static inline void *reactorTimerArmDeadlineGeneration(aioTimer *timer, asyncOpRoot *op, uint64_t generation, uint64_t deadline)
{
  reactorTimerDisarm(timer);
  aioObjectRoot *object = op->object;
  __uint64_atomic_store(&timer->armedDeadline, deadline, amoRelease);
  __uint64_atomic_store(&timer->armedObject, (uint64_t)object, amoRelease);
  __uint64_atomic_store(&timer->armedObjectGeneration, objectHeaderGeneration(&object->header), amoRelease);
  __uint64_atomic_store(&timer->header.tag.high, generation, amoRelease);
  __uint64_atomic_store(&timer->header.tag.low, 1, amoRelease);
  return reactorTimerUdata(timer);
}

static inline ReactorTimerEventAction reactorTimerDecodeDeadlineHandle(aioTimer *timer,
                                                                       uint64_t envelopeGeneration,
                                                                       uint64_t now,
                                                                       uint64_t *armedGeneration,
                                                                       aioObjectRoot**armedObject,
                                                                       uint64_t *armedObjectGeneration)
{
  uint64_t armed = __uint64_atomic_load(&timer->header.tag.low, amoAcquire);
  if (armed == 0)
    return rteIgnore;
  uint64_t generation = __uint64_atomic_load(&timer->header.tag.high, amoAcquire);
  if (generation != envelopeGeneration)
    return rteIgnore;
  uint64_t deadline = __uint64_atomic_load(&timer->armedDeadline, amoAcquire);
  aioObjectRoot *object = (aioObjectRoot*)__uint64_atomic_load(&timer->armedObject, amoAcquire);
  uint64_t objectGeneration = __uint64_atomic_load(&timer->armedObjectGeneration, amoAcquire);
  if (__uint64_atomic_load(&timer->header.tag.low, amoRelaxed) != armed ||
      objectHeaderGeneration(&timer->header) != generation ||
      now < deadline)
    return rteIgnore;
  *armedGeneration = generation;
  *armedObject = object;
  *armedObjectGeneration = objectGeneration;
  return rteExpireOperation;
}

static inline ReactorTimerEventAction reactorTimerDecodeDeadline(aioTimer *timer,
                                                                 uint64_t envelopeGeneration,
                                                                 uint64_t now,
                                                                 uint64_t *armedGeneration)
{
  aioObjectRoot *object;
  uint64_t objectGeneration;
  return reactorTimerDecodeDeadlineHandle(timer, envelopeGeneration, now, armedGeneration, &object, &objectGeneration);
}

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_REACTOR_H_
