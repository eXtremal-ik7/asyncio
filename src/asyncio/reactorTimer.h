// Shared timer protocol of the readiness backends (epoll/kqueue): the kernel
// publication of an arming and the decode of a delivered timer event. Both
// sides of the kernel round-trip live in one place so the two backends cannot
// drift apart, and the protocol stays exhaustively unit-testable - the same
// reasoning as the combinerSelect* decision kernels in asyncioImpl.h. The
// kernel syscalls themselves (kevent/timerfd+epoll_ctl) stay in the backends.
// Windows/IOCP is not a reactor and keeps its own waitable-timer scheme.
//
// The core problem both protocols solve: a timer event can sit in a harvested
// batch while its operation completes and the same physical timer is rearmed.
// kqueue carries a never-reused full-width ident (creation prefix + arm
// sequence), so an old envelope is rejected exactly. epoll has only the
// pointer: operation timers therefore snapshot the current absolute
// CLOCK_MONOTONIC deadline and may adopt it only after that deadline has
// genuinely passed; no delivery reads timerfd, so a stale reader cannot drain
// a newer arm. Periodic user events use a weaker readiness probe: their sole
// applier serializes timerfd reads with stop/rearm, while generation and event
// incarnation prevent publication into a different logical schedule.
// Both protocols are pure value logic over the aioTimer fields and are always
// compiled on every platform; each backend calls its own flavor, unit tests
// exercise both anywhere.
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
  rtIdentSeqBits = (int)(sizeof(uintptr_t) * 4)
};

typedef struct aioTimer {
  asyncBase *base;
  // Single release/acquire publication word of the current arming; 0 = the
  // disarmed sentinel, and an armed store can never equal it:
  //  - kqueue: the full composite ident of this arming (seq part skips 0);
  //  - epoll: (operation generation << 1) | 1 - the armed bit keeps even a
  //    legally wrapped generation 0 distinguishable from "disarmed".
  // Written with release at arm time, read with acquire at event delivery:
  // the kernel round-trip carries no memory ordering, this field is the
  // synchronization edge; timer->op and the fields behind it may only be
  // read after it.
  uintptr_t tag;
  // kqueue only: generation of the armed operation, captured at arm time
  // (the publication word is occupied by the ident, so the generation rides
  // in a side field). Release store at arm, acquire load inside the decode's
  // seqlock triple - see reactorTimerDecodeIdent for why both orderings are
  // load-bearing. epoll packs the generation into tag instead and never
  // touches this field.
  uintptr_t armedGeneration;
  // epoll operation timers: absolute CLOCK_MONOTONIC deadline in nanoseconds.
  // This is a side field of tag and therefore follows the release/acquire
  // snapshot protocol below. User-event timers use the serialized timerfd
  // probe path and do not read it.
  uintptr_t armedDeadline;
  // Operation timers only: owner handle captured before kernel publication.
  // Once opSetStatus wins, operation storage may recycle and these fields are
  // the only legal route to the object combiner. User-event timers leave them
  // unused and use their own incarnation claim.
  uintptr_t armedObject;
  uintptr_t armedObjectGeneration;
  // User-event timers only: logical lifetime identity of immutable op. The
  // timer cell itself survives event reuse, so every delivered envelope must
  // carry this side value into the event's two-phase reference claim.
  uintptr_t armedEventIncarnation;
  // epoll: the timerfd. kqueue: the full composite ident of the current
  // arming (kevent ident for EV_ADD/EV_DELETE); plain field, only the arming
  // side reads and writes it - arm/stop of one timer are serialized by op
  // slot ownership, and the happens-before between incarnations rides the
  // pool push/pop of the recycle, like every plain field of the op itself.
  intptr_t fd;
  asyncOpRoot *op;
  // Kind is bound by the first arm, after opCode has been initialized, and is
  // immutable afterwards. registeredBase is epoll owner-only state; broken is
  // a kqueue tombstone request after a hard EV_DELETE failure.
  asyncBase *registeredBase;
  unsigned kind;
  unsigned broken;
} aioTimer;

typedef char aioTimerMustStayCompact[
  sizeof(aioTimer) <= 2 * CACHE_LINE_SIZE ? 1 : -1];

static inline void reactorTimerInitializeSharedState(aioTimer *timer)
{
  __uintptr_atomic_store(&timer->tag, 0, amoRelaxed);
  __uintptr_atomic_store(&timer->armedGeneration, 0, amoRelaxed);
  __uintptr_atomic_store(&timer->armedDeadline, 0, amoRelaxed);
  __uintptr_atomic_store(&timer->armedObject, 0, amoRelaxed);
  __uintptr_atomic_store(&timer->armedObjectGeneration, 0, amoRelaxed);
  __uintptr_atomic_store(&timer->armedEventIncarnation, 0, amoRelaxed);
}

// What a delivered kernel timer event means for the backend loop.
typedef enum ReactorTimerEventAction {
  rteIgnore = 0,      // stale doorbell: nothing may be touched
  rteExpireOperation, // realtime timeout: opCancel(timer->op, generation, aosTimeout)
  rteUserEvent        // a user-event timer tick: activate and deliver
} ReactorTimerEventAction;

// The one constructor of timer udata. EVERY kernel publication that can ever
// deliver an event with this timer's pointer must carry the udataTimer bit.
// Otherwise a timer pointer would enter the fd-object branch and be decoded
// as a compact object handle even though aioTimer has no AsyncObjectHead.
static inline int reactorTimerBindKind(aioTimer *timer, ReactorTimerKind kind)
{
  if (timer->kind == rtkUnknown)
    timer->kind = (unsigned)kind;
  return timer->kind == (unsigned)kind;
}

static inline void *reactorTimerUdata(aioTimer *timer)
{
  return __tagged_pointer_make(timer,
                               udataTimer |
                                 (timer->kind == rtkUserEvent ? udataUserEvent : 0));
}

static inline void reactorTimerDisarm(aioTimer *timer)
{
  __uintptr_atomic_store(&timer->tag, 0, amoRelaxed);
}

// ---- kqueue flavor: per-arming ident delivered inside the event ----------

// Publish an arming. Computes the next composite ident into timer->fd (the
// caller registers EV_ADD with it), captures the operation's generation and
// releases both through the tag. The kernel keeps the ident in the knote and
// returns it in the delivered event: the arming's identity travels full-width
// through the kernel, nothing is read back from live mutable state.
static inline void *reactorTimerArmIdentGeneration(aioTimer *timer,
                                                   asyncOpRoot *op,
                                                   uintptr_t generation)
{
  // The backend binds the immutable kind while preparing the timer. Repeating
  // reactorTimerBindKind here added a branch/load to every arm in release
  // builds without protecting any additional transition.
  assert(timer->kind == (unsigned)(op->opCode == actUserEvent ?
                                     rtkUserEvent : rtkOperation));
  uintptr_t seqMask = (((uintptr_t)1) << rtIdentSeqBits) - 1;
  uintptr_t base = (uintptr_t)timer->fd & ~seqMask;
  uintptr_t seq = ((uintptr_t)timer->fd + 1) & seqMask;
  // The backend rotates the physical timer cell before this point. Reusing 1
  // after wrap would let an indefinitely delayed kevent validate against a
  // new arming of the same cell.
  assert(seq != 0 && "kqueue timer ident sequence exhausted without rotation");
  reactorTimerDisarm(timer);
  timer->fd = (intptr_t)(base | seq);
  if (timer->kind == rtkOperation) {
    aioObjectRoot *object = op->object;
    __uintptr_atomic_store(&timer->armedObject,
                           (uintptr_t)object,
                           amoRelease);
    __uintptr_atomic_store(&timer->armedObjectGeneration,
                           __uintptr_atomic_load(&object->Head.gen, amoRelaxed),
                           amoRelease);
  } else {
    __uintptr_atomic_store(
      &timer->armedEventIncarnation,
      __uintptr_atomic_load(&((aioUserEvent*)op)->incarnation, amoRelaxed),
      amoRelease);
  }
  // Release, not relaxed: the decode's seqlock triple acquires this store,
  // and the resulting synchronizes-with edge is what forces a reader that
  // observed a NEWER generation to also observe a newer tag (the previous
  // stop's tag store travels ahead of this one through the recycle's pool
  // release/acquire chain) - see reactorTimerDecodeIdent.
  __uintptr_atomic_store(&timer->armedGeneration, generation, amoRelease);
  __uintptr_atomic_store(&timer->tag, (uintptr_t)timer->fd, amoRelease);
  return reactorTimerUdata(timer);
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
static inline ReactorTimerEventAction reactorTimerDecodeIdentHandle(
  aioTimer *timer,
  uintptr_t udataBits,
  uintptr_t eventIdent,
  uintptr_t *armedGeneration,
  aioObjectRoot **armedObject,
  uintptr_t *armedObjectGeneration,
  uintptr_t *armedEventIncarnation)
{
  uintptr_t armed = __uintptr_atomic_load(&timer->tag, amoAcquire);
  if (armed == 0 || armed != eventIdent)
    return rteIgnore;
  uintptr_t generation = __uintptr_atomic_load(&timer->armedGeneration, amoAcquire);
  aioObjectRoot *object = 0;
  uintptr_t objectGeneration = 0;
  uintptr_t eventIncarnation = 0;
  if (udataBits & udataUserEvent) {
    eventIncarnation =
      __uintptr_atomic_load(&timer->armedEventIncarnation, amoAcquire);
  } else {
    object = (aioObjectRoot*)__uintptr_atomic_load(&timer->armedObject, amoAcquire);
    objectGeneration = __uintptr_atomic_load(&timer->armedObjectGeneration,
                                              amoAcquire);
  }
  // If generation came from a newer arm, its acquire pulls the intervening
  // tag<-0 into happens-before; per-location coherence then makes a relaxed
  // recheck sufficient to reject the old ident. If it came from this arm, the
  // first tag acquire already published all of its fields.
  if (__uintptr_atomic_load(&timer->tag, amoRelaxed) != armed)
    return rteIgnore;
  *armedGeneration = generation;
  *armedObject = object;
  *armedObjectGeneration = objectGeneration;
  *armedEventIncarnation = eventIncarnation;
  // The user-event bit rides in the udata tag: op->opCode belongs to the
  // operation and cannot be read before the event is validated.
  return (udataBits & udataUserEvent) ? rteUserEvent : rteExpireOperation;
}

static inline ReactorTimerEventAction reactorTimerDecodeIdent(aioTimer *timer,
                                                              uintptr_t udataBits,
                                                              uintptr_t eventIdent,
                                                              uintptr_t *armedGeneration)
{
  aioObjectRoot *object;
  uintptr_t objectGeneration;
  uintptr_t eventIncarnation;
  return reactorTimerDecodeIdentHandle(timer,
                                       udataBits,
                                       eventIdent,
                                       armedGeneration,
                                       &object,
                                       &objectGeneration,
                                       &eventIncarnation);
}

// ---- epoll user-event flavor: serialized timerfd readiness probes --------

// Publish a periodic user-event arming. timerfd_settime runs before this call
// and checked ADD/MOD runs after it. A loop delivery only snapshots the tag
// and posts a probe; the event applier is the sole reader of timerfd and the
// sole stop/rearm writer, so no harvested loop thread can drain a newer arm.
static inline void *reactorTimerArmCountGeneration(aioTimer *timer,
                                                   asyncOpRoot *op,
                                                   uintptr_t generation)
{
  assert(timer->kind == (unsigned)(op->opCode == actUserEvent ?
                                     rtkUserEvent : rtkOperation));
  reactorTimerDisarm(timer);
  if (timer->kind == rtkUserEvent) {
    __uintptr_atomic_store(
      &timer->armedEventIncarnation,
      __uintptr_atomic_load(&((aioUserEvent*)op)->incarnation, amoRelaxed),
      amoRelease);
  }
  __uintptr_atomic_store(&timer->tag, (generation << 1) | 1, amoRelease);
  return reactorTimerUdata(timer);
}

static inline void *reactorTimerArmCount(aioTimer *timer, asyncOpRoot *op)
{
  return reactorTimerArmCountGeneration(timer, op, opGetGeneration(op));
}

// Snapshot the two fields needed by an epoll user-event probe. A newer arm
// may publish its incarnation before its final tag; acquiring that side value
// also observes the intervening tag=0, so the relaxed recheck rejects the
// mixed snapshot by per-location coherence.
static inline int reactorTimerDecodeCountProbe(aioTimer *timer,
                                               uintptr_t *armedGeneration,
                                               uintptr_t *armedIncarnation)
{
  uintptr_t armed = __uintptr_atomic_load(&timer->tag, amoAcquire);
  if (armed == 0)
    return 0;
  uintptr_t incarnation =
    __uintptr_atomic_load(&timer->armedEventIncarnation, amoAcquire);
  if (__uintptr_atomic_load(&timer->tag, amoRelaxed) != armed)
    return 0;
  *armedGeneration = armed >> 1;
  *armedIncarnation = incarnation;
  return incarnation != 0;
}

// epoll operation timer publication. Every arm explicitly crosses tag==0;
// the release side store plus the final release tag let an old harvested
// envelope take a coherent snapshot even while another thread rearms this
// physical timer cell.
static inline void *reactorTimerArmDeadlineGeneration(aioTimer *timer,
                                                      asyncOpRoot *op,
                                                      uintptr_t generation,
                                                      uintptr_t deadline)
{
  reactorTimerDisarm(timer);
  aioObjectRoot *object = op->object;
  __uintptr_atomic_store(&timer->armedDeadline, deadline, amoRelease);
  __uintptr_atomic_store(&timer->armedObject, (uintptr_t)object, amoRelease);
  __uintptr_atomic_store(&timer->armedObjectGeneration,
                         __uintptr_atomic_load(&object->Head.gen, amoRelaxed),
                         amoRelease);
  __uintptr_atomic_store(&timer->tag, (generation << 1) | 1, amoRelease);
  return reactorTimerUdata(timer);
}

static inline ReactorTimerEventAction reactorTimerDecodeDeadlineHandle(
  aioTimer *timer,
  uintptr_t udataBits,
  uintptr_t now,
  uintptr_t *armedGeneration,
  aioObjectRoot **armedObject,
  uintptr_t *armedObjectGeneration)
{
  uintptr_t armed = __uintptr_atomic_load(&timer->tag, amoAcquire);
  if (armed == 0)
    return rteIgnore;
  uintptr_t deadline = __uintptr_atomic_load(&timer->armedDeadline, amoAcquire);
  aioObjectRoot *object =
    (aioObjectRoot*)__uintptr_atomic_load(&timer->armedObject, amoAcquire);
  uintptr_t objectGeneration =
    __uintptr_atomic_load(&timer->armedObjectGeneration, amoAcquire);
  if (__uintptr_atomic_load(&timer->tag, amoRelaxed) != armed || now < deadline)
    return rteIgnore;
  *armedGeneration = armed >> 1;
  *armedObject = object;
  *armedObjectGeneration = objectGeneration;
  return (udataBits & udataUserEvent) ? rteUserEvent : rteExpireOperation;
}

static inline ReactorTimerEventAction reactorTimerDecodeDeadline(
  aioTimer *timer,
  uintptr_t udataBits,
  uintptr_t now,
  uintptr_t *armedGeneration)
{
  aioObjectRoot *object;
  uintptr_t objectGeneration;
  return reactorTimerDecodeDeadlineHandle(timer,
                                          udataBits,
                                          now,
                                          armedGeneration,
                                          &object,
                                          &objectGeneration);
}

// ---- user-event tick delivery plan ----------------------------------------

// What the loop must do after a user-event tick was decoded as valid.
// activated is the eventReferenceTryActivate outcome, counterExhausted means this was
// the last counted tick. oneshotDelivery describes the backend: epoll's
// EPOLLONESHOT registration is consumed by every delivery and must be
// re-armed for the timer to keep reporting (kqueue's periodic EVFILT_TIMER
// stays armed in the kernel, so it passes 0 and rearm never fires).
typedef struct ReactorUserEventTickPlan {
  int activate;   // deliver: deactivate, finishMethod, drop the reference
  int stopTimer;  // counted out: stop the kernel timer, registration dies with it
  int rearm;      // re-enable the consumed oneshot registration
} ReactorUserEventTickPlan;

static inline ReactorUserEventTickPlan reactorTimerUserEventTick(int activated,
                                                                 int counterExhausted,
                                                                 int oneshotDelivery)
{
  ReactorUserEventTickPlan plan;
  plan.activate = activated;
  plan.stopTimer = activated && counterExhausted;
  // A dropped tick (activation collided with userEventActivate) still
  // re-arms: the kernel timer keeps ticking, and losing the only oneshot
  // registration would silence the periodic event forever
  plan.rearm = oneshotDelivery && !plan.stopTimer;
  return plan;
}

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_REACTORTIMER_H_
