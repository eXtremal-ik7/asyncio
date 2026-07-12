// Shared timer protocol of the readiness backends (epoll/kqueue): the kernel
// publication of an arming and the decode of a delivered timer event. Both
// sides of the kernel round-trip live in one place so the two backends cannot
// drift apart, and the protocol stays exhaustively unit-testable - the same
// reasoning as the combinerSelect* decision kernels in asyncioImpl.h. The
// kernel syscalls themselves (kevent/timerfd+epoll_ctl) stay in the backends.
// Windows/IOCP is not a reactor and keeps its own waitable-timer scheme.
//
// The core problem both protocols solve: a timer event is anonymous. It can
// sit in another loop thread's harvested batch while the operation completes,
// its storage recycles and the SAME timer is re-armed - so the delivery side
// must not validate the doorbell against live mutable state alone (that
// adopts the new arming's identity and expires a fresh operation), and it
// cannot rely on identity bits squeezed into the udata tag (finite bits wrap:
// ABA). Each backend instead uses an exact kernel-provided channel:
//  - kqueue delivers the full-width knote ident inside the event; idents are
//    per-arming and never reused (composite: creation base + arm sequence);
//  - epoll has no second payload field, but timerfd_settime at arm time
//    resets the fd's expiration counter, so a read() > 0 after the armed
//    check proves the CURRENT arming expired - and expiring the current
//    arming when its deadline has genuinely passed is correct no matter
//    whose stale envelope delivered the wakeup.
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
  aioObjectRoot root;
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
  // epoll: the timerfd. kqueue: the full composite ident of the current
  // arming (kevent ident for EV_ADD/EV_DELETE); plain field, only the arming
  // side reads and writes it - arm/stop of one timer are serialized by op
  // slot ownership, and the happens-before between incarnations rides the
  // pool push/pop of the recycle, like every plain field of the op itself.
  intptr_t fd;
  asyncOpRoot *op;
} aioTimer;

// What a delivered kernel timer event means for the backend loop.
typedef enum ReactorTimerEventAction {
  rteIgnore = 0,      // stale doorbell: nothing may be touched
  rteExpireOperation, // realtime timeout: opCancel(timer->op, generation, aosTimeout)
  rteUserEvent        // a user-event timer tick: activate and deliver
} ReactorTimerEventAction;

// The one constructor of timer udata. EVERY kernel publication that can ever
// deliver an event with this timer's pointer must carry the udataTimer bit -
// including the disabled registrations made by InitializeTimer/StopTimer on
// epoll: EPOLLERR/EPOLLHUP are mask-exempt, and an unmarked timer pointer
// would route such an event into the fd-object branch, pushing a combiner
// signal through the aioTimer's never-initialized Head.
static inline void *reactorTimerUdata(aioTimer *timer, int userEvent)
{
  return __tagged_pointer_make(timer, udataTimer | (userEvent ? udataUserEvent : 0));
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
  uintptr_t seqMask = (((uintptr_t)1) << rtIdentSeqBits) - 1;
  uintptr_t base = (uintptr_t)timer->fd & ~seqMask;
  uintptr_t seq = ((uintptr_t)timer->fd + 1) & seqMask;
  if (seq == 0)
    seq = 1;  // the seq part skips 0 so a full ident never equals the disarmed sentinel
  timer->fd = (intptr_t)(base | seq);
  // Release, not relaxed: the decode's seqlock triple acquires this store,
  // and the resulting synchronizes-with edge is what forces a reader that
  // observed a NEWER generation to also observe a newer tag (the previous
  // stop's tag store travels ahead of this one through the recycle's pool
  // release/acquire chain) - see reactorTimerDecodeIdent.
  __uintptr_atomic_store(&timer->armedGeneration, generation, amoRelease);
  __uintptr_atomic_store(&timer->tag, (uintptr_t)timer->fd, amoRelease);
  return reactorTimerUdata(timer, op->opCode == actUserEvent);
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
static inline ReactorTimerEventAction reactorTimerDecodeIdent(aioTimer *timer,
                                                              uintptr_t udataBits,
                                                              uintptr_t eventIdent,
                                                              uintptr_t *armedGeneration)
{
  uintptr_t armed = __uintptr_atomic_load(&timer->tag, amoAcquire);
  if (armed == 0 || armed != eventIdent)
    return rteIgnore;
  uintptr_t generation = __uintptr_atomic_load(&timer->armedGeneration, amoAcquire);
  // If generation came from a newer arm, its acquire pulls the intervening
  // tag<-0 into happens-before; per-location coherence then makes a relaxed
  // recheck sufficient to reject the old ident. If it came from this arm, the
  // first tag acquire already published all of its fields.
  if (__uintptr_atomic_load(&timer->tag, amoRelaxed) != armed)
    return rteIgnore;
  *armedGeneration = generation;
  // The user-event bit rides in the udata tag: op->opCode belongs to the
  // operation and cannot be read before the event is validated.
  return (udataBits & udataUserEvent) ? rteUserEvent : rteExpireOperation;
}

// ---- epoll flavor: the timerfd expiration counter as the arming oracle ---

// Publish an arming. ORDER CONTRACT with the backend: timerfd_settime runs
// BEFORE this call, EPOLL_CTL_MOD (re-enabling the EPOLLONESHOT registration)
// runs AFTER it. The MOD is the publication point - no doorbell of this
// arming can be delivered earlier - and a delivery's acquire of the tag then
// orders the settime (which reset the fd's expiration counter) before that
// delivery's read(): a counter residue of a previous arming can never be
// attributed to this one.
static inline void *reactorTimerArmCountGeneration(aioTimer *timer,
                                                   asyncOpRoot *op,
                                                   uintptr_t generation)
{
  __uintptr_atomic_store(&timer->tag, (generation << 1) | 1, amoRelease);
  return reactorTimerUdata(timer, op->opCode == actUserEvent);
}

static inline void *reactorTimerArmCount(aioTimer *timer, asyncOpRoot *op)
{
  return reactorTimerArmCountGeneration(timer, op, opGetGeneration(op));
}

// The armed gate BEFORE the backend touches the fd. On 0 the doorbell is
// stale (timer stopped) and the fd must NOT be read: the drain belongs to
// the stop path, and a stale reader could otherwise swallow the expiration
// count of the next arming. Nonzero return is the tag value to pass into
// reactorTimerDecodeCount after the read.
static inline uintptr_t reactorTimerArmedCountTag(aioTimer *timer)
{
  return __uintptr_atomic_load(&timer->tag, amoAcquire);
}

// Decode a delivered epoll timer event from the armed tag and the expiration
// count read from the timerfd (0 when the read returned EAGAIN or short).
// count > 0 proves the arming that last ran settime has expired; together
// with the armed gate it means the CURRENT arming's deadline has genuinely
// passed, and expiring it with its own generation is correct no matter whose
// stale envelope delivered the wakeup. A racing newer arming only makes the
// generation CAS in opCancel lose - benign. count == 0 is a stale doorbell
// of a re-armed timer: the settime of the current arming reset the counter
// and its deadline is still ahead.
static inline ReactorTimerEventAction reactorTimerDecodeCount(uintptr_t armedTag,
                                                              uintptr_t udataBits,
                                                              uint64_t expirations,
                                                              uintptr_t *armedGeneration)
{
  if (armedTag == 0 || expirations == 0)
    return rteIgnore;
  *armedGeneration = armedTag >> 1;
  return (udataBits & udataUserEvent) ? rteUserEvent : rteExpireOperation;
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
