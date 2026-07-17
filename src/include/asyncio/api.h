#ifndef __ASYNCIO_ASYNCOP_H_
#define __ASYNCIO_ASYNCOP_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include "coroutine.h"
#include "macro.h"
#include "atomic.h"
#include "atomic128.h"
#include "ringBuffer.h"
#include "asyncio/asyncioTypes.h"


#define MAX_SYNCHRONOUS_FINISHED_OPERATION 32

typedef enum AsyncMethod {
  amOSDefault = 0,
  amEPoll,
  amKQueue,
  amIOCP,
} AsyncMethod;


typedef enum AsyncInitFlags {
  aiNone = 0,
  // Set the SIGPIPE disposition to ignored, process-wide (POSIX; no-op on
  // Windows). Opt-in because signal disposition is global policy owned by
  // the application: writes to a dead stdout pipeline stop killing the
  // process, and the ignored disposition is inherited by exec()ed children.
  // Without the flag the library still protects its own descriptors: sockets
  // per-fd (MSG_NOSIGNAL / SO_NOSIGPIPE), pipes via F_SETNOSIGPIPE where the
  // OS has it, elsewhere by masking SIGPIPE around pipe writes; the flag
  // additionally removes that per-write masking cost.
  aiIgnoreSigpipe = 1
} AsyncInitFlags;


typedef enum IoObjectTy {
  ioObjectSocket,
  ioObjectDevice,
  ioObjectTimer,
  ioObjectUserDefined
} IoObjectTy;


typedef enum AsyncOpStatus {
  aosUnknown = -1,
  aosSuccess = 0,
  aosPending,
  aosTimeout,
  aosDisconnected,
  // The socket is not, or no longer, a usable connection for this operation:
  // a write to a peer that has gone away (EPIPE) or any I/O on a socket that
  // was never connected (ENOTCONN / WSAENOTCONN). Distinct from aosDisconnected,
  // which marks an established stream ending mid-flight (orderly EOF or reset).
  aosNotConnected,
  aosCanceled,
  aosBufferTooSmall,
  aosUnknownError,
  aosLast
} AsyncOpStatus;


typedef enum AsyncFlags {
  afNone = 0,
  afWaitAll = 1,
  afNoCopy = 2,
  afRealtime = 4,
  afActiveOnce = 8,
  afRunning = 16,
  afCoroutine = 32
} AsyncFlags;

typedef enum AsyncOpRunningTy {
  arWaiting = 0,
  arRunning,
  arCancelling
} AsyncOpRunningTy;

#ifdef __cplusplus
__NO_UNUSED_FUNCTION_BEGIN
static inline AsyncFlags operator|(AsyncFlags a, AsyncFlags b) {
  return static_cast<AsyncFlags>(static_cast<int>(a) | static_cast<int>(b));
}
__NO_UNUSED_FUNCTION_END
#endif

typedef struct asyncBase asyncBase;
typedef struct aioObjectRoot aioObjectRoot;
typedef struct asyncOpRoot asyncOpRoot;
typedef struct asyncOpListLink asyncOpListLink;
typedef struct coroutineTy coroutineTy;

typedef struct aioObject aioObject;
typedef struct aioUserEvent aioUserEvent;
typedef struct asyncOp asyncOp;

// Every address which can make a kernel round-trip begins with this prefix.
// tag.low belongs to the concrete protocol (object combiner, event references
// or timer publication); tag.high is the full generation represented by the
// compact reactor envelope. type is immutable for the physical cell, so a
// stale envelope may select its validation protocol before owning a reference.
typedef enum ObjectHeaderType {
  ohtObject,
  ohtUserEvent,
  ohtTimer
} ObjectHeaderType;

typedef struct objectHeader {
  volatile uint128 tag;
  volatile unsigned type;
  union {
    IoObjectTy objectType;
    int isSemaphore;
    struct {
      uint8_t kind;
      uint8_t registered;
      uint16_t reserved;
    } timer;
  };
  asyncBase *base;
} objectHeader;

static inline ObjectHeaderType objectHeaderGetType(objectHeader *header)
{
  return (ObjectHeaderType)__uint_atomic_load(&header->type, amoRelaxed);
}

static inline void objectHeaderSetType(objectHeader *header,
                                       ObjectHeaderType type)
{
  __uint_atomic_store(&header->type, (unsigned)type, amoRelaxed);
}

static inline uint64_t objectHeaderGeneration(objectHeader *header)
{
  return __uint64_atomic_load(&header->tag.high, amoRelaxed);
}

#ifdef __cplusplus
static_assert(sizeof(objectHeader) == 32, "objectHeader must stay compact");
static_assert(offsetof(objectHeader, tag) == 0, "tag must start objectHeader");
static_assert(alignof(objectHeader) == 16, "objectHeader must be DWCAS aligned");
#elif defined(_MSC_VER) && !defined(__clang__)
typedef char objectHeaderMustBe32Bytes[sizeof(objectHeader) == 32 ? 1 : -1];
typedef char tagMustStartObjectHeader[offsetof(objectHeader, tag) == 0 ? 1 : -1];
#else
_Static_assert(sizeof(objectHeader) == 32, "objectHeader must stay compact");
_Static_assert(offsetof(objectHeader, tag) == 0, "tag must start objectHeader");
_Static_assert(_Alignof(objectHeader) == 16, "objectHeader must be DWCAS aligned");
#endif

typedef struct List {
  asyncOpRoot *head;
  asyncOpRoot *tail;
} List;

typedef asyncOpRoot *newAsyncOpTy(asyncBase*, int, ConcurrentQueue*, ConcurrentQueue*);
typedef void initializeTimerTy(asyncBase*, asyncOpRoot*);
typedef AsyncOpStatus aioExecuteProc(asyncOpRoot*);
typedef int aioCancelProc(asyncOpRoot*);
typedef void aioFinishProc(asyncOpRoot*);
typedef void aioReleaseProc(asyncOpRoot*);
typedef void aioObjectDestructor(aioObjectRoot*);
typedef void aioObjectDestructorCb(aioObjectRoot*, void*);
typedef void userEventDestructorCb(aioUserEvent*, void*);

extern __tls unsigned currentFinishedSync;
extern __tls unsigned messageLoopThreadId;

#ifndef __cplusplus
#define STATIC_CAST(x, y) ((x)(y))
#define REINTERPRET_CAST(x, y) ((x)(y))
#else
#define STATIC_CAST(x, y) static_cast<x>(y)
#define REINTERPRET_CAST(x, y) reinterpret_cast<x>(y)
#endif

#define TAG_STATUS_SIZE 8
#define TAG_STATUS_MASK ((STATIC_CAST(uintptr_t, 1) << TAG_STATUS_SIZE)-1)
#define TAG_GENERATION_MASK (~TAG_STATUS_MASK)

// Signal bits carried in the low COMBINER_TAG_SIZE bits of Head (op aligned to
// 1<<COMBINER_TAG_SIZE). READ/WRITE ask the combiner to re-check the matching
// queue, or the one-shot initialization operation while it occupies its slot.
// ERROR carries peer-close readiness from a reactor, CANCEL asks for a terminal
// scan, and DELETE destroys the object. All are idempotent (OR-ed into Head),
// so a re-pushed signal can neither be lost nor double-counted - the action is
// derived from object/operation state, not carried in the node. Progress is
// always emitted; cancel only by the status-race winner (invariant 1).
// CANCELIO is the cancelIo() request: same terminal scan as CANCEL, plus the
// position defines the CancelIoFlag sweep boundary - a distinct bit, so an
// older CANCEL position (grid timeout/opCancel) cannot consume the flag early
// and exempt operations submitted before the cancelIo() call.
#define COMBINER_TAG_SIZE           6
#define COMBINER_TAG_PROGRESS_READ  (1u << 0)
#define COMBINER_TAG_PROGRESS_WRITE (1u << 1)
#define COMBINER_TAG_ERROR          (1u << 2)
#define COMBINER_TAG_CANCEL         (1u << 3)
#define COMBINER_TAG_DELETE         (1u << 4)
#define COMBINER_TAG_CANCELIO       (1u << 5)
#define COMBINER_TAG_PROGRESS_MASK  (COMBINER_TAG_PROGRESS_READ | COMBINER_TAG_PROGRESS_WRITE)

typedef struct AsyncOpTaggedPtr {
  uintptr_t data;
} AsyncOpTaggedPtr;

// tag is the first 16 bytes of every aioObjectRoot. Kernel producers without
// a strong reference validate both words with one hardware DWCAS; ordinary
// ref-held producers touch low alone. high is a monotonic generation token and
// publishes no payload. All accesses to this mixed-size region are atomic.

#define IO_EVENT_READ     1u
#define IO_EVENT_WRITE    2u
#define IO_EVENT_ERROR    4u

// User-event lifetime/rendezvous word. The top bit is the sticky terminal
// DELETE state and the next bit records a published coroutine waiter until its
// Yield returns. WAITER_COMMITTED is synchronization state, not a reference:
// the coroutine caller must own an ordinary strong reference throughout the
// helper call. Manual/timer pending state lives outside this word.
#if defined(OS_32)
#define TAG_EVENT_DELETE            STATIC_CAST(uintptr_t, 0x80000000U)
#define TAG_EVENT_WAITER_COMMITTED  STATIC_CAST(uintptr_t, 0x40000000U)
#define TAG_EVENT_REF_MASK          STATIC_CAST(uintptr_t, 0x3FFFFFFFU)
#elif defined(OS_64)
#define TAG_EVENT_DELETE            STATIC_CAST(uintptr_t, 0x8000000000000000ULL)
#define TAG_EVENT_WAITER_COMMITTED  STATIC_CAST(uintptr_t, 0x4000000000000000ULL)
#define TAG_EVENT_REF_MASK          STATIC_CAST(uintptr_t, 0x3FFFFFFFFFFFFFFFULL)
#else
#error Configution incomplete
#endif

#define OPCODE_READ 0
#define OPCODE_WRITE 0x01000000
#define OPCODE_OTHER 0x02000000

// Public intrusive ownership hooks. Retain/release are thread-safe; a caller
// may retain only through a reference it already owns. Destructor callbacks
// are construction-time configuration and must be set before publication.
uintptr_t objectIncrementReference(aioObjectRoot *object, uintptr_t count);
uintptr_t objectDecrementReference(aioObjectRoot *object, uintptr_t count);
void objectSetDestructorCb(aioObjectRoot *object, aioObjectDestructorCb callback, void *arg);
// Strong ownership for user events. External callers pass an ordinary positive
// reference count (no TAG_EVENT_* bits), may retain only while already owning a
// reference, and must release exactly what they own. All external references
// are equivalent for lifetime. Exactly one holder calls deleteUserEvent instead
// of ordinary release; that call alone publishes DELETING and consumes one
// reference. A holder may copy an already-owned reference after delete, but
// this only extends storage lifetime and cannot reopen the event. Activation
// itself owns no internal reference: the personal kernel readiness handler
// claims one for the current generation immediately before delivery. Coroutine
// helpers likewise borrow the caller's reference across Yield rather than
// retaining one internally. Delete may therefore discard readiness which has
// not reached its claim point.
void eventIncrementReference(aioUserEvent *event, uintptr_t count);
void eventDecrementReference(aioUserEvent *event, uintptr_t count);
// Construction-time configuration: set before publishing or deleting event.
// Called exactly once by the thread releasing the final reference, after the
// sole deleteUserEvent call, all external strong references, claimed
// deliveries and any timer-control transition already in flight at deletion
// have finished.
void eventSetDestructorCb(aioUserEvent *event, userEventDestructorCb callback, void *arg);

void *alignedMalloc(size_t size, size_t alignment);
void alignedFree(void *ptr);
// Type-stable object storage. Every published type embedding aioObjectRoot
// must recycle its cells exclusively through this pair; returning such a cell
// to free()/alignedFree() is forbidden because an already-harvested kernel
// event may still validate its header. A fresh cell whose construction failed
// before any publication may still be freed. Fresh cells are zeroed and start
// at generation one; pooled cells preserve their tag identity. objectFree
// leaves tag and the immutable header type readable and ASAN-poisons the
// remainder.
void *objectAlloc(ConcurrentQueue *pool, size_t size, size_t alignment);
void objectFree(ConcurrentQueue *pool, void *object, size_t size);
void *__tagged_pointer_make(void *ptr, uintptr_t data);
void __tagged_pointer_decode(void *ptr, void **outPtr, uintptr_t *outData);

// Recycling pools. Pooled memory is never returned to the allocator, in
// every build: under the address sanitizer a parked cell is marked with the
// manual poisoning API instead, so the allocation pattern stays identical to
// production while any illegal touch between free and the next alloc reports
// use-after-poison. What stays readable mirrors each type's stale-access
// contract: an object keeps its header (tag + immutable type) - harvested
// kernel envelopes validate through it; a parked operation keeps only the
// leading tag word - timeout-grid links and realtime timer envelopes hold op
// pointers past the operation's lifetime, and their generation CAS on the
// tag is what rejects them (nothing beyond the tag may be touched until that
// CAS succeeds, see asyncOpRoot); a grid link itself has no stale readers
// and parks fully poisoned.
#if defined(__has_include)
#if __has_include(<sanitizer/asan_interface.h>)
#include <sanitizer/asan_interface.h>  // ASAN_(UN)POISON_MEMORY_REGION expand to no-ops without asan
#endif
#endif
#ifndef ASAN_POISON_MEMORY_REGION
// Toolchain without the sanitizer interface header (e.g. MSVC without the
// asan toolset): the marks compile away exactly as in a non-asan build
#define ASAN_POISON_MEMORY_REGION(addr, size) ((void)(addr), (void)(size))
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) ((void)(addr), (void)(size))
#endif

// True only when the address sanitizer is actually active: gates the small
// pool-size bookkeeping that operation marks need, so production builds
// carry no extra work on the release path
#if defined(__SANITIZE_ADDRESS__)
#define ASYNCIO_POOL_MARKS 1
#elif defined(__has_feature)
#if __has_feature(address_sanitizer)
#define ASYNCIO_POOL_MARKS 1
#endif
#endif
#ifndef ASYNCIO_POOL_MARKS
#define ASYNCIO_POOL_MARKS 0
#endif

// Leak-scanner handoff for allocations deliberately retained by parked
// pooled cells: operation scratch, object read-ahead, SSL rings, paired
// timer cells. A parked owner is ASAN-poisoned and the leak scanner does
// not read pointers out of poisoned words, so such caches would report as
// leaks. Their lifetime belongs to the pool; a genuinely lost owner still
// reports as its own leak.
#if ASYNCIO_POOL_MARKS && defined(__has_include)
#if __has_include(<sanitizer/lsan_interface.h>)
#include <sanitizer/lsan_interface.h>
#define ASYNCIO_LSAN_HANDOFF 1
#endif
#endif
#ifndef ASYNCIO_LSAN_HANDOFF
#define ASYNCIO_LSAN_HANDOFF 0
#endif

static inline void poolCacheHandoff(const void *ptr)
{
#if ASYNCIO_LSAN_HANDOFF
  if (ptr)
    __lsan_ignore_object(ptr);
#else
  (void)ptr;
#endif
}

static inline int objectPoolGet(ConcurrentQueue *pool, void **result, size_t size)
{
  if (!concurrentQueuePop(pool, result))
    return 0;
  ASAN_UNPOISON_MEMORY_REGION(*result, size);
  return 1;
}

static inline void objectPoolPut(ConcurrentQueue *pool, void *object, size_t size)
{
  // Mark before publishing: once the pointer is in the queue another thread
  // may pop and unmark it, and this mark must not land after that
  ASAN_POISON_MEMORY_REGION(object, size);
  concurrentQueuePush(pool, object);
}

void eqRemove(List *list, asyncOpRoot *op);
void eqPushBack(List *list, asyncOpRoot *op);

// Timeout-grid link. The deadline lives here, not in the operation: after
// publication the operation may complete and be recycled at any moment, and
// until a generation CAS wins nothing beyond op->tag may be read. 48 bytes,
// pooled in asyncOpLinkListPool.
typedef struct asyncOpListLink {
  asyncOpRoot *op;
  uintptr_t generation;
  aioObjectRoot *object;
  uintptr_t objectGeneration;
  asyncOpListLink *next;
  uint64_t deadlineTick;
} asyncOpListLink;

typedef struct ListImpl {
  asyncOpRoot *prev;
  asyncOpRoot *next;
} ListImpl;


struct aioObjectRoot {
  objectHeader header;
  uintptr_t refs;
  List readQueue;
  List writeQueue;

  volatile uint32_t CancelIoFlag;
  // Set by objectDelete, never cleared: the object is dying, every combiner
  // pass sweeps the queues once more before releasing ownership. A plain
  // CancelIoFlag pass is positionally sloppy - it runs at the node its tag
  // landed on, so a submission parked later in the same captured chain
  // starts after the sweep; with no timeout armed such an operation would
  // hold its object reference forever and the DELETE tag would never fire
  volatile uint32_t DeletePending;
  // Optional one-shot transport initialization (TCP connect, SSL/ZMTP
  // handshake). It must be submitted before ordinary I/O; operations submitted
  // afterwards may wait in the read/write queues until initialization ends.
  // Objects already ready for I/O (accepted/UDP sockets, devices) never use it.
  // Claimed once by CAS at submission and cleared by the object's combiner.
  volatile uintptr_t initializationOp;

  aioObjectDestructor *destructor;
  aioObjectDestructorCb *destructorCb;
  void *destructorCbArg;
};

#ifdef __cplusplus
static_assert(offsetof(aioObjectRoot, header) == 0, "header must start aioObjectRoot");
#elif defined(_MSC_VER) && !defined(__clang__)
typedef char headerMustStartRoot[offsetof(aioObjectRoot, header) == 0 ? 1 : -1];
#else
_Static_assert(offsetof(aioObjectRoot, header) == 0, "header must start aioObjectRoot");
#endif

struct asyncOpRoot {
  // Generation (upper bits) + status (lower bits). Operation storage is
  // type-stable: a stale holder (timeout grid link, realtime timer event) may
  // legitimately touch the tag of an already recycled operation, so every
  // access goes through the atomic helpers; the generation part of the status
  // CAS is what rejects such stale writers. Nothing beyond the tag may be
  // read until that CAS succeeds
  volatile uintptr_t tag;
  ConcurrentQueue *objectPool;
  aioExecuteProc *executeMethod;
  aioCancelProc *cancelMethod;
  aioFinishProc *finishMethod;
  aioReleaseProc *releaseMethod;
  ListImpl executeQueue;
  AsyncOpTaggedPtr next;
  aioObjectRoot *object;
  void *callback;
  void *arg;
  int opCode;
  AsyncFlags flags;
  void *timerId;
  union {
    uint64_t timeout;
    uint64_t endTime;
  };
  AsyncOpRunningTy running;
};

// A parked operation is ASAN-poisoned beyond its first word; stale grid and
// timer holders are entitled to exactly the tag
#ifdef __cplusplus
static_assert(offsetof(asyncOpRoot, tag) == 0, "tag must start asyncOpRoot");
#elif defined(_MSC_VER) && !defined(__clang__)
typedef char tagMustStartOpRoot[offsetof(asyncOpRoot, tag) == 0 ? 1 : -1];
#else
_Static_assert(offsetof(asyncOpRoot, tag) == 0, "tag must start asyncOpRoot");
#endif

void initObjectRoot(aioObjectRoot *object, asyncBase *base, IoObjectTy type, aioObjectDestructor destructor);

void cancelIo(aioObjectRoot *object);
void objectDelete(aioObjectRoot *object);

uintptr_t opGetGeneration(asyncOpRoot *op);
AsyncOpStatus opGetStatus(asyncOpRoot *op);
int opSetStatus(asyncOpRoot *op, uintptr_t tag, AsyncOpStatus status);
void opForceStatus(asyncOpRoot *op, AsyncOpStatus status);

void opRelease(asyncOpRoot *op, AsyncOpStatus status, List *executeList);
// Start an op-node captured by the combiner (submission): enqueue/arm or drive
// the initialization slot. The former processAction(aaStart).
void startOperation(asyncOpRoot *op, uint32_t *needStart);
// CANCEL reconcile: scan read/write queues and the initialization slot for
// terminal operations and reap them positionally (an in-flight op whose cancelMethod
// returns 0 stays on its head until its late completion moves it).
void reapObject(aioObjectRoot *object, uint32_t tag, uint32_t *needStart);
void processInitializationOp(aioObjectRoot *object, uint32_t *needStart);
void executeOperationList(List *list);
void cancelOperationList(List *list, AsyncOpStatus status);

int opCancel(asyncOpRoot *op,
             uintptr_t generation,
             AsyncOpStatus status,
             aioObjectRoot *object,
             uintptr_t objectGeneration);
void resumeParent(asyncOpRoot *op, AsyncOpStatus status);

void addToGlobalQueue(asyncOpRoot *op);
int executeGlobalQueue(asyncBase *base);

typedef asyncOpRoot *CreateAsyncOpProc(aioObjectRoot*, AsyncFlags, uint64_t, void*, void*, int, void*);
typedef asyncOpRoot *SyncImplProc(aioObjectRoot*, AsyncFlags, uint64_t, void*, void*, void*);
typedef void MakeResultProc(void*);
typedef void InitOpProc(asyncOpRoot*, void*);

int asyncOpAlloc(asyncBase *base, size_t size, int isRealTime, ConcurrentQueue *objectPool, ConcurrentQueue *objectTimerPool, asyncOpRoot **result);
void releaseAsyncOp(asyncOpRoot *op);

void initAsyncOpRoot(asyncOpRoot *op,
                     aioExecuteProc *startMethod,
                     aioCancelProc *cancelMethod,
                     aioFinishProc *finishMethod,
                     aioReleaseProc *deleteMethod,
                     aioObjectRoot *object,
                     void *callback,
                     void *arg,
                     AsyncFlags flags,
                     int opCode,
                     uint64_t timeout);



void combiner(aioObjectRoot *object, AsyncOpTaggedPtr stackTop, AsyncOpTaggedPtr forRun);

__NO_UNUSED_FUNCTION_BEGIN

static inline AsyncOpTaggedPtr taggedAsyncOpNull()
{
  AsyncOpTaggedPtr result;
  result.data = 0;
  return result;
}

static inline AsyncOpTaggedPtr taggedAsyncOpStub()
{
  AsyncOpTaggedPtr result;
  result.data = (~STATIC_CAST(uintptr_t, 0)) ^ ((STATIC_CAST(uintptr_t, 1) << COMBINER_TAG_SIZE) -1);
  return result;
}

// An op node always enters Head with clean tag bits: signal tags are OR-ed
// into Head by the counter pushes and never travel on an operation node.
static inline AsyncOpTaggedPtr taggedAsyncOpMake(asyncOpRoot *op)
{
  AsyncOpTaggedPtr result;
  result.data = REINTERPRET_CAST(uintptr_t, op);
  return result;
}

static inline void taggedAsyncOpDecode(AsyncOpTaggedPtr ptr, asyncOpRoot **op, uint32_t *tag)
{
  uintptr_t COMBINER_TAG_MASK = (STATIC_CAST(uintptr_t, 1) << COMBINER_TAG_SIZE) - 1;
  *op = (asyncOpRoot*)(ptr.data & (~COMBINER_TAG_MASK));
  *tag = STATIC_CAST(uint32_t, ptr.data & COMBINER_TAG_MASK);
}

// Producer without a strong reference. The backend has already expanded its
// external handle to a full generation; the successful DWCAS pins that exact
// tag before any tail access. Pooled storage may have every following byte
// ASAN-poisoned. Mixed 64/128 atomic access is an explicit x86-64/ARM64
// hardware contract; no plain access to tag is permitted.
static inline int combinerPushValidated(aioObjectRoot *object, uint64_t generation, uint32_t tag)
{
  assert((tag & COMBINER_TAG_DELETE) == 0 && "DELETE must use the single-writer combinerPushDelete path");
  uint128 expected = {__uint64_atomic_load(&object->header.tag.low, amoRelaxed), generation};
  for (;;) {
    if (expected.high != generation)
      return 0;
    uint128 desired = { expected.low | tag, expected.high };
    if (__uint128_atomic_compare_and_swap(&object->header.tag, &expected, desired))
      break;
  }
  if (expected.low == 0)
    combiner(object, taggedAsyncOpStub(), taggedAsyncOpNull());
  return 1;
}

static inline asyncOpRoot *combinerAcquire(aioObjectRoot *object,
                                           List *queue,
                                           CreateAsyncOpProc *newAsyncOp,
                                           AsyncFlags flags,
                                           uint64_t usTimeout,
                                           void *callback,
                                           void *arg,
                                           int opCode,
                                           void *contextPtr) {
  AsyncOpTaggedPtr head;
  AsyncOpTaggedPtr opTagged = taggedAsyncOpStub();
  AsyncOpTaggedPtr allocatedTagged;
  asyncOpRoot *allocated = 0;

  do {
    // Speculative expected only; the successful ownership CAS below is the
    // acquire edge before any captured node can be dereferenced.
    head.data = __uint64_atomic_load(&object->header.tag.low, amoRelaxed);
    if (head.data) {
      if (!allocated) {
        allocated = newAsyncOp(object, flags, usTimeout, callback, arg, opCode, contextPtr);
        allocatedTagged = taggedAsyncOpMake(allocated);
      }

      allocated->next = head;
      opTagged = allocatedTagged;
    } else {
      opTagged = taggedAsyncOpStub();
    }
  } while (!__uint64_atomic_compare_and_swap(&object->header.tag.low,
                                              head.data,
                                              opTagged.data,
                                              amoSeqCst));

  if (!head.data) {
    // This thread entered a combiner
    if (queue->head ||
        __uintptr_atomic_load(&object->initializationOp, amoRelaxed) ||
        __uint_atomic_load(&object->DeletePending, amoRelaxed)) {
      // Object has operations in queue, initialization in flight, or is
      // closing. A closing object must not reach syncImpl: route the
      // operation through the combiner, where the sticky delete sweep at
      // the ownership-release point cancels it - the sync path after
      // objectDelete would otherwise keep succeeding forever and an
      // incoming flood could block teardown
      // Put operation to queue end and try exit combiner
      if (!allocated) {
        allocated = newAsyncOp(object, flags, usTimeout, callback, arg, opCode, contextPtr);
        allocatedTagged = taggedAsyncOpMake(allocated);
      }

      combiner(object, taggedAsyncOpStub(), allocatedTagged);
      return allocated;
    } else {
      if (allocated)
        releaseAsyncOp(allocated);
      return 0;
    }
  } else {
    return allocated;
  }
}

// Start push: the single op-node push over an operation's lifetime (submission).
// The node carries no action - "start" is implied by being an op-node; movement
// and cancellation are signals (combinerPushCounter/Move) that never touch the
// node, so op->next is written by exactly one thread and the Treiber-stack
// double-push is impossible by construction.
static inline void combinerPushOperation(asyncOpRoot *op)
{
  aioObjectRoot *object = op->object;
  AsyncOpTaggedPtr opTagged = taggedAsyncOpMake(op);
  AsyncOpTaggedPtr newOp;
  AsyncOpTaggedPtr head;
  do {
    // The value is only linked and compared until the successful CAS acquires
    // its publisher; no node is dereferenced on this speculative load.
    head.data = __uint64_atomic_load(&object->header.tag.low, amoRelaxed);
    if (head.data) {
      newOp = opTagged;
      op->next = head;
    } else {
      newOp = taggedAsyncOpStub();
    }
  } while (!__uint64_atomic_compare_and_swap(&object->header.tag.low,
                                              head.data,
                                              newOp.data,
                                              amoSeqCst));

  if (!head.data)
    combiner(object, taggedAsyncOpStub(), opTagged);
}

// Signal push: OR an idempotent tag into Head. This is a ref-held path, so its
// progress guarantee is independent of reclamation; a value-returning fetch-or
// may compile to a CAS loop on x86. Ownership is the 0->nonzero transition,
// exactly as for the op push, so at most one thread enters the combiner. When
// Head was 0 this thread owns it and drains the bit itself (stackTop = stub, no
// forRun - the bit sits in Head and is caught by the drain). A re-pushed bit can
// never be lost: the release target is the stub (clean low bits), so any OR
// moves Head off it and defeats the release CAS.
static inline void combinerPushCounter(aioObjectRoot *object, uint32_t tag) {
  if (__uint64_atomic_fetch_or(&object->header.tag.low, tag, amoSeqCst) == 0)
    combiner(object, taggedAsyncOpStub(), taggedAsyncOpNull());
}

// The final reference publishes DELETE exactly once per incarnation. With
// that invariant addition is equivalent to OR, while xadd/ldadd returns the
// old ownership word in one wait-free instruction instead of a fetch-or CAS
// loop on targets without a value-returning memory OR instruction.
static inline void combinerPushDelete(aioObjectRoot *object) {
  uintptr_t old = __uint64_atomic_fetch_and_add(&object->header.tag.low,
                                                 COMBINER_TAG_DELETE,
                                                 amoSeqCst);
  assert((old & COMBINER_TAG_DELETE) == 0 &&
         "DELETE published more than once for one object incarnation");
  if (old == 0)
    combiner(object, taggedAsyncOpStub(), taggedAsyncOpNull());
}

// Progress signal for one operation: re-check its positional direction.
// While initializationOp is occupied, its one-shot contract guarantees that
// no ordinary queue operation is running, so the backend routes either progress
// bit to initialization without a dedicated tag. This also keeps the producer
// independent of mutable object state. Proactor completions and resumeParent
// always emit progress; the combiner reconciles finish/continue/release from the
// operation status.
static inline void combinerPushProgress(asyncOpRoot *op)
{
  uint32_t bit = (op->opCode & OPCODE_WRITE) ? COMBINER_TAG_PROGRESS_WRITE : COMBINER_TAG_PROGRESS_READ;
  combinerPushCounter(op->object, bit);
}

static inline void runAioOperation(aioObjectRoot *object,
                                   CreateAsyncOpProc *createAsyncOp,
                                   SyncImplProc *syncImpl,
                                   MakeResultProc *makeResult,
                                   InitOpProc *initOp,
                                   AsyncFlags flags,
                                   uint64_t usTimeout,
                                   void *callback,
                                   void *arg,
                                   int opCode,
                                   void *contextPtr)
{
  if (!combinerAcquire(object, !(opCode & OPCODE_WRITE) ? &object->readQueue : &object->writeQueue, createAsyncOp, flags, usTimeout, callback, arg, opCode, contextPtr)) {
    // Object locked by current operation
    AsyncOpTaggedPtr forRun = taggedAsyncOpNull();
    asyncOpRoot *op = syncImpl(object, flags, usTimeout, callback, arg, contextPtr);
    if (!op) {
      // Fire-and-forget has no completion channel: the inline return value is
      // the only way the caller ever learns the result, so it must not depend
      // on the budget. The budget only pushes afActiveOnce callers back to
      // callback delivery and is not consumed by operations that cannot
      // return inline
      if (callback == 0 || ((flags & afActiveOnce) && currentFinishedSync++ < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
        makeResult(contextPtr);
      } else {
        // Budget exhausted: this completion goes through the loop, the inline
        // window restarts - without the reset a thread that never drains the
        // queues would lose the fast path forever after the first 32 calls.
        // The transfer itself already happened: the op only carries the
        // completion, so the write-payload capture copy is skipped (afNoCopy)
        if (flags & afActiveOnce)
          currentFinishedSync = 0;
        asyncOpRoot *op = createAsyncOp(object, flags | afNoCopy, usTimeout, callback, arg, opCode, contextPtr);
        initOp(op, contextPtr);
        opForceStatus(op, aosSuccess);
        addToGlobalQueue(op);
      }
    } else if (opGetStatus(op) != aosPending) {
      // Operation finished already
      addToGlobalQueue(op);
    } else {
      forRun = taggedAsyncOpMake(op);
    }

    combiner(object, taggedAsyncOpStub(), forRun);
  }
}

static inline asyncOpRoot *runIoOperation(aioObjectRoot *object,
                                          CreateAsyncOpProc *createAsyncOp,
                                          SyncImplProc *syncImpl,
                                          InitOpProc *initOp,
                                          AsyncFlags flags,
                                          uint64_t usTimeout,
                                          int opCode,
                                          void *contextPtr)
{
  assert(!coroutineIsMain() && "Trying to run 'io' operation from main coroutine");
  asyncOpRoot *op = combinerAcquire(object, !(opCode & OPCODE_WRITE) ? &object->readQueue : &object->writeQueue, createAsyncOp, flags | afCoroutine, usTimeout, 0, 0, opCode, contextPtr);
  if (!op) {
    // Object locked by current operation
    AsyncOpTaggedPtr forRun = taggedAsyncOpNull();
    op = syncImpl(object, flags | afCoroutine, usTimeout, 0, 0, contextPtr);
    if (!op) {
      if (!(++currentFinishedSync < MAX_SYNCHRONOUS_FINISHED_OPERATION)) {
        // The fairness fallback re-queues the completed sync result; the
        // operation must keep the caller's flags (afRealtime, afWaitAll, ...)
        // and the inline window restarts, same as in runAioOperation.
        // The transfer itself already happened, so the write-payload capture
        // copy is skipped (afNoCopy)
        currentFinishedSync = 0;
        op = createAsyncOp(object, flags | afCoroutine | afNoCopy, usTimeout, 0, 0, opCode, contextPtr);
        initOp(op, contextPtr);
        opForceStatus(op, aosSuccess);
        addToGlobalQueue(op);
      }
    } else if (opGetStatus(op) != aosPending) {
      // Operation finished already
      addToGlobalQueue(op);
    } else {
      forRun = taggedAsyncOpMake(op);
    }

    combiner(object, taggedAsyncOpStub(), forRun);
  }

  if (op)
    coroutineYield();
  return op;
}

__NO_UNUSED_FUNCTION_END

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_ASYNCOP_H_
