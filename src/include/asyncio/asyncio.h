#ifndef __ASYNCIO_ASYNCIO_H_
#define __ASYNCIO_ASYNCIO_H_

#include "asyncio/asyncioTypes.h"
#include <stddef.h>
#include <stdint.h>

// Every timeout silently saturates here (~142 years). The tightest backend range is the kqueue microsecond payload converted by the kernel to
// signed 64-bit nanoseconds; 2^52 us * 1000 stays below INT64_MAX with a 2x margin.
#define MAX_TIMEOUT_US (1ULL << 52)

typedef enum AsyncMethod {
  amOSDefault = 0,
  amEPoll,
  amKQueue,
  amIOCP,
} AsyncMethod;

typedef enum AsyncInitFlags {
  aiNone = 0,
  // Set the SIGPIPE disposition to ignored process-wide (POSIX; no-op on Windows). This is opt-in because signal disposition is application
  // policy. Without the flag the library still protects its own descriptors.
  aiIgnoreSigpipe = 1
} AsyncInitFlags;

typedef enum AsyncOpStatus {
  aosUnknown = -1,
  aosSuccess = 0,
  aosPending,
  aosTimeout,
  // The established stream ended (orderly EOF or reset), a connection attempt was refused, or a connected datagram received an asynchronous
  // peer rejection.
  aosDisconnected,
  // The operation requires a usable connected socket, but none exists for it: EPIPE, ENOTCONN or their Winsock equivalents.
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

#ifdef __cplusplus
static inline AsyncFlags operator|(AsyncFlags a, AsyncFlags b)
{
  return static_cast<AsyncFlags>(static_cast<int>(a) | static_cast<int>(b));
}
#endif

typedef struct asyncBase asyncBase;
typedef struct aioObjectRoot aioObjectRoot;
typedef struct asyncOpRoot asyncOpRoot;
typedef struct aioObject aioObject;
typedef struct aioUserEvent aioUserEvent;

typedef void aioObjectDestructorCb(aioObjectRoot*, void*);
typedef void userEventDestructorCb(aioUserEvent*, void*);
typedef void aioEventCb(aioUserEvent*, void*);
typedef void aioConnectCb(AsyncOpStatus, aioObject*, void*);
typedef void aioAcceptCb(AsyncOpStatus, aioObject*, HostAddress, socketTy, void*);
typedef void aioCb(AsyncOpStatus, aioObject*, size_t, void*);
typedef void aioReadMsgCb(AsyncOpStatus, aioObject*, HostAddress, size_t, void*);

#ifdef __cplusplus
extern "C" {
#endif

socketTy aioObjectSocket(aioObject *object);
iodevTy aioObjectDevice(aioObject *object);
aioObjectRoot *aioObjectHandle(aioObject *object);

// Public intrusive ownership hooks. Retain/release are thread-safe; a caller may retain only through a reference it already owns. Destructor
// callbacks are construction-time configuration and must be set before publication.
uintptr_t objectIncrementReference(aioObjectRoot *object, uintptr_t count);
uintptr_t objectDecrementReference(aioObjectRoot *object, uintptr_t count);
void objectSetDestructorCb(aioObjectRoot *object, aioObjectDestructorCb callback, void *arg);
void cancelIo(aioObjectRoot *object);

// Library-wide one-time initialization; must be called before any other API. Returns 0 on success and a platform error code on failure.
int initializeAsyncIo(AsyncInitFlags flags);

// loopThreads is the maximum number of threads that will run asyncLoop() on this base concurrently (0 is treated as 1). It sizes the
// timer-wakeup slot array, one cache line per thread. An invocation beyond the declared limit is rejected before entering the backend wait.
// Returns 0 on allocation or OS multiplexor creation failure.
asyncBase *createAsyncBase(AsyncMethod method, unsigned loopThreads);

// On failure the descriptor remains owned by the caller. On success the object owns it until deleteAioObject.
aioObject *newSocketIo(asyncBase *base, socketTy hSocket);
aioObject *newDeviceIo(asyncBase *base, iodevTy hDevice);
void deleteAioObject(aioObject *object);
asyncBase *aioGetBase(aioObject *object);

// Grows the object's internal read-ahead buffer (this is not SO_RCVBUF). Calls for the same object must be externally serialized and must not
// overlap any executing or pending read: an IOCP read may retain the old buffer address until its completion is delivered.
void setSocketBuffer(aioObject *socket, size_t bufferSize);

// A non-NULL user-event callback runs asynchronously. Manual activations and timer activations on reactor backends run on an asyncLoop(base)
// thread. On IOCP, timer activations run directly on a Windows thread-pool worker and may be delivered before asyncLoop(base) is ever entered.
// No callback thread affinity is guaranteed across activation sources. With isSemaphore == 0, activations coalesce while one delivery is
// pending; with isSemaphore != 0, activations are counted independently while the event remains open. deleteUserEvent may discard activations
// not yet accepted for callback delivery. The pending gate is released before the callback, so callbacks of the same event may overlap. The
// caller receives one initial strong reference. Exactly one holder closes the event with deleteUserEvent instead of ordinary release.
aioUserEvent *newUserEvent(asyncBase *base, int isSemaphore, aioEventCb callback, void *arg);

// External callers pass ordinary positive reference counts and may retain only while already owning a reference. The destructor callback runs
// exactly once on final release, after the sole deleteUserEvent and all claimed work.
void eventIncrementReference(aioUserEvent *event, uintptr_t count);
void eventDecrementReference(aioUserEvent *event, uintptr_t count);
void eventSetDestructorCb(aioUserEvent *event, userEventDestructorCb callback, void *arg);

// Starts a periodic timer and replaces the previous schedule. usTimeout must be nonzero and saturates at MAX_TIMEOUT_US. counter > 0 limits
// deliveries; counter <= 0 repeats until stopped. Start/Stop calls for the same event must be externally serialized.
void userEventStartTimer(aioUserEvent *event, uint64_t usTimeout, int counter);

// Does not cancel an already accepted callback delivery. Subject to the external-serialization rule above, Stop is idempotent.
void userEventStopTimer(aioUserEvent *event);

// Thread-safe cross-thread activation; see newUserEvent for coalescing rules.
void userEventActivate(aioUserEvent *event);

// May be called exactly once from any thread, including the event callback. It publishes the terminal state, stops the timer and consumes one
// strong reference. Other strong references keep storage alive but cannot reopen the event. Callback batches already accepted before close are
// not retracted.
void deleteUserEvent(aioUserEvent *event);

asyncOpRoot *implRead(aioObject *object,
                      void *buffer,
                      size_t size,
                      AsyncFlags flags,
                      uint64_t usTimeout,
                      aioCb callback,
                      void *arg,
                      size_t *bytesTransferred);
asyncOpRoot *implWrite(aioObject *object,
                       const void *buffer,
                       size_t size,
                       AsyncFlags flags,
                       uint64_t usTimeout,
                       aioCb callback,
                       void *arg,
                       size_t *bytesTransferred);
void implReadModify(asyncOpRoot *op, void *buffer, size_t size);

void aioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout, aioConnectCb callback, void *arg);

// The callback is mandatory: it is the only channel that can receive the accepted socket.
void aioAccept(aioObject *object, uint64_t usTimeout, aioAcceptCb callback, void *arg);

// Byte-stream I/O for devices and stream-oriented sockets. Message-oriented sockets must use the Msg variants to preserve datagram boundaries
// and truncation. Msg variants reject sizes above INT_MAX with aosUnknownError on every platform because native datagram APIs use narrower
// length types.
ssize_t aioRead(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout, aioCb callback, void *arg);
ssize_t aioReadMsg(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout, aioReadMsgCb callback, void *arg);
ssize_t aioWrite(aioObject *object, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout, aioCb callback, void *arg);
ssize_t aioWriteMsg(aioObject *object,
                    const HostAddress *address,
                    const void *buffer,
                    size_t size,
                    AsyncFlags flags,
                    uint64_t usTimeout,
                    aioCb callback,
                    void *arg);

int ioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout);

// Returns 0 on success or -AsyncOpStatus on failure. acceptedSocket must not be NULL and is set to INVALID_SOCKET on failure; remoteAddress may
// be NULL.
int ioAccept(aioObject *object, socketTy *acceptedSocket, HostAddress *remoteAddress, uint64_t usTimeout);
ssize_t ioRead(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioReadMsg(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioWrite(aioObject *object, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioWriteMsg(aioObject *object, const HostAddress *address, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);

// Coroutine-only helpers for a dedicated event created with callback == NULL. Calls on that event are serialized by the coroutine; only
// activation and the single delete may overlap from other threads. The caller owns a strong reference for the complete call, including
// suspension. A coroutine may resume on another OS thread; in particular, an IOCP timer resumes it directly on a Windows thread-pool worker.
// Concurrent deletion wakes a parked helper asynchronously through the event's normal manual-delivery path.
void ioSleep(aioUserEvent *event, uint64_t usTimeout);
void ioWaitUserEvent(aioUserEvent *event);

void asyncLoop(asyncBase *base);

// Sticky level-triggered stop: every current and future asyncLoop invocation on the base drains already-queued callbacks and returns. Pending
// operations, armed timers and queued callbacks survive until resetQuitOperation.
void postQuitOperation(asyncBase *base);

// Rearms the base. Not thread-safe: every asyncLoop must have returned and no postQuitOperation/asyncLoop/reset may run concurrently.
void resetQuitOperation(asyncBase *base);

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_ASYNCIO_H_
