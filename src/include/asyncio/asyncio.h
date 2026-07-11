#include "asyncio/api.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void aioEventCb(aioUserEvent*, void*);
typedef void aioConnectCb(AsyncOpStatus, aioObject*, void*);
typedef void aioAcceptCb(AsyncOpStatus, aioObject*, HostAddress, socketTy, void*);
typedef void aioCb(AsyncOpStatus, aioObject*, size_t, void*);
typedef void aioReadMsgCb(AsyncOpStatus, aioObject*, HostAddress, size_t, void*);
  
socketTy aioObjectSocket(aioObject *object);
iodevTy aioObjectDevice(aioObject *object);
aioObjectRoot *aioObjectHandle(aioObject *object);

// Library-wide one-time initialization; must be called before any other API.
// Replaces initializeSocketSubsystem().
void initializeAsyncIo(AsyncInitFlags flags);

// loopThreads is the maximum number of threads that will run asyncLoop() on
// this base concurrently (0 is treated as 1). It sizes the grace-period slot
// array - one cache line per thread. Exceeding it is memory-safe but disables
// reclamation of dead objects for the rest of the base's life, so err on the
// high side when the count is not exact.
// Returns 0 when the OS multiplexor cannot be created (descriptor or handle
// exhaustion) or memory allocation fails.
asyncBase *createAsyncBase(AsyncMethod method, unsigned loopThreads);
aioObject *newSocketIo(asyncBase *base, socketTy hSocket);
aioObject *newDeviceIo(asyncBase *base, iodevTy hDevice);
void deleteAioObject(aioObject *object);
asyncBase *aioGetBase(aioObject *object);

void setSocketBuffer(aioObject *socket, size_t bufferSize);

// A non-NULL user-event callback runs asynchronously on an asyncLoop(base)
// thread. With isSemaphore == 0, activations coalesce while one delivery is
// pending; with isSemaphore != 0, every userEventActivate call produces a
// delivery. The pending gate is released before the callback, so callbacks of
// the same event may overlap when several threads run asyncLoop(base).
aioUserEvent *newUserEvent(asyncBase* base, int isSemaphore, aioEventCb callback, void* arg);

// Starts a periodic timer and atomically replaces the previous timer schedule.
// usTimeout > 0 is the period; counter > 0 limits the number of delivered timer
// activations, while counter <= 0 repeats until stopped. A delivery already
// accepted from the previous schedule is not retracted.
void userEventStartTimer(aioUserEvent *event, uint64_t usTimeout, int counter);

// Stops the current timer schedule. It does not cancel manual or already
// accepted activations. Concurrent start/stop calls take effect in their
// linearized order, so the last one wins.
void userEventStopTimer(aioUserEvent *event);

// Thread-safe cross-thread activation; see newUserEvent for coalescing rules.
void userEventActivate(aioUserEvent *event);

// May be called exactly once, from any thread (including the event callback).
// It stops the timer and releases the event after accepted deliveries finish;
// no new call using event may start after deletion.
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

void aioConnect(aioObject *object,
                const HostAddress *address,
                uint64_t usTimeout,
                aioConnectCb callback,
                void *arg);

void aioAccept(aioObject *object,
               uint64_t usTimeout,
               aioAcceptCb callback,
               void *arg);

ssize_t aioRead(aioObject *object,
                void *buffer,
                size_t size,
                AsyncFlags flags,
                uint64_t usTimeout,
                aioCb callback,
                void *arg);

ssize_t aioReadMsg(aioObject *object,
                   void *buffer,
                   size_t size,
                   AsyncFlags flags,
                   uint64_t usTimeout,
                   aioReadMsgCb callback,
                   void *arg);

ssize_t aioWrite(aioObject *object,
                 const void *buffer,
                 size_t size,
                 AsyncFlags flags,
                 uint64_t usTimeout,
                 aioCb callback,
                 void *arg);

ssize_t aioWriteMsg(aioObject *object,
                    const HostAddress *address,
                    const void *buffer,
                    size_t size,
                    AsyncFlags flags,
                    uint64_t usTimeout,
                    aioCb callback,
                    void *arg);


int ioConnect(aioObject *object, const HostAddress *address, uint64_t usTimeout);
// Returns 0 on success or -AsyncOpStatus on failure. acceptedSocket must not
// be NULL and is set to INVALID_SOCKET on failure; remoteAddress may be NULL.
int ioAccept(aioObject *object,
             socketTy *acceptedSocket,
             HostAddress *remoteAddress,
             uint64_t usTimeout);
ssize_t ioRead(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioReadMsg(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioWrite(aioObject *object, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioWriteMsg(aioObject *object, const HostAddress *address, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
// Coroutine-only helpers for a dedicated event created with callback == NULL.
// Only one coroutine may wait on the event at a time; activation may still
// come from any thread. A pending activation is consumed without suspending.
void ioSleep(aioUserEvent *event, uint64_t usTimeout);
void ioWaitUserEvent(aioUserEvent *event);

void asyncLoop(asyncBase *base);
void postQuitOperation(asyncBase *base);

#ifdef __cplusplus
}
#endif
