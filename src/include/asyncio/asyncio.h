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

aioUserEvent *newUserEvent(asyncBase* base, int isSemaphore, aioEventCb callback, void* arg);
void userEventStartTimer(aioUserEvent *event, uint64_t usTimeout, int counter);
void userEventStopTimer(aioUserEvent *event);
void userEventActivate(aioUserEvent *event);
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
socketTy ioAccept(aioObject *object, uint64_t usTimeout);
ssize_t ioRead(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioReadMsg(aioObject *object, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioWrite(aioObject *object, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioWriteMsg(aioObject *object, const HostAddress *address, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
void ioSleep(aioUserEvent *event, uint64_t usTimeout);
void ioWaitUserEvent(aioUserEvent *event);

void asyncLoop(asyncBase *base);
void postQuitOperation(asyncBase *base);

#ifdef __cplusplus
}
#endif
