#ifndef __DEVICE_H_
#define __DEVICE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/asyncioTypes.h"

// Returns INVALID_DEVICE on failure.
iodevTy serialPortOpen(const char *name);

struct pipeTy {
  iodevTy read;
  iodevTy write;
};

void serialPortClose(iodevTy port);

int serialPortSetConfig(iodevTy port,
                        int speed,
                        int dataBits,
                        int stopBits,
                        int parity);

void serialPortFlush(iodevTy port);

int pipeCreate(struct pipeTy *pipePtr, int isAsync);
void pipeClose(struct pipeTy pipePtr);

// Best-effort fast path for non-blocking/overlapped devices: 1 = the request
// completed here, 0 = it must continue on the async path. In either case
// *bytesTransferred reports progress already made (possibly partial for a
// wait-all request); errors are reported by the async attempt. POSIX tries
// both directions. Windows probes buffered input on pipes and COM ports;
// writes never complete synchronously because an overlapped handle has no
// would-block probe and a pending WriteFile owns the caller's buffer.
int deviceSyncRead(iodevTy hDevice, void *buffer, size_t size, int waitAll, size_t *bytesTransferred);
int deviceSyncWrite(iodevTy hDevice, const void *buffer, size_t size, int waitAll, size_t *bytesTransferred);

#ifdef __cplusplus
}
#endif

#endif //__DEVICE_H_
