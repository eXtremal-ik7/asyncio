#ifndef __ASYNCIO_DYNAMICBUFFER_H_
#define __ASYNCIO_DYNAMICBUFFER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/asyncioTypes.h"


typedef struct dynamicBuffer {
  void *data;
  size_t size;
  size_t allocatedSize;
  size_t offset;
} dynamicBuffer;

void dynamicBufferInit(dynamicBuffer *buffer, size_t initialSize);
void dynamicBufferFree(dynamicBuffer *buffer);
void *dynamicBufferAlloc(dynamicBuffer *buffer, size_t size);
void dynamicBufferClear(dynamicBuffer *buffer);
void *dynamicBufferPtr(dynamicBuffer *buffer);
void dynamicBufferWrite(dynamicBuffer *buffer, const void *data, size_t size);


#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_DYNAMICBUFFER_H_
