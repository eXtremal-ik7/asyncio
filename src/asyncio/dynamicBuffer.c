#include "asyncio/dynamicBuffer.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


static void dynamicBufferGrow(dynamicBuffer *buffer, size_t extra)
{
  // The doubling needs a non-zero seed (a zero-sized buffer allocates lazily)
  // and saturates instead of wrapping when it cannot double up to the request.
  size_t required = (extra <= SIZE_MAX - buffer->offset) ? buffer->offset + extra : SIZE_MAX;
  size_t newMemorySize = buffer->allocatedSize ? buffer->allocatedSize : 64;
  while (newMemorySize < required)
    newMemorySize = (newMemorySize <= SIZE_MAX / 2) ? newMemorySize * 2 : required;

  if (newMemorySize != buffer->allocatedSize) {
    buffer->data = buffer->data ? realloc(buffer->data, newMemorySize) : malloc(newMemorySize);
    buffer->allocatedSize = newMemorySize;
  }
}


void dynamicBufferInit(dynamicBuffer *buffer, size_t initialSize)
{
  buffer->data = initialSize ? malloc(initialSize) : NULL;
  buffer->offset = 0;
  buffer->size = 0;
  buffer->allocatedSize = initialSize;
}

void dynamicBufferFree(dynamicBuffer *buffer)
{
  free(buffer->data);
}


void *dynamicBufferAlloc(dynamicBuffer *buffer, size_t size)
{
  void *ptr;

  dynamicBufferGrow(buffer, size);
  ptr = dynamicBufferPtr(buffer);
  buffer->offset += size;
  if (buffer->offset > buffer->size)
    buffer->size = buffer->offset;
    
  return ptr;
}


void dynamicBufferClear(dynamicBuffer *buffer)
{
  buffer->offset = 0;
  buffer->size = 0;
}


void *dynamicBufferPtr(dynamicBuffer *buffer)
{
  return (uint8_t*)buffer->data + buffer->offset;  
}


void dynamicBufferWrite(dynamicBuffer *buffer, const void *data, size_t size)
{
  memcpy(dynamicBufferAlloc(buffer, size), data, size);
}
