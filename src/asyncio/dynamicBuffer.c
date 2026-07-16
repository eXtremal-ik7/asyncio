#include "asyncio/dynamicBuffer.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>


static void dynamicBufferGrow(dynamicBuffer *buffer, size_t extra)
{
  size_t newMemorySize = buffer->allocatedSize;
  while (newMemorySize < buffer->offset + extra)
    newMemorySize *= 2;

  if (newMemorySize != buffer->allocatedSize) {
    buffer->data = buffer->data ? realloc(buffer->data, newMemorySize) : malloc(newMemorySize);
    buffer->allocatedSize = newMemorySize;
  }
}


void dynamicBufferInit(dynamicBuffer *buffer, size_t initialSize)
{
  if (initialSize)
    buffer->data = malloc(initialSize);
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
