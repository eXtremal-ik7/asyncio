#ifndef __ASYNCIO_LIFETIME_H_
#define __ASYNCIO_LIFETIME_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/asyncio.h"
#include "asyncio/ringBuffer.h"
#include "atomic.h"
#include "atomic128.h"
#include "macro.h"
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#define TAGGED_POINTER_DATA_SIZE 6
#define TAGGED_POINTER_ALIGNMENT (((intptr_t)1) << TAGGED_POINTER_DATA_SIZE)
#define TAGGED_POINTER_DATA_MASK (TAGGED_POINTER_ALIGNMENT - 1)
#define TAGGED_POINTER_PTR_MASK (~TAGGED_POINTER_DATA_MASK)
#define CACHE_LINE_SIZE 64

#ifndef __cplusplus
#define STATIC_CAST(x, y) ((x)(y))
#define REINTERPRET_CAST(x, y) ((x)(y))
#else
#define STATIC_CAST(x, y) static_cast<x>(y)
#define REINTERPRET_CAST(x, y) reinterpret_cast<x>(y)
#endif

typedef enum IoObjectTy {
  ioObjectSocket,
  ioObjectDevice,
  ioObjectTimer,
  ioObjectUserDefined
} IoObjectTy;

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

static inline void objectHeaderSetType(objectHeader *header, ObjectHeaderType type)
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

typedef AsyncOpStatus aioExecuteProc(asyncOpRoot*);
typedef int aioCancelProc(asyncOpRoot*);
typedef void aioFinishProc(asyncOpRoot*);
typedef void aioReleaseProc(asyncOpRoot*);
typedef void aioObjectDestructor(aioObjectRoot*);

void *alignedMalloc(size_t size, size_t alignment);
void alignedFree(void *ptr);

// Type-stable storage. Pooled cells retain the header needed by stale kernel
// envelopes; the remainder is poisoned between uses in ASan builds.
void *objectAlloc(ConcurrentQueue *pool, size_t size, size_t alignment);
void objectFree(ConcurrentQueue *pool, void *object, size_t size);
void *__tagged_pointer_make(void *ptr, uintptr_t data);
void __tagged_pointer_decode(void *ptr, void **outPtr, uintptr_t *outData);

#if defined(__has_include)
#if __has_include(<sanitizer/asan_interface.h>)
#include <sanitizer/asan_interface.h>
#endif
#endif
#ifndef ASAN_POISON_MEMORY_REGION
#define ASAN_POISON_MEMORY_REGION(addr, size) ((void)(addr), (void)(size))
#define ASAN_UNPOISON_MEMORY_REGION(addr, size) ((void)(addr), (void)(size))
#endif

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
  ASAN_POISON_MEMORY_REGION(object, size);
  concurrentQueuePush(pool, object);
}

void initObjectRoot(aioObjectRoot *object, asyncBase *base, IoObjectTy type, aioObjectDestructor destructor);
void objectDelete(aioObjectRoot *object);

int asyncOpAlloc(asyncBase *base,
                 size_t size,
                 int isRealTime,
                 ConcurrentQueue *objectPool,
                 ConcurrentQueue *objectTimerPool,
                 asyncOpRoot **result);
void releaseAsyncOp(asyncOpRoot *op);
void initAsyncOpRoot(asyncOpRoot *op,
                     aioExecuteProc *startMethod,
                     aioCancelProc *cancelMethod,
                     aioFinishProc *finishMethod,
                     aioReleaseProc *releaseMethod,
                     aioObjectRoot *object,
                     void *callback,
                     void *arg,
                     AsyncFlags flags,
                     int opCode,
                     uint64_t timeout);

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_LIFETIME_H_
