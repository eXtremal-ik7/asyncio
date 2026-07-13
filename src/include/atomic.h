#ifndef __ATOMIC_H_
#define __ATOMIC_H_

#include "asyncio/asyncioTypes.h"
#include "macro.h"

typedef enum AtomicMemoryOrder {
#ifndef _MSC_VER // Not Microsoft compiler
  amoRelaxed = __ATOMIC_RELAXED,
  amoAcquire = __ATOMIC_ACQUIRE,
  amoRelease = __ATOMIC_RELEASE,
  amoSeqCst = __ATOMIC_SEQ_CST
#else
  // Values match GCC/Clang __ATOMIC_* constants
  amoRelaxed = 0,
  amoAcquire = 2,
  amoRelease = 3,
  amoSeqCst = 5
#endif
} AtomicMemoryOrder;

__NO_UNUSED_FUNCTION_BEGIN
static inline unsigned __uint_atomic_fetch_and_add(unsigned volatile *ptr,
                                                    unsigned value,
                                                    AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_fetch_add(ptr, value, order);
#else
  (void)order;
  return (unsigned)InterlockedExchangeAdd((volatile LONG*)ptr, (LONG)value);
#endif
}

static inline int __uint_atomic_compare_and_swap(unsigned volatile *ptr,
                                                  unsigned expected,
                                                  unsigned desired,
                                                  AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_compare_exchange_n(ptr,
                                     &expected,
                                     desired,
                                     0,
                                     order,
                                     order == amoRelease ? amoRelaxed : order);
#else
  (void)order;
  return InterlockedCompareExchange((volatile LONG*)ptr, (LONG)desired, (LONG)expected) == (LONG)expected;
#endif
}

static inline unsigned __uint_atomic_exchange(unsigned volatile *ptr,
                                               unsigned value,
                                               AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_exchange_n(ptr, value, order);
#else
  (void)order;
  return (unsigned)InterlockedExchange((volatile LONG*)ptr, (LONG)value);
#endif
}

static inline unsigned __uint_atomic_fetch_or(unsigned volatile *ptr,
                                               unsigned value,
                                               AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_fetch_or(ptr, value, order);
#else
  (void)order;
  return (unsigned)InterlockedOr((volatile LONG*)ptr, (LONG)value);
#endif
}

static inline unsigned __uint_atomic_fetch_and(unsigned volatile *ptr,
                                                unsigned value,
                                                AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_fetch_and(ptr, value, order);
#else
  (void)order;
  return (unsigned)InterlockedAnd((volatile LONG*)ptr, (LONG)value);
#endif
}

static inline unsigned __uint_atomic_load(unsigned volatile *ptr, AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_load_n(ptr, order);
#else
  return order == amoRelaxed ? (unsigned)ReadNoFence((volatile LONG*)ptr) : (unsigned)ReadAcquire((volatile LONG*)ptr);
#endif
}

static inline void __uint_atomic_store(unsigned volatile *ptr, unsigned value, AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  __atomic_store_n(ptr, value, order);
#else
  if (order == amoRelaxed)
    WriteNoFence((volatile LONG*)ptr, (LONG)value);
  else if (order == amoRelease)
    WriteRelease((volatile LONG*)ptr, (LONG)value);
  else
    InterlockedExchange((volatile LONG*)ptr, (LONG)value);
#endif
}

static inline uintptr_t __uintptr_atomic_fetch_and_add(uintptr_t volatile *ptr,
                                                        uintptr_t value,
                                                        AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_fetch_add(ptr, value, order);
#else
  (void)order;
#ifdef OS_32
  return (uintptr_t)InterlockedExchangeAdd((volatile LONG*)ptr, (LONG)value);
#else
  return (uintptr_t)InterlockedExchangeAdd64((volatile LONG64*)ptr, (LONG64)value);
#endif
#endif
}

static inline uintptr_t __uintptr_atomic_fetch_or(uintptr_t volatile *ptr,
                                                   uintptr_t value,
                                                   AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_fetch_or(ptr, value, order);
#else
  (void)order;
#ifdef OS_32
  return (uintptr_t)InterlockedOr((volatile LONG*)ptr, (LONG)value);
#else
  return (uintptr_t)InterlockedOr64((volatile LONG64*)ptr, (LONG64)value);
#endif
#endif
}

static inline uintptr_t __uintptr_atomic_fetch_and(uintptr_t volatile *ptr,
                                                    uintptr_t value,
                                                    AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_fetch_and(ptr, value, order);
#else
  (void)order;
#ifdef OS_32
  return (uintptr_t)InterlockedAnd((volatile LONG*)ptr, (LONG)value);
#else
  return (uintptr_t)InterlockedAnd64((volatile LONG64*)ptr, (LONG64)value);
#endif
#endif
}

static inline int __uintptr_atomic_compare_and_swap(uintptr_t volatile *ptr,
                                                     uintptr_t expected,
                                                     uintptr_t desired,
                                                     AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_compare_exchange_n(ptr,
                                     &expected,
                                     desired,
                                     0,
                                     order,
                                     order == amoRelease ? amoRelaxed : order);
#else
  (void)order;
#ifdef OS_32
  return InterlockedCompareExchange((volatile LONG*)ptr, (LONG)desired, (LONG)expected) == (LONG)expected;
#else
  return InterlockedCompareExchange64((volatile LONG64*)ptr, (LONG64)desired, (LONG64)expected) == (LONG64)expected;
#endif
#endif
}

static inline int __uintptr_atomic_compare_exchange(uintptr_t volatile *ptr,
                                                     uintptr_t *expected,
                                                     uintptr_t desired,
                                                     AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_compare_exchange_n(ptr,
                                     expected,
                                     desired,
                                     0,
                                     order,
                                     order == amoRelease ? amoRelaxed : order);
#else
  (void)order;
  uintptr_t wanted = *expected;
#ifdef OS_32
  uintptr_t observed = (uintptr_t)InterlockedCompareExchange((volatile LONG*)ptr,
                                                              (LONG)desired,
                                                              (LONG)wanted);
#else
  uintptr_t observed = (uintptr_t)InterlockedCompareExchange64((volatile LONG64*)ptr,
                                                                (LONG64)desired,
                                                                (LONG64)wanted);
#endif
  if (observed == wanted)
    return 1;
  *expected = observed;
  return 0;
#endif
}

static inline uintptr_t __uintptr_atomic_load(uintptr_t volatile *ptr, AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_load_n(ptr, order);
#else
#ifdef OS_32
  return order == amoRelaxed ? (uintptr_t)ReadNoFence((volatile LONG*)ptr) : (uintptr_t)ReadAcquire((volatile LONG*)ptr);
#else
  return order == amoRelaxed ? (uintptr_t)ReadNoFence64((volatile LONG64*)ptr) : (uintptr_t)ReadAcquire64((volatile LONG64*)ptr);
#endif
#endif
}

static inline void __uintptr_atomic_store(uintptr_t volatile *ptr, uintptr_t value, AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  __atomic_store_n(ptr, value, order);
#else
#ifdef OS_32
  if (order == amoRelaxed)
    WriteNoFence((volatile LONG*)ptr, (LONG)value);
  else if (order == amoRelease)
    WriteRelease((volatile LONG*)ptr, (LONG)value);
  else
    InterlockedExchange((volatile LONG*)ptr, (LONG)value);
#else
  if (order == amoRelaxed)
    WriteNoFence64((volatile LONG64*)ptr, (LONG64)value);
  else if (order == amoRelease)
    WriteRelease64((volatile LONG64*)ptr, (LONG64)value);
  else
    InterlockedExchange64((volatile LONG64*)ptr, (LONG64)value);
#endif
#endif
}

static inline void *__pointer_atomic_load(void *volatile *ptr, AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_load_n(ptr, order);
#else
  return order == amoRelaxed ? ReadPointerNoFence(ptr) : ReadPointerAcquire(ptr);
#endif
}

static inline void __pointer_atomic_store(void *volatile *ptr, void *value, AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  __atomic_store_n(ptr, value, order);
#else
  if (order == amoRelaxed)
    WritePointerNoFence(ptr, value);
  else if (order == amoRelease)
    WritePointerRelease(ptr, value);
  else
    InterlockedExchangePointer(ptr, value);
#endif
}

static inline int __pointer_atomic_compare_and_swap(void *volatile *ptr,
                                                    void *expected,
                                                    void *desired,
                                                    AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_compare_exchange_n(ptr,
                                     &expected,
                                     desired,
                                     0,
                                     order,
                                     order == amoRelease ? amoRelaxed : order);
#else
  (void)order;
  return InterlockedCompareExchangePointer(ptr, desired, expected) == expected;
#endif
}

static inline void *__pointer_atomic_exchange(void *volatile *ptr,
                                              void *value,
                                              AtomicMemoryOrder order)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_exchange_n(ptr, value, order);
#else
  (void)order;
  return InterlockedExchangePointer(ptr, value);
#endif
}

__NO_UNUSED_FUNCTION_END
#endif //__ATOMIC_H_
