#ifndef __ATOMIC_H_
#define __ATOMIC_H_

#include "asyncio/asyncioTypes.h"
#include "macro.h"

__NO_UNUSED_FUNCTION_BEGIN
static inline unsigned __uint_atomic_fetch_and_add(unsigned volatile *ptr, unsigned value)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __sync_fetch_and_add(ptr, value);
#else
  return InterlockedExchangeAdd((volatile LONG*)ptr, value);
#endif
}

static inline int __uint_atomic_compare_and_swap(unsigned volatile *ptr, unsigned v1, unsigned v2)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __sync_bool_compare_and_swap(ptr, v1, v2);
#else
  return InterlockedCompareExchange((volatile LONG*)ptr, v2, v1) == v1;
#endif
}

static inline unsigned __uint_atomic_exchange(unsigned volatile *ptr, unsigned value)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __atomic_exchange_n(ptr, value, __ATOMIC_SEQ_CST);
#else
  return (unsigned)InterlockedExchange((volatile LONG*)ptr, (LONG)value);
#endif
}

static inline uintptr_t __uintptr_atomic_fetch_and_add(uintptr_t volatile *ptr, uintptr_t value)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __sync_fetch_and_add(ptr, value);
#else
#ifdef OS_32
  return InterlockedExchangeAdd((volatile LONG*)ptr, value);
#else
  return InterlockedExchangeAdd64((volatile LONG64*)ptr, value);
#endif
#endif
}

static inline uintptr_t __uintptr_atomic_fetch_or(uintptr_t volatile *ptr, uintptr_t value)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __sync_fetch_and_or(ptr, value);
#else
#ifdef OS_32
  return (uintptr_t)InterlockedOr((volatile LONG*)ptr, (LONG)value);
#else
  return (uintptr_t)InterlockedOr64((volatile LONG64*)ptr, (LONG64)value);
#endif
#endif
}

static inline int __uintptr_atomic_compare_and_swap(uintptr_t volatile *ptr, uintptr_t v1, uintptr_t v2)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __sync_bool_compare_and_swap(ptr, v1, v2);
#else
#ifdef OS_32
  return InterlockedCompareExchange((volatile LONG*)ptr, v2, v1) == v1;
#else
  return InterlockedCompareExchange64((volatile LONG64*)ptr, v2, v1) == v1;
#endif
#endif
}

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

// MSVC branches below fold to a single instruction when 'order' is a compile-time constant;
// seq_cst load maps to acquire (sufficient on x86 TSO and when paired with full-barrier stores)
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

static inline int __pointer_atomic_compare_and_swap(void *volatile *tag, void *v1, void *v2)
{
#ifndef _MSC_VER // Not Microsoft compiler
  return __sync_bool_compare_and_swap(tag, v1, v2);
#else
  return InterlockedCompareExchangePointer(tag, v2, v1) == v1;
#endif
}

static inline void *__pointer_atomic_exchange(void *volatile *pointer, void *v1)
{
#ifndef _MSC_VER
  return __atomic_exchange_n(pointer, v1, __ATOMIC_SEQ_CST);
#else
  return InterlockedExchangePointer(pointer, v1);
#endif
}

static inline void __spinlock_acquire(unsigned *lock)
{
  for (;;) {
    int i;
    for (i = 0; i < 7777; i++) {
      if (__uint_atomic_compare_and_swap(lock, 0, 1))
        return;
    }
#ifdef OS_WINDOWS
    SwitchToThread();
#else
    sched_yield();
#endif
  }
}

static inline int __spinlock_try_acquire(volatile unsigned *lock)
{
  return __uint_atomic_compare_and_swap(lock, 0, 1) ? 1 : 0;
}

static inline void __spinlock_release(volatile unsigned *lock)
{
  __uint_atomic_store(lock, 0, amoRelease);
}

__NO_UNUSED_FUNCTION_END
#endif //__ATOMIC_H_
