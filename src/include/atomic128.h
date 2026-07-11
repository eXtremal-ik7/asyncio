#ifndef __ATOMIC128_H_
#define __ATOMIC128_H_

#include "asyncio/asyncioTypes.h"
#include "macro.h"

// 128-bit atomic pair for DWCAS-based protocols (timer wheel slots).
// Hardware contract: x86-64 with cmpxchg16b (-mcx16 outside MSVC), 64-bit
// ARMv8 (casp on LSE, ldxp/stxp otherwise); 32-bit targets are not supported.
// The pair changes ONLY through compare-and-swap — there is no atomic wide
// store, and the load is deliberately not a full-width snapshot.
//
// All operations are full-barrier (seq_cst) equivalents; relaxed/acq_rel
// variants are a separate patch with per-target asm review.

#if defined(_MSC_VER) && !defined(__clang__)
typedef struct __declspec(align(16)) uint128Pair {
  uint64_t low;
  uint64_t high;
} uint128Pair;
#else
typedef struct __attribute__((aligned(16))) uint128Pair {
  uint64_t low;
  uint64_t high;
} uint128Pair;
#endif

// The pair must stay two packed 64-bit words: CAS compares every bit
#ifdef __cplusplus
static_assert(sizeof(uint128Pair) == 16, "uint128Pair must be two packed 64-bit words");
#elif defined(_MSC_VER) && !defined(__clang__)
// MSVC C mode runs without /std:c11 here and lacks _Static_assert; a
// negative array size fails the build the same way
typedef char uint128PairMustBeTwoPackedWords[sizeof(uint128Pair) == 16 ? 1 : -1];
#else
_Static_assert(sizeof(uint128Pair) == 16, "uint128Pair must be two packed 64-bit words");
#endif

// Build-time guard against silent non-lock-free degradation (a mutex inside
// libatomic would reintroduce the spinlock this layer exists to remove).
#if defined(_MSC_VER) && !defined(__clang__)
#if !defined(_M_X64) && !defined(_M_ARM64)
#error "atomic128: InterlockedCompareExchange128 requires a 64-bit target (x64 or ARM64)"
#endif
#elif defined(__clang__)
#ifdef __cplusplus
static_assert(__atomic_always_lock_free(16, 0), "atomic128: 128-bit CAS is not lock-free on this target (x86-64 requires -mcx16)");
#else
_Static_assert(__atomic_always_lock_free(16, 0), "atomic128: 128-bit CAS is not lock-free on this target (x86-64 requires -mcx16)");
#endif
#else
#if !defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_16)
#error "atomic128: no inline 128-bit __sync CAS on this target (x86-64 requires -mcx16; 32-bit targets are not supported)"
#endif
#endif

__NO_UNUSED_FUNCTION_BEGIN

// Strong CAS. On failure *expected receives the observed pair — that value is
// an atomic snapshot, unlike __uint128_atomic_load below. Returns non-zero on
// success.
static inline int __uint128_atomic_compare_and_swap(volatile uint128Pair *ptr, uint128Pair *expected, uint128Pair desired)
{
#if defined(_MSC_VER) && !defined(__clang__)
  return InterlockedCompareExchange128((volatile LONG64*)ptr, (LONG64)desired.high, (LONG64)desired.low, (LONG64*)expected);
#else
  // __sync, not __atomic: GCC lowers 16-byte __atomic_* to libatomic calls even
  // with -mcx16, while __sync_val_compare_and_swap inlines cmpxchg16b/casp.
  // TSan instruments both forms the same way (__tsan_atomic128_*).
  typedef unsigned __int128 uint128Alias __attribute__((may_alias));
  uint128Alias expectedValue;
  uint128Alias desiredValue;
  __builtin_memcpy(&expectedValue, expected, sizeof(uint128Alias));
  __builtin_memcpy(&desiredValue, &desired, sizeof(uint128Alias));
  uint128Alias observed = __sync_val_compare_and_swap((volatile uint128Alias*)ptr, expectedValue, desiredValue);
  if (observed == expectedValue)
    return 1;
  __builtin_memcpy(expected, &observed, sizeof(uint128Alias));
  return 0;
#endif
}

// Two independent acquire loads, NOT a CAS(0,0) full-width read: the result
// may be torn by a concurrent CAS. Deliberate: every routing decision made
// from this read is re-validated by the caller's subsequent CAS on the same
// pair, and payload publication is synchronized exclusively through CAS —
// never through this load. Readers stay write-free (no cache-line ownership
// transfer, works on read-only mappings).
static inline uint128Pair __uint128_atomic_load(const volatile uint128Pair *ptr)
{
  uint128Pair result;
#if defined(_MSC_VER) && !defined(__clang__)
  result.low = (uint64_t)ReadAcquire64((volatile LONG64*)&ptr->low);
  result.high = (uint64_t)ReadAcquire64((volatile LONG64*)&ptr->high);
#else
  result.low = __atomic_load_n(&ptr->low, __ATOMIC_ACQUIRE);
  result.high = __atomic_load_n(&ptr->high, __ATOMIC_ACQUIRE);
#endif
  return result;
}

// CAS loop — there is no native 128-bit exchange. Lock-free, not wait-free:
// every retry means some other CAS on the pair succeeded.
static inline uint128Pair __uint128_atomic_exchange(volatile uint128Pair *ptr, uint128Pair value)
{
  uint128Pair expected = __uint128_atomic_load(ptr);
  while (!__uint128_atomic_compare_and_swap(ptr, &expected, value))
    continue;
  return expected;
}

__NO_UNUSED_FUNCTION_END
#endif //__ATOMIC128_H_
