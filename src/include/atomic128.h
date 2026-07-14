#ifndef __ATOMIC128_H_
#define __ATOMIC128_H_

#include "asyncio/asyncioTypes.h"
#include "macro.h"

// 128-bit atomic pair for DWCAS-based protocols (timer-wheel slots, object
// generation heads and user-event mailboxes).
// Hardware contract: x86-64 with cmpxchg16b (-mcx16 outside MSVC), 64-bit
// ARMv8 (casp on LSE, ldxp/stxp otherwise); 32-bit targets are not supported.
// The pair changes ONLY through compare-and-swap — there is no atomic wide
// store, and the load is deliberately not a full-width snapshot.
//
// The default operations are full-barrier (seq_cst) equivalents, and that is
// deliberate: on every supported target the strongest CAS ordering costs the
// same instruction as the weakest sound one - x86-64 has a single encoding,
// lock cmpxchg16b, for every ordering; ARMv8 LSE lowers seq-cst and acq-rel
// CAS alike to caspal (LL/SC targets to a ldaxp/stlxp loop) - with no
// separate fence emitted, reviewed on the generated code of both ISAs. An
// ordering parameter would buy nothing, so none is exposed. Explicit relaxed
// variants are provided only for protocols that publish no payload through
// the pair itself (for example numeric timer control words); on compilers without
// a proven inline ordered DWCAS they deliberately fall back to the stronger
// default operation.

#if defined(_MSC_VER) && !defined(__clang__)
typedef struct __declspec(align(16)) uint128 {
  uint64_t low;
  uint64_t high;
} uint128;
#else
typedef struct __attribute__((aligned(16))) uint128 {
  uint64_t low;
  uint64_t high;
} uint128;
#endif

// The pair must stay two packed 64-bit words: CAS compares every bit
#ifdef __cplusplus
static_assert(sizeof(uint128) == 16, "uint128 must be two packed 64-bit words");
static_assert(alignof(uint128) == 16, "uint128 must be DWCAS aligned");
#elif defined(_MSC_VER) && !defined(__clang__)
// MSVC C mode runs without /std:c11 here and lacks _Static_assert; a
// negative array size fails the build the same way
typedef char uint128MustBeTwoPackedWords[sizeof(uint128) == 16 ? 1 : -1];
typedef char uint128MustBe16Aligned[__alignof(uint128) == 16 ? 1 : -1];
#else
_Static_assert(sizeof(uint128) == 16, "uint128 must be two packed 64-bit words");
_Static_assert(_Alignof(uint128) == 16, "uint128 must be DWCAS aligned");
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
static inline int __uint128_atomic_compare_and_swap(volatile uint128 *ptr, uint128 *expected, uint128 desired)
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

// Same atomic transition without ordering. Clang's lock-free guarantee above
// makes __atomic_compare_exchange_n an inline cmpxchg16b/casp/LL-SC here. GCC
// is kept on __sync because some supported GCC targets lower 16-byte
// __atomic_* to libatomic even when __sync CAS is inline; stronger ordering is
// preferable to silently reintroducing a lock.
static inline int __uint128_atomic_compare_and_swap_relaxed(volatile uint128 *ptr, uint128 *expected, uint128 desired)
{
#if defined(__clang__)
  typedef unsigned __int128 uint128Alias __attribute__((may_alias));
  uint128Alias expectedValue;
  uint128Alias desiredValue;
  __builtin_memcpy(&expectedValue, expected, sizeof(uint128Alias));
  __builtin_memcpy(&desiredValue, &desired, sizeof(uint128Alias));
  int exchanged = __atomic_compare_exchange_n((volatile uint128Alias*)ptr, &expectedValue, desiredValue, 0, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
  if (!exchanged)
    __builtin_memcpy(expected, &expectedValue, sizeof(uint128Alias));
  return exchanged;
#else
  return __uint128_atomic_compare_and_swap(ptr, expected, desired);
#endif
}

// Two independent acquire loads, NOT a CAS(0,0) full-width read: the result
// may be torn by a concurrent CAS. Deliberate: every routing decision made
// from this read is re-validated by the caller's subsequent CAS on the same
// pair, and payload publication is synchronized exclusively through CAS —
// never through this load. Readers stay write-free (no cache-line ownership
// transfer, works on read-only mappings).
static inline uint128 __uint128_atomic_load(const volatile uint128 *ptr)
{
  uint128 result;
#if defined(_MSC_VER) && !defined(__clang__)
  result.low = (uint64_t)ReadAcquire64((volatile LONG64*)&ptr->low);
  result.high = (uint64_t)ReadAcquire64((volatile LONG64*)&ptr->high);
#else
  result.low = __atomic_load_n(&ptr->low, __ATOMIC_ACQUIRE);
  result.high = __atomic_load_n(&ptr->high, __ATOMIC_ACQUIRE);
#endif
  return result;
}

// Like the ordinary routing load this may be torn; the following CAS validates
// every decision. It is intended for numeric/token pairs only.
static inline uint128 __uint128_atomic_load_relaxed(const volatile uint128 *ptr)
{
  uint128 result;
#if defined(_MSC_VER) && !defined(__clang__)
  result.low = (uint64_t)ReadNoFence64((volatile LONG64*)&ptr->low);
  result.high = (uint64_t)ReadNoFence64((volatile LONG64*)&ptr->high);
#else
  result.low = __atomic_load_n(&ptr->low, __ATOMIC_RELAXED);
  result.high = __atomic_load_n(&ptr->high, __ATOMIC_RELAXED);
#endif
  return result;
}

// CAS loop — there is no native 128-bit exchange. Lock-free, not wait-free:
// every retry means some other CAS on the pair succeeded.
static inline uint128 __uint128_atomic_exchange(volatile uint128 *ptr, uint128 value)
{
  // The seed is only a CAS expected. A successful full-barrier CAS acquires
  // the returned value; a failure replaces it with the CAS's atomic snapshot.
  uint128 expected = __uint128_atomic_load_relaxed(ptr);
  while (!__uint128_atomic_compare_and_swap(ptr, &expected, value))
    continue;
  return expected;
}

static inline uint128 __uint128_atomic_exchange_relaxed(volatile uint128 *ptr, uint128 value)
{
  uint128 expected = __uint128_atomic_load_relaxed(ptr);
  while (!__uint128_atomic_compare_and_swap_relaxed(ptr, &expected, value))
    continue;
  return expected;
}

__NO_UNUSED_FUNCTION_END
#endif //__ATOMIC128_H_
