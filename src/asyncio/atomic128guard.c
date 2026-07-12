// Anchor TU for the post-build asm guard (cmake/atomic128-guard.cmake):
// forces the inline atomic128 layer into a standalone object so nm/objdump
// can prove the DWCAS stayed inline (no silent libatomic fallback) and that
// TSan builds route it through __tsan_atomic128_*.
#include "atomic128.h"

int atomic128GuardCas(volatile uint128Pair *ptr, uint128Pair *expected, uint128Pair desired)
{
  return __uint128_atomic_compare_and_swap(ptr, expected, desired);
}

uint128Pair atomic128GuardLoad(const volatile uint128Pair *ptr)
{
  return __uint128_atomic_load(ptr);
}

uint128Pair atomic128GuardExchange(volatile uint128Pair *ptr, uint128Pair value)
{
  return __uint128_atomic_exchange(ptr, value);
}

int atomic128GuardCasRelaxed(volatile uint128Pair *ptr,
                             uint128Pair *expected,
                             uint128Pair desired)
{
  return __uint128_atomic_compare_and_swap_relaxed(ptr, expected, desired);
}

uint128Pair atomic128GuardLoadRelaxed(const volatile uint128Pair *ptr)
{
  return __uint128_atomic_load_relaxed(ptr);
}

uint128Pair atomic128GuardExchangeRelaxed(volatile uint128Pair *ptr,
                                          uint128Pair value)
{
  return __uint128_atomic_exchange_relaxed(ptr, value);
}
