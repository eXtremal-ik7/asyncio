#ifndef __LIBP2P_STREXTRAS_H_
#define __LIBP2P_STREXTRAS_H_

#include <stdint.h>
#include <algorithm>
#include "asyncioconfig.h"

#if defined(__GNUC__) || defined(__clang__)
static inline uint16_t xswap(uint16_t value) { return __builtin_bswap16(value); }
static inline int16_t xswap(int16_t value) { return __builtin_bswap16(value); }
static inline uint32_t xswap(uint32_t value) { return __builtin_bswap32(value); }
static inline int32_t xswap(int32_t value) { return __builtin_bswap32(value); }
static inline uint64_t xswap(uint64_t value) { return __builtin_bswap64(value); }
static inline int64_t xswap(int64_t value) { return __builtin_bswap64(value); }
#elif defined(_MSC_VER)
static inline uint16_t xswap(uint16_t value) { return _byteswap_ushort(value); }
static inline int16_t xswap(int16_t value) { return _byteswap_ushort(value); }
static inline uint32_t xswap(uint32_t value) { return _byteswap_ulong(value); }
static inline int32_t xswap(int32_t value) { return _byteswap_ulong(value); }
static inline uint64_t xswap(uint64_t value) { return _byteswap_uint64(value); }
static inline int64_t xswap(int64_t value) { return _byteswap_uint64(value); }
#endif

template<typename IntType> IntType xhtole(IntType value) { return is_bigendian() ? xswap(value) : value; }
template<typename IntType> IntType xhtobe(IntType value) { return is_bigendian() ? value : xswap(value); }
template<typename IntType> IntType xletoh(IntType value) { return is_bigendian() ? xswap(value) : value; }
template<typename IntType> IntType xbetoh(IntType value) { return is_bigendian() ? value : xswap(value); }
template<typename IntType> IntType xhton(IntType value) { return xhtobe(value); }
template<typename IntType> IntType xntoh(IntType value) { return xbetoh(value); }

template<typename Type> size_t xitoa(Type value, char *out)
{
  Type lvalue = value;
  char *lout = out;
  char *pout = out, *pOutEnd;
  
  if (lvalue < 0) {
    lvalue = ((Type)0)-lvalue;
    *pout++ = '-';
    lout++;
  }
  
  do {
    *pout++ = '0' + lvalue % 10;
    lvalue /= 10;
  } while (lvalue);
  
  pOutEnd = pout;
  *pOutEnd = 0;
  
  pout--;
  while (lout < pout)
    std::swap(*lout++, *pout--); 
  
  return pOutEnd - out;
}

template<typename Type> size_t xitoah(Type value, char *out, bool leadingZeroes, bool zeroxPrefix, bool upperCase)
{
  char *pOut = out;
  if (value < 0) {
    value = static_cast<Type>(0) - value;
    *pOut++ = '-';
  }

  if (zeroxPrefix) {
    *pOut++ = '0';
    *pOut++ = 'x';
  }

  bool leadingZeroFinished = false;
  for (size_t i = 0; i < sizeof(Type)*2; i++) {
    char digit = (value >> (sizeof(Type)*8 - 4)) & 0xFF;
    if (!leadingZeroes && digit == 0 && !leadingZeroFinished) {
      value <<= 4;
      continue;
    }

    if (digit < 10)
      digit += '0';
    else
      digit += (upperCase ? 'A' : 'a') - 10;

    *pOut++ = digit;
    value <<= 4;
    leadingZeroFinished = true;
  }

  if (!leadingZeroFinished)
    *pOut++ = '0';

  *pOut = 0;
  return pOut - out;
}

template<typename Type> Type xatoi(const char *in)
{
  int minus = 0;
  Type lvalue = 0;
  const char *p = in;
  if (*p == '-') {
    minus = 1;
    p++;
  } else if (*p == '+') {
    p++;
  }
  while (*p)
    lvalue = (lvalue*10) + (*p++ - '0');
  
  return minus ? static_cast<Type>(0)-lvalue : lvalue;
}

#endif //__LIBP2P_STREXTRAS_H_
