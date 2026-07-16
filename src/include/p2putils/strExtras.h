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

#endif //__LIBP2P_STREXTRAS_H_
