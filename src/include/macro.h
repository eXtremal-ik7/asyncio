#ifndef __UNUSED_H_
#define __UNUSED_H_
#define __UNUSED(x) (void)x

#if defined(__clang__)
#define __NO_PADDING_BEGIN \
  _Pragma("clang diagnostic push") \
  _Pragma("clang diagnostic ignored \"-Wpadded\"")
#define __NO_PADDING_END _Pragma("clang diagnostic pop")
#elif defined(__GNUC__)
#define __NO_PADDING_BEGIN \
  _Pragma("GCC diagnostic push") \
  _Pragma("GCC diagnostic ignored \"-Wpadded\"")
#define __NO_PADDING_END _Pragma("GCC diagnostic pop")
#else
#define __NO_PADDING_BEGIN
#define __NO_PADDING_END
#endif

#ifdef __clang__
#define __NO_UNUSED_FUNCTION_BEGIN \
  _Pragma("clang diagnostic push") \
  _Pragma("clang diagnostic ignored \"-Wunused-function\"")
#define __NO_UNUSED_FUNCTION_END _Pragma("clang diagnostic pop")
#else
#define __NO_UNUSED_FUNCTION_BEGIN
#define __NO_UNUSED_FUNCTION_END
#endif

#endif
