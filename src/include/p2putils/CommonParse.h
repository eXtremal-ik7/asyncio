#ifndef __LIBP2P_COMMONPARSE_H_
#define __LIBP2P_COMMONPARSE_H_

#include <stddef.h>

typedef struct Raw {
  const char *data;
  size_t size;
} Raw;

typedef enum ParserResultTy {
  ParserResultOk = 0,
  ParserResultNeedMoreData,
  ParserResultError,
  ParserResultCancelled
} ParserResultTy;

#endif //__LIBP2P_COMMONPARSE_H_
