#pragma once

#include "p2putils/CommonParse.h"
#include "p2putils/HttpHeaderTable.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// One slot of a CHD perfect hash table over token names, built either at
// generation time by tokengen (HttpTokenTableData.h) or at runtime by
// httpHeaderTablePrepare: the original spelling of the name, the 64-bit
// FNV-1a hash of its folded bytes and the value delivered on a match;
// empty slots keep name == NULL
typedef struct HttpTokenEntry {
  const char *name;
  uint32_t length;
  int32_t value;
  uint64_t hash;
} HttpTokenEntry;

// The built-in recognition table: the reserved names only. Substituted by
// the parsers for a NULL table argument; never freed.
extern const HttpHeaderTable httpHeaderDefaultTable;

// Recognizes an HTTP request method via the generated perfect hash table
// (HttpTokenTableData.h), validating the RFC 9110 token charset and
// verifying the matched key, so a hash collision can not misidentify it:
//  - Ok: *p points at the terminator (' '), *token is the hm* value or
//    hmUnknown for a valid extension method missing from the table
//  - NeedMoreData: the terminator was not reached, *p is left untouched
//  - Error: empty name or a byte outside of the token charset
ParserResultTy httpMethodLookup(const char **p, const char *end, int *token);

// Same lookup contract for a header field name against a prepared table,
// case-insensitively, up to the ':' terminator: *token is the id assigned
// by the table owner, one of the reserved hh* ids, or 0 for a valid name
// missing from the table
ParserResultTy httpHeaderTableLookup(const HttpHeaderTable *table,
                                     const char **p, const char *end, int *token);

#ifdef __cplusplus
}
#endif
