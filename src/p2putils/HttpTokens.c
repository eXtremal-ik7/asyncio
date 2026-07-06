#include "HttpTokens.h"
#include "HttpTokenTableData.h"

static ParserResultTy lookupToken(const char **p,
                                  const char *end,
                                  char eos,
                                  const unsigned char *byteTable,
                                  const HttpTokenEntry *entries,
                                  const uint16_t *displacement,
                                  size_t tableSize,
                                  size_t groupCount,
                                  int *token)
{
  const char *begin = *p;
  const char *s = begin;
  uint64_t hash = 0xcbf29ce484222325ull;
  for (;;) {
    if (s == end)
      return ParserResultNeedMoreData;
    if (*s == eos)
      break;
    unsigned char folded = byteTable[(unsigned char)*s];
    if (folded == 0)
      return ParserResultError;
    hash = (hash ^ folded) * 0x100000001b3ull;
    s++;
  }

  size_t length = (size_t)(s - begin);
  if (length == 0)
    return ParserResultError;

  size_t slot = ((uint32_t)hash + displacement[(hash >> 32) & (groupCount - 1)]) & (tableSize - 1);
  const HttpTokenEntry *entry = &entries[slot];

  *token = 0;
  if (entry->name && entry->hash == hash && entry->length == length) {
    // a hash match alone must not identify a token: the input comes from the
    // network and can be crafted to collide, compare the folded bytes
    size_t i;
    for (i = 0; i < length; i++) {
      if (byteTable[(unsigned char)entry->name[i]] != byteTable[(unsigned char)begin[i]])
        break;
    }
    if (i == length)
      *token = entry->value;
  }

  *p = s;
  return ParserResultOk;
}

ParserResultTy httpHeaderNameLookup(const char **p, const char *end, int *token)
{
  return lookupToken(p, end, ':', httpTokenFoldTable,
                     HttpHeaderTable, HttpHeaderDisplacement, HttpHeaderTableSize, HttpHeaderGroupCount, token);
}

ParserResultTy httpMethodLookup(const char **p, const char *end, int *token)
{
  return lookupToken(p, end, ' ', httpTokenCharTable,
                     HttpMethodTable, HttpMethodDisplacement, HttpMethodTableSize, HttpMethodGroupCount, token);
}
