#ifndef __LIBP2P_HTTPHEADERTABLE_H_
#define __LIBP2P_HTTPHEADERTABLE_H_

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// One entry of a client-owned list of recognized HTTP header names: when a
// parsed header field matches `name` (case-insensitively), its component
// carries `id`. Ids are plain ints so client lists and switches can use
// C enum constants directly; the list owner picks them from [1, 0x3FFFFFFF],
// values from 0x40000000 up belong to names the library recognizes with
// every table (bit 30 keeps them positive ints clear of the user range).
typedef struct HttpHeaderTableEntry {
  const char *name;  // RFC 9110 token charset, the case is not significant
  int id;            // [1, 0x3FFFFFFF]
} HttpHeaderTableEntry;

// A prepared name-recognition table. The layout is private: filled by
// httpHeaderTablePrepare (one backing allocation), read by the parser.
// A prepared table is immutable, it can be shared by parsers on any threads
// and lives until httpHeaderTableFree.
typedef struct HttpHeaderTable {
  const struct HttpTokenEntry *entries;
  const uint16_t *displacement;
  size_t tableSize;
  size_t groupCount;
  void *block;
} HttpHeaderTable;

// Builds the recognition table for a user list: validates the entries,
// merges in the names reserved by the library and searches perfect hash
// parameters for the union. Names are copied inside, so the list and its
// strings can be released right after the call. count == 0 (entries may be
// NULL) is legal and yields a table of the reserved names only.
// Returns 1 on success. Returns 0 and leaves *table empty (safe to free)
// when: an id is outside of [1, 0x3FFFFFFF], a name is NULL, empty or
// contains a byte outside of the RFC 9110 token charset, two names coincide
// after the case fold, a name is reserved by the library, the full 64-bit
// name hashes collide, an allocation fails or no perfect hash placement
// exists.
int httpHeaderTablePrepare(HttpHeaderTable *table,
                           const HttpHeaderTableEntry *entries, size_t count);
void httpHeaderTableFree(HttpHeaderTable *table);

// The recognition table of the httpParseDefault callback (asyncio http
// client; httpParseDefaultInit installs it on the client): the reserved
// names plus the names the callback itself consumes, with the hpd* ids.
// The composition belongs to the callback and may change in any release -
// do not rely on it in your own parsing. Static, never freed.
extern const HttpHeaderTable httpParseDefaultTable;

#ifdef __cplusplus
}
#endif

#endif //__LIBP2P_HTTPHEADERTABLE_H_
