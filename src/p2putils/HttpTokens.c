#include "HttpTokens.h"
#include "HttpTokenTableData.h"
#include <stdlib.h>
#include <string.h>

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

ParserResultTy httpMethodLookup(const char **p, const char *end, int *token)
{
  return lookupToken(p, end, ' ', httpTokenCharTable,
                     HttpMethodTable, HttpMethodDisplacement, HttpMethodTableSize, HttpMethodGroupCount, token);
}

ParserResultTy httpHeaderTableLookup(const HttpHeaderTable *table,
                                     const char **p, const char *end, int *token)
{
  return lookupToken(p, end, ':', httpTokenFoldTable,
                     table->entries, table->displacement, table->tableSize, table->groupCount, token);
}

const HttpHeaderTable httpHeaderDefaultTable = {
  HttpHeaderReservedTable,
  HttpHeaderReservedDisplacement,
  HttpHeaderReservedTableSize,
  HttpHeaderReservedGroupCount,
  0
};

const HttpHeaderTable httpParseDefaultTable = {
  HttpParseDefaultTable,
  HttpParseDefaultDisplacement,
  HttpParseDefaultTableSize,
  HttpParseDefaultGroupCount,
  0
};

// Runtime builder of user header-recognition tables. Produces the same CHD
// shape the generated tables use, so the lookup above serves both. The
// reserved names are merged into every built table straight from the
// generated reserved table, which stays the single list of them.

enum {
  // table keys are bounded, so are the CHD retries
  httpHeaderTableMaxSize = 65536
};

// FNV-1a over the folded name bytes, mirroring lookupToken; rejects an
// empty name and bytes outside of the RFC 9110 token charset
static int hashHeaderName(const char *name, uint64_t *hashOut, uint32_t *lengthOut)
{
  uint64_t hash = 0xcbf29ce484222325ull;
  size_t length = 0;
  while (name[length]) {
    unsigned char folded = httpTokenFoldTable[(unsigned char)name[length]];
    if (folded == 0)
      return 0;
    hash = (hash ^ folded) * 0x100000001b3ull;
    length++;
  }
  if (length == 0 || length > UINT32_MAX)
    return 0;
  *hashOut = hash;
  *lengthOut = (uint32_t)length;
  return 1;
}

static int compareHashes(const void *l, const void *r)
{
  uint64_t lh = *(const uint64_t*)l, rh = *(const uint64_t*)r;
  return lh < rh ? -1 : (lh > rh ? 1 : 0);
}

typedef struct HeaderTableGroup {
  uint32_t index;
  uint32_t size;
} HeaderTableGroup;

// largest group first; ties keep the group order for a deterministic result
static int compareGroups(const void *l, const void *r)
{
  const HeaderTableGroup *lg = (const HeaderTableGroup*)l;
  const HeaderTableGroup *rg = (const HeaderTableGroup*)r;
  if (lg->size != rg->size)
    return lg->size > rg->size ? -1 : 1;
  return lg->index < rg->index ? -1 : (lg->index > rg->index ? 1 : 0);
}

// One CHD placement attempt: keys are grouped by the high hash bits, groups
// are placed largest first by searching a displacement that maps every key
// of the group onto free slots. slots (table index -> key index, -1 = free)
// and displacement are filled on success. groups, bucketStart, bucketed and
// positions are caller-provided scratch of groupCount, groupCount+1 and
// keyCount elements respectively.
static int placeHeaderKeys(const HttpTokenEntry *keys, size_t keyCount,
                           size_t tableSize, size_t groupCount,
                           HeaderTableGroup *groups, size_t *bucketStart,
                           uint32_t *bucketed, size_t *positions,
                           int32_t *slots, uint16_t *displacement)
{
  size_t i;
  for (i = 0; i < groupCount; i++) {
    groups[i].index = (uint32_t)i;
    groups[i].size = 0;
  }
  for (i = 0; i < keyCount; i++)
    groups[(keys[i].hash >> 32) & (groupCount - 1)].size++;

  // bucket the key indices by group, keeping the list order inside a group
  bucketStart[0] = 0;
  for (i = 0; i < groupCount; i++)
    bucketStart[i + 1] = bucketStart[i] + groups[i].size;
  for (i = 0; i < keyCount; i++) {
    size_t group = (keys[i].hash >> 32) & (groupCount - 1);
    bucketed[bucketStart[group]++] = (uint32_t)i;
  }
  for (i = groupCount; i > 0; i--)
    bucketStart[i] = bucketStart[i - 1];
  bucketStart[0] = 0;

  qsort(groups, groupCount, sizeof(HeaderTableGroup), compareGroups);

  for (i = 0; i < tableSize; i++)
    slots[i] = -1;
  memset(displacement, 0, groupCount * sizeof(uint16_t));

  for (i = 0; i < groupCount; i++) {
    size_t groupSize = groups[i].size;
    if (groupSize == 0)
      break;  // sorted by size, the rest are empty

    size_t begin = bucketStart[groups[i].index];
    size_t d;
    int placed = 0;
    for (d = 0; d < tableSize && !placed; d++) {
      size_t n;
      for (n = 0; n < groupSize; n++) {
        size_t pos = ((uint32_t)keys[bucketed[begin + n]].hash + d) & (tableSize - 1);
        size_t m;
        if (slots[pos] >= 0)
          break;
        for (m = 0; m < n; m++) {
          if (positions[m] == pos)
            break;
        }
        if (m < n)
          break;
        positions[n] = pos;
      }

      if (n == groupSize) {
        for (n = 0; n < groupSize; n++)
          slots[positions[n]] = (int32_t)bucketed[begin + n];
        displacement[groups[i].index] = (uint16_t)d;
        placed = 1;
      }
    }

    if (!placed)
      return 0;
  }

  return 1;
}

// Searches table shapes from the smallest fitting one up (power-of-two
// sizes, displacement arrays of the size down to a quarter of it) and packs
// the winning placement into the single backing allocation of *table
static int buildHeaderTable(HttpHeaderTable *table,
                            const HttpTokenEntry *keys, size_t keyCount,
                            size_t stringBytes,
                            uint32_t *bucketed, size_t *positions)
{
  size_t tableSize = 1;
  while (tableSize < keyCount)
    tableSize *= 2;

  int32_t *slots = 0;
  uint16_t *displacement = 0;
  size_t groupCount = 0;
  int placed = 0;
  for (; tableSize <= httpHeaderTableMaxSize && !placed; tableSize *= 2) {
    HeaderTableGroup *groups = (HeaderTableGroup*)malloc(tableSize * sizeof(HeaderTableGroup));
    size_t *bucketStart = (size_t*)malloc((tableSize + 1) * sizeof(size_t));
    slots = (int32_t*)malloc(tableSize * sizeof(int32_t));
    displacement = (uint16_t*)malloc(tableSize * sizeof(uint16_t));
    int allocOk = groups && bucketStart && slots && displacement;
    if (allocOk) {
      size_t lowerBound = tableSize / 4 > 0 ? tableSize / 4 : 1;
      size_t g;
      for (g = tableSize; g >= lowerBound && !placed; g /= 2) {
        if (placeHeaderKeys(keys, keyCount, tableSize, g,
                            groups, bucketStart, bucketed, positions, slots, displacement)) {
          groupCount = g;
          placed = 1;
        }
      }
    }
    free(groups);
    free(bucketStart);
    if (!placed) {
      free(slots);
      free(displacement);
      slots = 0;
      displacement = 0;
      if (!allocOk)
        return 0;
    }
  }
  if (!placed)
    return 0;
  tableSize /= 2;  // the loop advanced past the winning size

  // single backing block: entries, then displacement, then the name bytes
  size_t entriesBytes = tableSize * sizeof(HttpTokenEntry);
  size_t displacementBytes = groupCount * sizeof(uint16_t);
  unsigned char *block = (unsigned char*)malloc(entriesBytes + displacementBytes + stringBytes);
  if (!block) {
    free(slots);
    free(displacement);
    return 0;
  }

  HttpTokenEntry *entries = (HttpTokenEntry*)block;
  uint16_t *finalDisplacement = (uint16_t*)(block + entriesBytes);
  char *strings = (char*)(block + entriesBytes + displacementBytes);
  memcpy(finalDisplacement, displacement, displacementBytes);

  size_t i;
  for (i = 0; i < tableSize; i++) {
    if (slots[i] >= 0) {
      const HttpTokenEntry *key = &keys[slots[i]];
      memcpy(strings, key->name, key->length);
      strings[key->length] = 0;
      entries[i].name = strings;
      entries[i].length = key->length;
      entries[i].value = key->value;
      entries[i].hash = key->hash;
      strings += key->length + 1;
    } else {
      entries[i].name = 0;
      entries[i].length = 0;
      entries[i].value = 0;
      entries[i].hash = 0;
    }
  }
  free(slots);
  free(displacement);

  table->entries = entries;
  table->displacement = finalDisplacement;
  table->tableSize = tableSize;
  table->groupCount = groupCount;
  table->block = block;
  return 1;
}

int httpHeaderTablePrepare(HttpHeaderTable *table,
                           const HttpHeaderTableEntry *entries, size_t count)
{
  table->entries = 0;
  table->displacement = 0;
  table->tableSize = 0;
  table->groupCount = 0;
  table->block = 0;

  // more keys than the largest table can hold never places (this also keeps
  // the arithmetic below far from overflow)
  if (count > httpHeaderTableMaxSize)
    return 0;

  size_t reservedCount = 0;
  size_t slot;
  for (slot = 0; slot < HttpHeaderReservedTableSize; slot++)
    reservedCount += HttpHeaderReservedTable[slot].name != 0;
  size_t keyCount = count + reservedCount;

  HttpTokenEntry *keys = (HttpTokenEntry*)malloc(keyCount * sizeof(HttpTokenEntry));
  uint64_t *sortedHashes = (uint64_t*)malloc(keyCount * sizeof(uint64_t));
  uint32_t *bucketed = (uint32_t*)malloc(keyCount * sizeof(uint32_t));
  size_t *positions = (size_t*)malloc(keyCount * sizeof(size_t));

  int result = 0;
  if (keys && sortedHashes && bucketed && positions) {
    size_t stringBytes = 0;
    size_t i;
    int valid = 1;
    for (i = 0; i < count && valid; i++) {
      valid = entries[i].name != 0 &&
              entries[i].id >= 1 && entries[i].id < hhReservedBase &&
              hashHeaderName(entries[i].name, &keys[i].hash, &keys[i].length);
      if (valid) {
        keys[i].name = entries[i].name;
        keys[i].value = entries[i].id;
        sortedHashes[i] = keys[i].hash;
        stringBytes += (size_t)keys[i].length + 1;
      }
    }
    if (valid) {
      for (slot = 0; slot < HttpHeaderReservedTableSize; slot++) {
        if (!HttpHeaderReservedTable[slot].name)
          continue;
        keys[i] = HttpHeaderReservedTable[slot];
        sortedHashes[i] = keys[i].hash;
        stringBytes += (size_t)keys[i].length + 1;
        i++;
      }
    }

    if (valid) {
      // equal folded-bytes hashes reject duplicated names, reserved names
      // appearing in the user list and genuine 64-bit collisions alike:
      // the lookup can deliver only one value per full hash
      qsort(sortedHashes, keyCount, sizeof(uint64_t), compareHashes);
      for (i = 1; i < keyCount && valid; i++)
        valid = sortedHashes[i] != sortedHashes[i - 1];
    }

    if (valid)
      result = buildHeaderTable(table, keys, keyCount, stringBytes, bucketed, positions);
  }

  free(keys);
  free(sortedHashes);
  free(bucketed);
  free(positions);
  return result;
}

void httpHeaderTableFree(HttpHeaderTable *table)
{
  free(table->block);
  table->entries = 0;
  table->displacement = 0;
  table->tableSize = 0;
  table->groupCount = 0;
  table->block = 0;
}
