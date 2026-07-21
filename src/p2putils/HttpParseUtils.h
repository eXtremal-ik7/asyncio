#pragma once

// Shared low-level helpers of the HTTP response (HttpParse.cpp) and request
// (HttpRequestParse.cpp) parsers. Line scanners advance the caller's pointer
// only on ParserResultOk; parseTrailers is the deliberate exception and
// commits each complete field before returning NeedMoreData.

#include "p2putils/CommonParse.h"
#include "p2putils/HttpParseCommon.h"
#include "HttpTokens.h"
#include "macro.h"
#include <stdint.h>
#include <string.h>

static inline int isDigit(char s)
{
  return s >= '0' && s <= '9';
}

static inline int canRead(const char *ptr, const char *end, size_t size)
{
  return size <= static_cast<size_t>(end - ptr);
}

// the caller must ensure at least /size/ readable bytes
static inline ParserResultTy compareUnchecked(const char **ptr, const char *substr, size_t size)
{
  if (memcmp(*ptr, substr, size) == 0) {
    (*ptr) += size;
    return ParserResultOk;
  } else {
    return ParserResultError;
  }
}

// request line separator: at least one space is required
static inline ParserResultTy skipSPCharacters(const char **ptr, const char *end)
{
  if (*ptr == end)
    return ParserResultNeedMoreData;
  if (**ptr != ' ')
    return ParserResultError;
  while (*ptr != end && **ptr == ' ')
    (*ptr)++;
  return ParserResultOk;
}

// optional whitespace around header values (RFC 9110 OWS)
static inline void skipOWS(const char **ptr, const char *end)
{
  while (*ptr != end && (**ptr == ' ' || **ptr == '\t'))
    (*ptr)++;
}

static inline void trimTrailingOWS(Raw *value)
{
  while (value->size && (value->data[value->size-1] == ' ' || value->data[value->size-1] == '\t'))
    value->size--;
}

static inline ParserResultTy readUntilCRLF(const char **ptr, const char *end)
{
  const char *p = *ptr;
  const size_t size = static_cast<size_t>(end - p);
  const char *cr = static_cast<const char*>(memchr(p, '\r', size));
  const size_t prefixSize = cr ? static_cast<size_t>(cr - p) : size;
  const char *lf = static_cast<const char*>(memchr(p, '\n', prefixSize));
  if (!cr)
    return lf ? ParserResultError : ParserResultNeedMoreData;
  if (lf)
    return ParserResultError;
  if (cr + 1 == end)
    return ParserResultNeedMoreData;
  if (cr[1] != '\n')
    return ParserResultError;
  *ptr = cr + 2;
  return ParserResultOk;
}

// decimal Content-Length value followed by CRLF; empty values, values with
// unexpected characters and values overflowing size_t are errors
static inline ParserResultTy readDec(const char **ptr, const char *end, size_t *size)
{
  const char *p = *ptr;
  size_t value = 0;
  int digits = 0;
  while (p != end && isDigit(*p)) {
    unsigned digit = static_cast<unsigned>(*p - '0');
    if (value > (SIZE_MAX - digit) / 10)
      return ParserResultError;
    value = value*10 + digit;
    digits++;
    p++;
  }

  skipOWS(&p, end);
  if (!canRead(p, end, 2))
    return ParserResultNeedMoreData;
  if (digits == 0 || p[0] != '\r' || p[1] != '\n')
    return ParserResultError;

  *ptr = p + 2;
  *size = value;
  return ParserResultOk;
}

// hexadecimal chunk size; after the required ';', chunk-extension contents
// are skipped without validation up to CRLF
static inline ParserResultTy readHex(const char **ptr, const char *end, size_t *size)
{
  const char *p = *ptr;
  size_t value = 0;
  int digits = 0;
  for (; p != end; p++) {
    unsigned digit;
    if (*p >= '0' && *p <= '9')
      digit = static_cast<unsigned>(*p - '0');
    else if (*p >= 'a' && *p <= 'f')
      digit = static_cast<unsigned>(*p - 'a' + 10);
    else if (*p >= 'A' && *p <= 'F')
      digit = static_cast<unsigned>(*p - 'A' + 10);
    else
      break;
    if (value > (SIZE_MAX >> 4))
      return ParserResultError;
    value = (value << 4) | digit;
    digits++;
  }

  if (p != end && *p != '\r') {
    // chunk-ext starts with ';'; arbitrary text after the size is not an
    // extension and must not be skipped as if it were one
    if (*p != ';')
      return ParserResultError;
    while (p != end && *p != '\r' && *p != '\n')
      p++;
  }
  if (!canRead(p, end, 2))
    return ParserResultNeedMoreData;
  if (digits == 0 || p[0] != '\r' || p[1] != '\n')
    return ParserResultError;

  *ptr = p + 2;
  *size = value;
  return ParserResultOk;
}

// RFC 9112: a message is chunked when "chunked" is the final transfer coding
static inline int isChunkedTransferEncoding(const Raw *value)
{
  const char chunked[] = "chunked";
  if (value->size < sizeof(chunked)-1)
    return 0;
  const char *tail = value->data + value->size - (sizeof(chunked)-1);
  for (size_t i = 0; i < sizeof(chunked)-1; i++) {
    if ((tail[i] | 0x20) != chunked[i])
      return 0;
  }
  if (value->size == sizeof(chunked)-1)
    return 1;
  const char *separator = tail;
  while (separator != value->data &&
         (separator[-1] == ' ' || separator[-1] == '\t'))
    separator--;
  return separator != value->data && separator[-1] == ',';
}

// Commit complete trailer fields one by one, retaining only the unfinished
// line across buffers. The terminating empty line is committed by the caller
// after its final callback succeeds.
static inline ParserResultTy parseTrailers(const char **ptr, const char *end,
                                           const char **trailersEnd)
{
  const char *p = *ptr;
  for (;;) {
    if (canRead(p, end, 2) && p[0] == '\r' && p[1] == '\n') {
      *trailersEnd = p + 2;
      return ParserResultOk;
    }

    ParserResultTy result = readUntilCRLF(&p, end);
    if (result != ParserResultOk)
      return result;
    *ptr = p;
  }
}

// One header line shared by the response and request parsers: the name
// lookup against the caller's recognition table plus the framing
// interpretation (Content-Length feeds dataRemaining, Transfer-Encoding the
// chunked flag; conflicting repeats and CL+TE combinations are rejected).
// The component is emitted for every header line with the
// raw value always filled; Content-Length additionally carries the parsed
// number in sizeValue. The caller sets component->type once and keeps the
// end-of-headers (empty line) handling to itself; emit() returning false
// becomes ParserResultCancelled (the response parser cannot cancel, its
// emitter always returns true and the branch folds away at instantiation).
template<typename State, typename Component, typename Emit>
static ParserResultTy parseHeaderLine(State *state, const HttpHeaderTable *table, Component *component, Emit emit)
{
  ParserResultTy result;
  int token;
  const char *p = state->ptr;
  if ((result = httpHeaderTableLookup(table, &p, state->end, &token)) != ParserResultOk)
    return result;

  component->header.entryType = token;
  component->header.entryName.data = state->ptr;
  component->header.entryName.size = static_cast<size_t>(p - state->ptr);
  ++p;
  skipOWS(&p, state->end);

  component->header.stringValue.data = p;
  component->header.sizeValue = 0;
  if (token == hhContentLength) {
    size_t contentSize;
    if ((result = readDec(&p, state->end, &contentSize)) != ParserResultOk)
      return result;
    // Framing conflicts are smuggling vectors: Content-Length next to
    // Transfer-Encoding is rejected, a repeat only accepted when identical
    if (state->seenTransferEncoding || (state->seenContentLength && state->dataRemaining != contentSize))
      return ParserResultError;
    component->header.sizeValue = contentSize;
    state->dataRemaining = contentSize;
    state->seenContentLength = 1;
  } else {
    if ((result = readUntilCRLF(&p, state->end)) != ParserResultOk)
      return result;
  }

  component->header.stringValue.size = static_cast<size_t>(p - component->header.stringValue.data - 2);
  trimTrailingOWS(&component->header.stringValue);
  if (token == hhTransferEncoding) {
    // a coding listed after chunked would make chunked non-final; next to
    // Content-Length the framings conflict - both are rejected
    if (state->seenContentLength || state->chunked)
      return ParserResultError;
    state->chunked = isChunkedTransferEncoding(&component->header.stringValue);
    state->seenTransferEncoding = 1;
  }
  if (!emit())
    return ParserResultCancelled;

  state->ptr = p;
  return ParserResultOk;
}

// The chunked-body machine shared by the response and request parsers: data
// of the current chunk (emitted as it arrives), the CRLF closing a non-first
// chunk and the size line of the next one. The zero chunk transitions to a
// separate trailer state so complete trailer lines can be discarded while
// streaming. The parsers differ only in their fragment emitter.
template<typename State, typename StateValue, typename EmitFragment>
static ParserResultTy parseChunkedBody(State *state, StateValue trailerState,
                                       EmitFragment emitFragment)
{
  ParserResultTy result;
  const char *p = state->ptr;

  for (;;) {
    if (state->dataRemaining) {
      const char *readyChunk = p;
      const size_t remaining = state->dataRemaining;
      const bool complete = canRead(p, state->end, remaining);
      const size_t readyChunkSize = complete
          ? remaining : static_cast<size_t>(state->end - p);

      if (readyChunkSize && !emitFragment(readyChunk, readyChunkSize))
        return ParserResultCancelled;

      p += readyChunkSize;
      state->ptr = p;
      state->dataRemaining = remaining - readyChunkSize;
      if (!complete)
        return ParserResultNeedMoreData;
      state->firstFragment = false;
    } else {
      // we at begin of next chunk
      if (!state->firstFragment) {
        // every non-final chunk is followed by an exact CRLF
        if (!canRead(p, state->end, 2))
          return ParserResultNeedMoreData;
        if (p[0] != '\r' || p[1] != '\n')
          return ParserResultError;
        p += 2;
      }

      size_t chunkSize;
      if ((result = readHex(&p, state->end, &chunkSize)) != ParserResultOk)
        return result;

      if (chunkSize == 0) {
        state->ptr = p;
        state->state = trailerState;
        return ParserResultOk;
      }

      // The size line is complete even when no payload byte is available yet.
      // Commit it so a resumed buffer starts at the payload, not at the size.
      state->ptr = p;
      state->dataRemaining = chunkSize;
    }
  }
}
