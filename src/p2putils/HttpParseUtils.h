#pragma once

// Shared low-level helpers of the HTTP response (HttpParse.cpp) and request
// (HttpRequestParse.cpp) parsers. All scanning helpers advance the caller's
// pointer only on ParserResultOk, so an interrupted line is rescanned from
// its beginning once more data arrives.

#include "p2putils/CommonParse.h"
#include "p2putils/HttpParseCommon.h"
#include "HttpTokens.h"
#include <algorithm>
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
  for (;;) {
    size_t available = static_cast<size_t>(end - p);
    if (available < 2)
      return ParserResultNeedMoreData;
    const char *cr = static_cast<const char*>(memchr(p, '\r', available));
    if (!cr || cr == end - 1)
      return ParserResultNeedMoreData;
    if (cr[1] == '\n') {
      *ptr = cr + 2;
      return ParserResultOk;
    }
    p = cr + 1;
  }
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

// hexadecimal chunk size; chunk extensions (";name=value") are skipped
// without validation up to CRLF
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

  while (p != end && *p != '\r' && *p != '\n')
    p++;
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
  char before = tail[-1];
  return before == ',' || before == ' ' || before == '\t';
}

// trailer section after the last chunk: optional trailer fields are skipped
// up to the terminating empty line
static inline ParserResultTy skipTrailers(const char **ptr, const char *end)
{
  const char *p = *ptr;
  for (;;) {
    if (!canRead(p, end, 2))
      return ParserResultNeedMoreData;
    if (p[0] == '\r' && p[1] == '\n') {
      *ptr = p + 2;
      return ParserResultOk;
    }

    ParserResultTy result = readUntilCRLF(&p, end);
    if (result != ParserResultOk)
      return result;
  }
}

// One header line shared by the response and request parsers: the name-token
// lookup plus the Content-Length / string-value branch. The caller sets
// component->type once and keeps the end-of-headers (empty line) handling to
// itself; emit() returning false becomes ParserResultCancelled (the response
// parser cannot cancel, its emitter always returns true and the branch folds
// away at instantiation).
template<typename State, typename Component, typename Emit>
static ParserResultTy parseHeaderLine(State *state, Component *component, Emit emit)
{
  ParserResultTy result;
  int token;
  const char *p = state->ptr;
  if ((result = httpHeaderNameLookup(&p, state->end, &token)) != ParserResultOk)
    return result;

  component->header.entryType = token;
  component->header.entryName.data = state->ptr;
  component->header.entryName.size = static_cast<size_t>(p - state->ptr);
  ++p;
  skipOWS(&p, state->end);

  if (token == hhContentLength) {
    size_t contentSize;
    if ((result = readDec(&p, state->end, &contentSize)) != ParserResultOk)
      return result;
    component->header.sizeValue = contentSize;
    state->dataRemaining = contentSize;
    if (!emit())
      return ParserResultCancelled;
  } else {
    component->header.stringValue.data = p;
    if ((result = readUntilCRLF(&p, state->end)) != ParserResultOk)
      return result;
    component->header.stringValue.size = static_cast<size_t>(p - component->header.stringValue.data - 2);
    trimTrailingOWS(&component->header.stringValue);
    if (token == hhTransferEncoding)
      state->chunked = isChunkedTransferEncoding(&component->header.stringValue);
    if (!emit())
      return ParserResultCancelled;
  }

  state->ptr = p;
  return ParserResultOk;
}

// The chunked-body machine shared by the response and request parsers: data
// of the current chunk (emitted as it arrives), the CRLF closing a non-first
// chunk, the size line of the next one, and the zero chunk with its trailers.
// The parsers differ only in the emitted component constants and in the
// cancellation contract, so both variations live in the emit callables (a
// false return becomes ParserResultCancelled); lastState is the parser's
// terminal state, stored once the zero chunk is consumed. The non-chunked
// body branches genuinely differ between the parsers and stay with them.
template<typename State, typename StateValue, typename EmitFragment, typename EmitLast>
static ParserResultTy parseChunkedBody(State *state, StateValue lastState, EmitFragment emitFragment, EmitLast emitLast)
{
  ParserResultTy result;
  const char *p = state->ptr;

  for (;;) {
    if (state->dataRemaining) {
      bool needMoreData = false;
      const char *readyChunk = p;
      size_t readyChunkSize;
      if (canRead(p, state->end, state->dataRemaining)) {
        readyChunkSize = state->dataRemaining;
        p += state->dataRemaining;
        state->dataRemaining = 0;
        state->firstFragment = false;
      } else {
        readyChunkSize = std::min(state->dataRemaining, static_cast<size_t>(state->end - p));
        p += readyChunkSize;
        state->dataRemaining -= readyChunkSize;
        needMoreData = true;
      }

      if (readyChunkSize) {
        if (!emitFragment(readyChunk, readyChunkSize))
          return ParserResultCancelled;
        state->ptr = p;
      }

      if (needMoreData)
        return ParserResultNeedMoreData;
    } else {
      // we at begin of next chunk
      if (!state->firstFragment) {
        // skip CRLF for non-first chunk
        if (!canRead(p, state->end, 2))
          return ParserResultNeedMoreData;
        p += 2;
      }

      size_t chunkSize;
      if ((result = readHex(&p, state->end, &chunkSize)) != ParserResultOk)
        return result;

      if (chunkSize == 0) {
        if ((result = skipTrailers(&p, state->end)) != ParserResultOk)
          return result;
        if (!emitLast(p))
          return ParserResultCancelled;
        state->state = lastState;
        state->ptr = p;
        return ParserResultOk;
      }

      state->dataRemaining = chunkSize;
    }
  }
}
