#include "p2putils/HttpParse.h"
#include "HttpParseUtils.h"
#include "HttpTokens.h"
#include <algorithm>

static ParserResultTy httpParseStartLine(HttpParserState *state, httpParseCb callback, void *arg)
{
  const char prefix[] = "HTTP/";

  ParserResultTy result;
  HttpComponent component;
  const char *ptr = state->ptr;
  if (!canRead(ptr, state->end, sizeof(prefix)-1 + 3))
    return ParserResultNeedMoreData;

  // HTTP protocol version
  if (compareUnchecked(&ptr, prefix, sizeof(prefix)-1) != ParserResultOk)
    return ParserResultError;
  if ( !(isDigit(ptr[0]) && ptr[1] == '.' && isDigit(ptr[2])) )
    return ParserResultError;
  component.startLine.majorVersion = static_cast<unsigned char>(ptr[0]-'0');
  component.startLine.minorVersion = static_cast<unsigned char>(ptr[2]-'0');
  ptr += 3;

  // HTTP response code
  if ( (result = skipSPCharacters(&ptr, state->end)) != ParserResultOk )
    return result;
  if (!canRead(ptr, state->end, 3))
    return ParserResultNeedMoreData;
  if ( !(isDigit(ptr[0]) && isDigit(ptr[1]) && isDigit(ptr[2])) )
    return ParserResultError;
  component.startLine.code =
      100u*static_cast<unsigned char>(ptr[0]-'0') +
      10u*static_cast<unsigned char>(ptr[1]-'0') +
      static_cast<unsigned char>(ptr[2]-'0');
  ptr += 3;

  // associated textual phrase (optional)
  if (ptr == state->end)
    return ParserResultNeedMoreData;
  if (*ptr == ' ')
    skipOWS(&ptr, state->end);
  else if (*ptr != '\r')
    return ParserResultError;
  component.startLine.description.data = ptr;
  if ( (result = readUntilCRLF(&ptr, state->end)) != ParserResultOk )
    return result;
  component.startLine.description.size = static_cast<size_t>(ptr-component.startLine.description.data-2);
  component.type = httpDtStartLine;
  callback(&component, arg);

  state->ptr = ptr;
  return ParserResultOk;
}

void httpInit(HttpParserState *state)
{
  state->state = httpStStartLine;
  state->chunked = false;
  state->dataRemaining = 0;
  state->firstFragment = true;
}

void httpSetBuffer(HttpParserState *state, const void *buffer, size_t size)
{
  state->ptr = state->buffer = static_cast<const char*>(buffer);
  state->end = state->buffer + size;
}

ParserResultTy httpParse(HttpParserState *state, httpParseCb callback, void *arg)
{
  ParserResultTy result;
  HttpComponent component;

  if (state->state == httpStStartLine) {
    if ( (result = httpParseStartLine(state, callback, arg)) != ParserResultOk)
      return result;
    state->state = httpStHeader;
  }

  if (state->state == httpStHeader) {
    for (;;) {
      if (!canRead(state->ptr, state->end, 2))
        return ParserResultNeedMoreData;

      if (state->ptr[0] == '\r' && state->ptr[1] == '\n') {
        state->ptr += 2;
        state->state = httpStBody;
        break;
      }

      int token;
      const char *p = state->ptr;
      if ( (result = httpHeaderNameLookup(&p, state->end, &token)) != ParserResultOk )
        return result;

      component.type = httpDtHeaderEntry;
      component.header.entryType = token;
      component.header.entryName.data = state->ptr;
      component.header.entryName.size = static_cast<size_t>(p - state->ptr);
      ++p;
      skipOWS(&p, state->end);

      if (token == hhContentLength) {
        size_t contentSize;
        if ( (result = readDec(&p, state->end, &contentSize)) != ParserResultOk )
          return result;
        component.header.sizeValue = contentSize;
        state->dataRemaining = contentSize;
        callback(&component, arg);
      } else {
        component.header.stringValue.data = p;
        if ( (result = readUntilCRLF(&p, state->end)) != ParserResultOk )
          return result;
        component.header.stringValue.size = static_cast<size_t>(p-component.header.stringValue.data-2);
        trimTrailingOWS(&component.header.stringValue);
        if (token == hhTransferEncoding)
          state->chunked = isChunkedTransferEncoding(&component.header.stringValue);
        callback(&component, arg);
      }

      state->ptr = p;
    }
  }

  if (state->state == httpStBody) {
    if (state->chunked) {
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
            component.type = httpDtDataFragment;
            component.data.data = readyChunk;
            component.data.size = readyChunkSize;
            callback(&component, arg);
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
          if ( (result = readHex(&p, state->end, &chunkSize)) != ParserResultOk )
            return result;

          if (chunkSize == 0) {
            if ( (result = skipTrailers(&p, state->end)) != ParserResultOk )
              return result;
            component.type = httpDtData;
            component.data.data = p;
            component.data.size = 0;
            callback(&component, arg);
            state->state = httpStLast;
            state->ptr = p;
            break;
          } else {
            state->dataRemaining = chunkSize;
          }
        }
      }
    } else {
      const char *p = state->ptr;
      if (canRead(p, state->end, state->dataRemaining)) {
        component.type = state->firstFragment ? httpDtData : httpDtDataFragment;
        component.data.data = p;
        component.data.size = state->dataRemaining;
        callback(&component, arg);
        state->ptr = p + state->dataRemaining;
        state->state = httpStLast;
      } else {
        if (p != state->end) {
          size_t size = std::min(state->dataRemaining, static_cast<size_t>(state->end - p));
          component.type = httpDtDataFragment;
          component.data.data = p;
          component.data.size = size;
          callback(&component, arg);
          state->ptr = p + size;
          state->firstFragment = false;
          state->dataRemaining -= size;
        }
        return ParserResultNeedMoreData;
      }
    }
  }

  return ParserResultOk;
}

const void *httpDataPtr(HttpParserState *state)
{
  return state->ptr;
}

size_t httpDataRemaining(HttpParserState *state)
{
  return static_cast<size_t>(state->end - state->ptr);
}
