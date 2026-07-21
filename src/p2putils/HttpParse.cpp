#include "p2putils/HttpParse.h"
#include "HttpParseUtils.h"

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
  state->ptr = nullptr;
  state->end = nullptr;
  state->chunked = false;
  state->dataRemaining = 0;
  state->firstFragment = true;
  state->seenContentLength = 0;
  state->seenTransferEncoding = 0;
}

void httpSetBuffer(HttpParserState *state, const void *buffer, size_t size)
{
  state->ptr = static_cast<const char*>(buffer);
  state->end = state->ptr + size;
}

static __NOINLINE ParserResultTy
httpParseBody(HttpParserState *state, httpParseCb callback, void *arg)
{
  ParserResultTy result;
  HttpComponent component;

  if (state->state == httpStBody) {
    if (state->chunked) {
      result = parseChunkedBody(state, httpStTrailer,
        [&](const char *data, size_t size) {
          component.type = httpDtDataFragment;
          component.data.data = data;
          component.data.size = size;
          callback(&component, arg);
          return true;
        });
      if (result != ParserResultOk)
        return result;
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
          size_t size = static_cast<size_t>(state->end - p);
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

  if (state->state == httpStTrailer) {
    const char *trailersEnd;
    result = parseTrailers(&state->ptr, state->end, &trailersEnd);
    if (result != ParserResultOk)
      return result;

    component.type = httpDtData;
    component.data.data = trailersEnd;
    component.data.size = 0;
    callback(&component, arg);
    state->ptr = trailersEnd;
    state->state = httpStLast;
  }

  return ParserResultOk;
}

static __NOINLINE ParserResultTy
httpParseHead(HttpParserState *state, const HttpHeaderTable *table,
              httpParseCb callback, void *arg)
{
  ParserResultTy result;
  HttpComponent component;

  if (state->state == httpStStartLine) {
    if ( (result = httpParseStartLine(state, callback, arg)) != ParserResultOk)
      return result;
    state->state = httpStHeader;
  }

  if (state->state == httpStHeader) {
    if (!table)
      table = &httpHeaderDefaultTable;
    for (;;) {
      if (!canRead(state->ptr, state->end, 2))
        return ParserResultNeedMoreData;

      if (state->ptr[0] == '\r' && state->ptr[1] == '\n') {
        state->ptr += 2;
        // Transfer-Encoding without a final chunked means read-until-close
        // framing, which this parser does not have; the old silent zero-length
        // framing desynchronized the connection
        if (state->seenTransferEncoding && !state->chunked)
          return ParserResultError;
        state->state = httpStBody;
        break;
      }

      component.type = httpDtHeaderEntry;
      result = parseHeaderLine(state, table, &component, [&]() {
        callback(&component, arg);
        return true;
      });
      if (result != ParserResultOk)
        return result;
    }
  }

  return httpParseBody(state, callback, arg);
}

ParserResultTy httpParse(HttpParserState *state, const HttpHeaderTable *table,
                         httpParseCb callback, void *arg)
{
  if (state->state == httpStLast)
    return ParserResultOk;
  if (state->state >= httpStBody)
    return httpParseBody(state, callback, arg);
  return httpParseHead(state, table, callback, arg);
}

const void *httpDataPtr(HttpParserState *state)
{
  return state->ptr;
}

size_t httpDataRemaining(HttpParserState *state)
{
  return static_cast<size_t>(state->end - state->ptr);
}
