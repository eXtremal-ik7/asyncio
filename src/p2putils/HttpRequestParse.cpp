#include "p2putils/HttpRequestParse.h"
#include "p2putils/uriParse.h"
#include "HttpParseUtils.h"
#include "HttpTokens.h"

struct UriArg {
  httpRequestParseCb *callback;
  void *arg;
};

void httpRequestParserInit(HttpRequestParserState *state)
{
  state->end = nullptr;
  state->ptr = nullptr;
  state->state = httpRequestMethod;
  state->chunked = 0;
  state->dataRemaining = 0;
  state->firstFragment = true;
  state->seenContentLength = 0;
  state->seenTransferEncoding = 0;
}

void httpRequestSetBuffer(HttpRequestParserState *state, const void *buffer, size_t size)
{
  state->ptr = static_cast<const char*>(buffer);
  state->end = state->ptr + size;
}

static __NOINLINE ParserResultTy
httpRequestParseBody(HttpRequestParserState *state,
                     httpRequestParseCb callback, void *arg)
{
  ParserResultTy result;
  HttpRequestComponent component;

  if (state->state == httpRequestBody) {
    if (state->chunked) {
      result = parseChunkedBody(state, httpRequestTrailer,
        [&](const char *data, size_t size) {
          component.type = httpRequestDtData;
          component.data.data = data;
          component.data.size = size;
          return callback(&component, arg) != 0;
        });
      if (result != ParserResultOk)
        return result;
    } else {
      const char *p = state->ptr;
      if (canRead(p, state->end, state->dataRemaining)) {
        component.type = httpRequestDtDataLast;
        component.data.data = p;
        component.data.size = state->dataRemaining;
        if (!callback(&component, arg))
          return ParserResultCancelled;
        state->ptr = p + state->dataRemaining;
        state->state = httpRequestStLast;
      } else {
        if (p != state->end) {
          size_t size = static_cast<size_t>(state->end - p);
          component.type = httpRequestDtData;
          component.data.data = p;
          component.data.size = size;
          if (!callback(&component, arg))
            return ParserResultCancelled;
          state->ptr = p + size;
          state->dataRemaining -= size;
        }
        return ParserResultNeedMoreData;
      }
    }
  }

  if (state->state == httpRequestTrailer) {
    const char *trailersEnd;
    result = parseTrailers(&state->ptr, state->end, &trailersEnd);
    if (result != ParserResultOk)
      return result;

    component.type = httpRequestDtDataLast;
    component.data.data = trailersEnd;
    component.data.size = 0;
    if (!callback(&component, arg))
      return ParserResultCancelled;
    state->ptr = trailersEnd;
    state->state = httpRequestStLast;
  }

  return ParserResultOk;
}

static __NOINLINE ParserResultTy
httpRequestParseHead(HttpRequestParserState *state,
                     const HttpHeaderTable *table,
                     httpRequestParseCb callback, void *arg)
{
  ParserResultTy localResult;
  HttpRequestComponent component;

  if (state->state == httpRequestMethod) {
    int token;
    if ((localResult = httpMethodLookup(&state->ptr, state->end, &token)) != ParserResultOk)
      return localResult;

    component.type = httpRequestDtMethod;
    component.method = token;
    if (!callback(&component, arg))
      return ParserResultCancelled;

    // Consume the method/path separator exactly once. Once the parser is in
    // httpRequestUriPath, state->ptr is the retained path tail and a resumed
    // buffer starts directly with path data.
    if ((localResult = skipSPCharacters(&state->ptr, state->end)) != ParserResultOk)
      return localResult;
    state->state = httpRequestUriPath;
  }

  if (state->state >= httpRequestUriPath &&
      state->state <= httpRequestUriFragment) {
    UriArg uriArg = {callback, arg};

    if (state->state == httpRequestUriPath) {
      // skipSPCharacters may have reached the previous buffer boundary in the
      // middle of a lenient multi-SP separator
      while (state->ptr != state->end && *state->ptr == ' ')
        state->ptr++;

      // Parse URI path
      localResult = uriParsePath(&state->ptr, state->end, false, [](URIComponent *source, void *arg) {
        HttpRequestComponent component;
        UriArg *uriArg = static_cast<UriArg*>(arg);
        if (source->type == uriCtPathElement) {
          component.type = httpRequestDtUriPathElement;
          component.data.data = source->raw.data;
          component.data.size = source->raw.size;
          return uriArg->callback(&component, uriArg->arg);
        } else {
          return 1;
        }
      }, &uriArg);
      if (localResult != ParserResultOk)
        return localResult;

      // uriParsePath(..., false) succeeds only once it sees the request-target
      // delimiter, so the normal path can choose the next phase immediately.
      if (state->ptr == state->end)
        return ParserResultNeedMoreData;
      if (*state->ptr == '?') {
        state->ptr++;
        state->state = httpRequestUriQuery;
      } else {
        state->state = httpRequestUriFragment;
      }
    }

    // Parse URI query
    if (state->state == httpRequestUriQuery) {
      localResult = uriParseQuery(&state->ptr, state->end, false, [](URIComponent *source, void *arg) {
        HttpRequestComponent component;
        UriArg *uriArg = static_cast<UriArg*>(arg);
        if (source->type == uriCtQueryElement) {
          component.type = httpRequestDtUriQueryElement;
          component.data.data = source->raw.data;
          component.data.size = source->raw.size;
          component.data2.data = source->raw2.data;
          component.data2.size = source->raw2.size;
          return uriArg->callback(&component, uriArg->arg);
        } else {
          return 1;
        }
      }, &uriArg);
      if (localResult != ParserResultOk)
        return localResult;
      state->state = httpRequestUriFragment;
    }

    // Parse URI fragment
    if (state->state == httpRequestUriFragment) {
      if (state->ptr == state->end)
        return ParserResultNeedMoreData;

      if (*state->ptr == '#') {
        const char *fragmentMarker = state->ptr;
        state->ptr++;
        localResult = uriParseFragment(&state->ptr, state->end, false, [](URIComponent *source, void *arg) {
          HttpRequestComponent component;
          UriArg *uriArg = static_cast<UriArg*>(arg);
          if (source->type == uriCtFragment) {
            component.type = httpRequestDtUriFragment;
            component.data.data = source->raw.data;
            component.data.size = source->raw.size;
            return uriArg->callback(&component, uriArg->arg);
          } else {
            return 1;
          }
        }, &uriArg);
        if (localResult != ParserResultOk) {
          // Fragment parsing does not emit or consume anything before it knows
          // the terminating SP. Retain the marker as well so this state can
          // consume it again after the caller appends more bytes.
          if (localResult == ParserResultNeedMoreData)
            state->ptr = fragmentMarker;
          return localResult;
        }
      }

      // As with the method separator, consume this once before entering the
      // resumable version state.
      if ((localResult = skipSPCharacters(&state->ptr, state->end)) != ParserResultOk)
        return localResult;
      state->state = httpRequestVersion;
    }
  }

  if (state->state == httpRequestVersion) {
    // The request-target/version separator can likewise span buffers.
    while (state->ptr != state->end && *state->ptr == ' ')
      state->ptr++;

    const char version[] = "HTTP/";
    if (!canRead(state->ptr, state->end, sizeof(version)-1 + 3 + 2))
      return ParserResultNeedMoreData;

    if ( (localResult = compareUnchecked(&state->ptr, version, sizeof(version)-1)) != ParserResultOk )
      return localResult;
    if ( !(isDigit(state->ptr[0]) && state->ptr[1] == '.' && isDigit(state->ptr[2]) && state->ptr[3] == '\r' && state->ptr[4] == '\n') )
      return ParserResultError;

    component.type = httpRequestDtVersion;
    component.version.majorVersion = static_cast<unsigned char>(state->ptr[0]-'0');
    component.version.minorVersion = static_cast<unsigned char>(state->ptr[2]-'0');
    if (!callback(&component, arg))
      return ParserResultCancelled;
    state->ptr += 5;
    state->state = httpRequestHeader;
  }

  if (state->state == httpRequestHeader) {
    if (!table)
      table = &httpHeaderDefaultTable;
    for (;;) {
      if (!canRead(state->ptr, state->end, 2))
        return ParserResultNeedMoreData;

      if (state->ptr[0] == '\r' && state->ptr[1] == '\n') {
        state->ptr += 2;
        // Transfer-Encoding without a final chunked leaves the request length
        // undeterminable (RFC 9112: reject with 400)
        if (state->seenTransferEncoding && !state->chunked)
          return ParserResultError;
        // RFC 9110: a request has a body when Content-Length or a chunked
        // Transfer-Encoding is present, whatever the method is
        if (state->chunked || state->dataRemaining != 0) {
          state->state = httpRequestBody;
        } else {
          component.type = httpRequestDtDataLast;
          component.data.data = nullptr;
          component.data.size = 0;
          if (!callback(&component, arg))
            return ParserResultCancelled;
          state->state = httpRequestStLast;
        }
        break;
      }

      component.type = httpRequestDtHeaderEntry;
      localResult = parseHeaderLine(state, table, &component, [&]() {
        return callback(&component, arg) != 0;
      });
      if (localResult != ParserResultOk)
        return localResult;
    }
  }

  if (state->state == httpRequestStLast)
    return ParserResultOk;
  return httpRequestParseBody(state, callback, arg);
}

ParserResultTy httpRequestParse(HttpRequestParserState *state,
                                const HttpHeaderTable *table,
                                httpRequestParseCb callback, void *arg)
{
  if (state->state == httpRequestStLast)
    return ParserResultOk;
  if (state->state >= httpRequestBody)
    return httpRequestParseBody(state, callback, arg);
  return httpRequestParseHead(state, table, callback, arg);
}

const void *httpRequestDataPtr(HttpRequestParserState *state)
{
  return state->ptr;
}

size_t httpRequestDataRemaining(HttpRequestParserState *state)
{
  return static_cast<size_t>(state->end - state->ptr);
}
