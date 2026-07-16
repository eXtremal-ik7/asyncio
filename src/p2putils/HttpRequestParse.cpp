#include "p2putils/HttpRequestParse.h"
#include "p2putils/uriParse.h"
#include "HttpParseUtils.h"
#include "HttpTokens.h"
#include <algorithm>

struct UriArg {
  httpRequestParseCb *callback;
  void *arg;
};

void httpRequestParserInit(HttpRequestParserState *state)
{
  state->buffer = nullptr;
  state->end = nullptr;
  state->ptr = nullptr;
  state->state = httpRequestMethod;
  state->haveBody = 0;
  state->chunked = 0;
  state->dataRemaining = 0;
  state->firstFragment = true;
}

void httpRequestSetBuffer(HttpRequestParserState *state, const void *buffer, size_t size)
{
  state->ptr = state->buffer = static_cast<const char*>(buffer);
  state->end = state->buffer + size;
}

ParserResultTy httpRequestParse(HttpRequestParserState *state, httpRequestParseCb callback, void *arg)
{
  ParserResultTy localResult;
  HttpRequestComponent component;
  UriArg uriArg;
  uriArg.callback = callback;
  uriArg.arg = arg;

  if (state->state == httpRequestMethod) {
    int token;
    if ((localResult = httpMethodLookup(&state->ptr, state->end, &token)) != ParserResultOk)
      return localResult;

    component.type = httpRequestDtMethod;
    component.method = token;
    if (!callback(&component, arg))
      return ParserResultCancelled;

    state->state = httpRequestUriPath;
  }

  if (state->state == httpRequestUriPath) {
    if ((localResult = skipSPCharacters(&state->ptr, state->end)) != ParserResultOk)
      return localResult;

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
    state->state = httpRequestUriQueryBegin;
  }

  if (state->state == httpRequestUriQueryBegin) {
    if (state->ptr == state->end)
      return ParserResultNeedMoreData;

    if (*state->ptr == '?') {
      state->state = httpRequestUriQuery;
      state->ptr++;
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
      if (localResult != ParserResultOk)
        return localResult;
    }

    state->state = httpRequestVersion;
  }

  if (state->state == httpRequestVersion) {
    if ((localResult = skipSPCharacters(&state->ptr, state->end)) != ParserResultOk)
      return localResult;

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
    for (;;) {
      if (!canRead(state->ptr, state->end, 2))
        return ParserResultNeedMoreData;

      if (state->ptr[0] == '\r' && state->ptr[1] == '\n') {
        state->ptr += 2;
        // RFC 9110: a request has a body when Content-Length or a chunked
        // Transfer-Encoding is present, whatever the method is
        state->haveBody = state->chunked || state->dataRemaining != 0;
        if (state->haveBody) {
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
      localResult = parseHeaderLine(state, &component, [&]() {
        return callback(&component, arg) != 0;
      });
      if (localResult != ParserResultOk)
        return localResult;
    }
  }

  if (state->state == httpRequestBody) {
    if (state->chunked) {
      localResult = parseChunkedBody(state, httpRequestStLast,
        [&](const char *data, size_t size) {
          component.type = httpRequestDtData;
          component.data.data = data;
          component.data.size = size;
          return callback(&component, arg) != 0;
        },
        [&](const char *trailersEnd) {
          component.type = httpRequestDtDataLast;
          component.data.data = trailersEnd;
          component.data.size = 0;
          return callback(&component, arg) != 0;
        });
      if (localResult != ParserResultOk)
        return localResult;
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
          size_t size = std::min(state->dataRemaining, static_cast<size_t>(state->end - p));
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

  return ParserResultOk;
}

const void *httpRequestDataPtr(HttpRequestParserState *state)
{
  return state->ptr;
}

size_t httpRequestDataRemaining(HttpRequestParserState *state)
{
  return static_cast<size_t>(state->end - state->ptr);
}
