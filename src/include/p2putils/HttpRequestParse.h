#ifndef __LIBP2P_HTTPREQUESTPARSE_H_
#define __LIBP2P_HTTPREQUESTPARSE_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "HttpParseCommon.h"
#include "CommonParse.h"
#include "HttpHeaderTable.h"
#include <stddef.h>
#include <stdint.h>

typedef enum HttpRequestParserStateTy {
  httpRequestMethod = 0,
  httpRequestUriPath,
  httpRequestUriQuery,
  httpRequestUriFragment,
  httpRequestVersion,
  httpRequestHeader,
  httpRequestBody,
  httpRequestStLast,
  httpRequestTrailer
} HttpRequestParserStateTy;

typedef enum HttpRequestParserDataTy {
  httpRequestDtInitialize = 0,
  httpRequestDtMethod,
  httpRequestDtUriPathElement,
  httpRequestDtUriQueryElement,
  httpRequestDtUriFragment,
  httpRequestDtVersion,
  httpRequestDtHeaderEntry,
  httpRequestDtData,
  httpRequestDtDataLast,
} HttpRequestParserDataTy;

typedef struct HttpRequestParserState {
  HttpRequestParserStateTy state;
  uint8_t chunked;
  uint8_t firstFragment;
  uint8_t seenContentLength;
  uint8_t seenTransferEncoding;
  const char *ptr;
  const char *end;
  size_t dataRemaining;
} HttpRequestParserState;

typedef struct HttpRequestComponent {
  HttpRequestParserDataTy type;
  union {
    int method;
    Raw data;

    // Version
    struct {
      unsigned majorVersion;
      unsigned minorVersion;
    } version;

    // Header
    struct {
      int entryType;    // id from the parse-call table / reserved hh* id / 0
      Raw entryName;    // the name as it appears in the message
      Raw stringValue;  // raw value with the OWS trimmed, always filled
      size_t sizeValue; // parsed number, valid only for hhContentLength
    } header;
  };
  Raw data2;
} HttpRequestComponent;

typedef int httpRequestParseCb(HttpRequestComponent *component, void *arg);

void httpRequestParserInit(HttpRequestParserState *state);
void httpRequestSetBuffer(HttpRequestParserState *state, const void *buffer, size_t size);
// table types the header names of the message (NULL = the built-in table of
// the reserved names only); it is read for the duration of the call
ParserResultTy httpRequestParse(HttpRequestParserState *state, const HttpHeaderTable *table,
                                httpRequestParseCb callback, void *arg);

const void *httpRequestDataPtr(HttpRequestParserState *state);
size_t httpRequestDataRemaining(HttpRequestParserState *state);

#ifdef __cplusplus
}
#endif

#endif //__LIBP2P_HTTPREQUESTPARSE_H_
