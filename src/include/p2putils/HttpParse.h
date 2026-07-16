#ifndef __LIBP2P_HTTPPARSE_H_
#define __LIBP2P_HTTPPARSE_H_

#ifdef __cplusplus
extern "C" {
#endif 

#include <stddef.h>
#include <stdint.h>
#include "CommonParse.h"
#include "HttpParseCommon.h"
#include "HttpHeaderTable.h"

typedef enum HttpParserStateTy {
  httpStStartLine = 0,
  httpStHeader,  
  httpStBody,
  httpStLast
} HttpParserStateTy;

typedef enum HttpParserDataTy {
  httpDtInitialize = 0,
  httpDtStartLine,
  httpDtHeaderEntry,
  httpDtData,
  httpDtDataFragment,
  httpDtFinalize
} HttpParserDataTy;

typedef struct HttpParserState {
  HttpParserStateTy state;
  const char *buffer;
  const char *ptr;
  const char *end;
  int chunked;
  int firstFragment;
  size_t dataRemaining;
} HttpParserState;

typedef struct HttpComponent {
  int type;
  union {
    // Start line
    struct {
      unsigned majorVersion;
      unsigned minorVersion;
      unsigned code;
      Raw description;
    } startLine;
    
    // Header
    struct {
      int entryType;    // id from the parse-call table / reserved hh* id / 0
      Raw entryName;    // the name as it appears in the message
      Raw stringValue;  // raw value with the OWS trimmed, always filled
      size_t sizeValue; // parsed number, valid only for hhContentLength
    } header;

    Raw data;
  };
} HttpComponent;

typedef void httpParseCb(HttpComponent *component, void *arg);

void httpInit(HttpParserState *state);
void httpSetBuffer(HttpParserState *state, const void *buffer, size_t size);
// table types the header names of the message (NULL = the built-in table of
// the reserved names only); it is read for the duration of the call
ParserResultTy httpParse(HttpParserState *state, const HttpHeaderTable *table,
                         httpParseCb callback, void *arg);

const void *httpDataPtr(HttpParserState *state);
size_t httpDataRemaining(HttpParserState *state);

#ifdef __cplusplus
}
#endif

#endif //__LIBP2P_HTTPPARSE_H_
