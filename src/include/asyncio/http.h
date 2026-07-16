#ifndef __ASYNCIO_HTTP_H_
#define __ASYNCIO_HTTP_H_

#ifdef __cplusplus
#include <string>
#endif

#ifdef __cplusplus
extern "C" {
#endif 
  
#include "asyncio/socketSSL.h"
#include "p2putils/HttpParse.h"
#include "asyncio/dynamicBuffer.h"
  
typedef struct HTTPClient HTTPClient;
typedef struct HTTPOp HTTPOp;

typedef void httpConnectCb(AsyncOpStatus, HTTPClient*, void*);
typedef void httpRequestCb(AsyncOpStatus, HTTPClient*, void*);

typedef struct HTTPClient {
  aioObjectRoot root;
  int isHttps;
  union {
    aioObject *plainSocket;
    SSLSocket *sslSocket;
  };
 
  uint8_t *inBuffer;
  size_t inBufferSize;
  size_t inBufferOffset;
  size_t requestBytesSent;
  HttpParserState state;
} HTTPClient;


typedef struct HTTPOp {
  asyncOpRoot root;
  int state;
  HostAddress address;
  httpParseCb *parseCallback;
  void *parseArg;
  uint8_t *internalBuffer;
  size_t internalBufferSize;
  size_t dataSize;
} HTTPOp;

typedef struct HTTPParseDefaultContext {
  unsigned resultCode;
  Raw contentType;
  Raw body;
  dynamicBuffer buffer;
  size_t contentTypeOffset;
  size_t bodyOffset;
} HTTPParseDefaultContext;


void httpParseDefaultInit(HTTPParseDefaultContext *context);
void httpParseDefault(HttpComponent *component, void *arg);

HTTPClient *httpClientNew(asyncBase *base, aioObject *socket);
HTTPClient *httpsClientNew(asyncBase *base, SSLSocket *socket);
void httpClientDelete(HTTPClient *client);

void aioHttpConnect(HTTPClient *client,
                    const HostAddress *address,
                    const char *tlsextHostName,
                    uint64_t usTimeout,
                    httpConnectCb callback,
                    void *arg);

void aioHttpRequest(HTTPClient *client,
                    const char *request,
                    size_t requestSize,
                    uint64_t usTimeout,
                    httpParseCb parseCallback,
                    void *parseArg,
                    httpRequestCb callback,
                    void *arg);

int ioHttpConnect(HTTPClient *client, const HostAddress *address, const char *tlsextHostName, uint64_t usTimeout);
AsyncOpStatus ioHttpRequest(HTTPClient *client, const char *request, size_t requestSize, uint64_t usTimeout, httpParseCb parseCallback, void *parseArg);
                

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus


#endif

#endif //__ASYNCIO_HTTP_H_
