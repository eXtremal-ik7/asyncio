#ifdef __cplusplus
extern "C" {
#endif

#ifndef __ASYNCIO_SOCKETSSL_H_
#define __ASYNCIO_SOCKETSSL_H_

#include "asyncio/api.h"
#include "openssl/bio.h"

typedef struct SSLOp SSLOp;
typedef struct SSLSocket SSLSocket;

typedef void sslConnectCb(AsyncOpStatus status, SSLSocket *object, void *arg);
typedef void sslCb(AsyncOpStatus status, SSLSocket *object, size_t transferred, void *arg);

typedef struct SSLSocket {
  aioObjectRoot root;

  aioObject *object;
  SSL_CTX *sslContext;
  SSL *ssl;
  BIO *bioIn;
  BIO *bioOut;
  size_t sslReadBufferSize;
  uint8_t *sslReadBuffer;
  size_t sslWriteBufferSize;
  uint8_t *sslWriteBuffer;
} SSLSocket;

typedef struct SSLOp {
  asyncOpRoot root;
  HostAddress address;
  int state;    
  void *buffer;
  size_t transactionSize;
  size_t bytesTransferred;  
  void *internalBuffer;
  size_t internalBufferSize;
} SSLOp;


SSLSocket *sslSocketNew(asyncBase *base, aioObject *socket);
void sslSocketDelete(SSLSocket *socket);

socketTy sslGetSocket(const SSLSocket *socket);

asyncOpRoot *implSslRead(SSLSocket *socket,
                         void *buffer,
                         size_t size,
                         AsyncFlags flags,
                         uint64_t usTimeout,
                         sslCb callback,
                         void *arg,
                         size_t *bytesTransferred);

asyncOpRoot *implSslWrite(SSLSocket *socket,
                          const void *buffer,
                          size_t size,
                          AsyncFlags flags,
                          uint64_t usTimeout,
                          sslCb callback,
                          void *arg);

void aioSslConnect(SSLSocket *socket,
                   const HostAddress *address,
                   const char *tlsextHostName,
                   uint64_t usTimeout,
                   sslConnectCb callback,
                   void *arg);

ssize_t aioSslRead(SSLSocket *socket,
                   void *buffer,
                   size_t size,
                   AsyncFlags flags,
                   uint64_t usTimeout,
                   sslCb callback,
                   void *arg);

ssize_t aioSslWrite(SSLSocket *socket,
                    const void *buffer,
                    size_t size,
                    AsyncFlags flags,
                    uint64_t usTimeout,
                    sslCb callback,
                    void *arg);


int ioSslConnect(SSLSocket *socket, const HostAddress *address, const char *tlsextHostName, uint64_t usTimeout);
ssize_t ioSslRead(SSLSocket *socket, void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);
ssize_t ioSslWrite(SSLSocket *socket, const void *buffer, size_t size, AsyncFlags flags, uint64_t usTimeout);

#endif //__ASYNCIO_SOCKETSSL_H_

#ifdef __cplusplus
}
#endif
