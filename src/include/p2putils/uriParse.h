#ifndef __LIBP2P_URIPARSE_H_
#define __LIBP2P_URIPARSE_H_

#include "p2putils/CommonParse.h"
#include <stddef.h>
#include <stdint.h>

#include <string>

// Low-level interface

enum uriComponentTy {
  uriCtSchema = 0,
  uriCtUserInfo,
  uriCtHostIPv4,
  uriCtHostIPv6,
  uriCtHostDNS,
  uriCtPort,
  uriCtPathElement,
  uriCtPath,
  uriCtQueryElement,
  uriCtQuery,
  uriCtFragment
};


struct URIComponent {
  int type;
  union {
    struct {
      const char *data;
      size_t size;
    } raw;
    int32_t i32;
    uint32_t u32;
    uint16_t ipv6[8];   // groups in written order, host byte order
  };
  struct {
    const char *data;
    size_t size;
  } raw2;    
};

typedef int uriParseCb(URIComponent *component, void *arg);

ParserResultTy uriParsePath(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg);
ParserResultTy uriParseQuery(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg);
ParserResultTy uriParseFragment(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg);

int uriParse(const char *uri, uriParseCb callback, void *arg);

// host[:port] without scheme/userinfo/path; the whole string must match.
// uriCtPort is emitted only when the port is present
int uriParseHostPort(const char *hostport, uriParseCb callback, void *arg);

// High-level interface

struct URI {
public:
  enum {
    HostTypeNone = 0,
    HostTypeIPv4,
    HostTypeIPv6,
    HostTypeDNS
  };
  
  std::string schema;
  std::string userInfo;
  
  int hostType;
  union {
    uint32_t ipv4;
    uint16_t ipv6[8];
  };
  
  std::string domain;
  int port;   // -1 = no explicit port; build() emits the port only when >= 0
  
  std::string path;
  std::string query;
  std::string fragment;
  
public:
  void build(std::string &out);
};

int uriParse(const char *uri, URI *data);

// Absent port → defaultPort; an explicit port (including ":0") wins
int uriParseHostPort(const char *hostport, URI *data, uint16_t defaultPort);

#endif //__LIBP2P_URIPARSE_H_
