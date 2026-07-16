#include "asyncioconfig.h"
#include "p2putils/uriParse.h"
#include "p2putils/strExtras.h"
#include <string.h>

static int isLetter(char s)
{
  return (s >= 'A' && s <= 'Z') || (s >= 'a' && s <= 'z');
}

static int isDigit(char s)
{
  return (s >= '0' && s <= '9');
}

static int isHexDigit(char s)
{
  return ( (s >= '0' && s <= '9') ||
         (s >= 'A' && s <= 'F') ||
         (s >= 'a' && s <= 'f') );
}

static int isSubDelims(char s)
{
  return s == '!' ||
         s == '$' ||
         s == '&' ||
         s == '\'' ||
         s == '(' ||
         s == ')' ||
         s == '*' ||
         s == '+' ||
         s == ',' ||
         s == ';' ||
         s == '=';
}

static int isUnreserved(char s)
{
  return isLetter(s) ||
         isDigit(s) ||
         s == '-' ||
         s == '.' ||
         s == '_' ||
         s == '~';
}

static int isPctEncoded(const char **ptr)
{
  const char *p = *ptr;
  if (*p == '%' && isHexDigit(*(p+1)) && isHexDigit(*(p+2))) {
    *ptr += 3;
    return 1;
  } else {
    return 0;
  }
}

static int isPChar(const char **ptr)
{
  const char *p = *ptr;
  if (isUnreserved(*p) || isSubDelims(*p) || *p == ':' || *p == '@') {
    *ptr = p+1;
    return 1;
  } else if (isPctEncoded(ptr)) {
    return 1;
  } else {
    return 0;
  } 
}


static int uriParseScheme(const char **ptr, uriParseCb callback, void *arg)
{
  const char *p = *ptr;
  if (isLetter(*p)) {
    for (;;) {
      char s = *p++;
      if (isLetter(s) || isDigit(s) || s == '+' || s == '-' || s == '.')
        continue;
      else {
        p--;
        URIComponent component;
        component.type = uriCtSchema;
        component.raw.data = *ptr;
        component.raw.size = static_cast<size_t>(p - *ptr);
        callback(&component, arg);
        *ptr = p;
        return 1;
      }
    }
  }
  
  return 0;
}

static char decodeHex(char s);

// RFC 3986 IPv6address (IPvFuture not supported); *ptr points just past '[',
// on success it is moved past ']'
static int uriParseIpLiteral(const char **ptr, uriParseCb callback, void *arg)
{
  const char *p = *ptr;
  uint16_t groups[8];
  uint16_t tail[8];
  int headCount = 0;
  int tailCount = 0;
  int compression = 0;

  memset(groups, 0, sizeof(groups));
  memset(tail, 0, sizeof(tail));

  if (*p == ':') {
    if (p[1] != ':')
      return 0;
    p += 2;
    compression = 1;
  }

  while (*p != ']') {
    if (headCount + tailCount == 8)
      return 0;

    const char *groupStart = p;
    uint32_t value = 0;
    while (isHexDigit(*p)) {
      value = (value << 4) | static_cast<uint32_t>(decodeHex(*p));
      p++;
      if (p - groupStart > 4)
        return 0;
    }
    if (p == groupStart)
      return 0;

    if (*p == '.') {
      // embedded IPv4 (ls32): reparse the group as a dotted quad closing the literal
      uint32_t octets[4];
      p = groupStart;
      for (int i = 0; i < 4; i++) {
        if (i != 0) {
          if (*p != '.')
            return 0;
          p++;
        }
        const char *octetStart = p;
        uint32_t octet = 0;
        while (isDigit(*p)) {
          octet = octet*10u + static_cast<uint32_t>(*p - '0');
          p++;
          if (p - octetStart > 3)
            return 0;
        }
        if (p == octetStart || octet > 255)
          return 0;
        octets[i] = octet;
      }
      if (*p != ']' || headCount + tailCount > 6)
        return 0;
      uint16_t high = static_cast<uint16_t>((octets[0] << 8) | octets[1]);
      uint16_t low = static_cast<uint16_t>((octets[2] << 8) | octets[3]);
      if (compression) {
        tail[tailCount++] = high;
        tail[tailCount++] = low;
      } else {
        groups[headCount++] = high;
        groups[headCount++] = low;
      }
      break;
    }

    if (compression)
      tail[tailCount++] = static_cast<uint16_t>(value);
    else
      groups[headCount++] = static_cast<uint16_t>(value);

    if (*p == ':') {
      if (p[1] == ':') {
        if (compression)
          return 0;
        compression = 1;
        p += 2;
      } else {
        p++;
        if (!isHexDigit(*p))
          return 0;
      }
    } else if (*p != ']') {
      return 0;
    }
  }

  // '::' must stand for at least one zero group
  if (compression ? (headCount + tailCount > 7) : (headCount != 8))
    return 0;

  for (int i = 0; i < tailCount; i++)
    groups[8 - tailCount + i] = tail[i];

  URIComponent component;
  component.type = uriCtHostIPv6;
  memcpy(component.ipv6, groups, sizeof(component.ipv6));
  callback(&component, arg);

  *ptr = p + 1;
  return 1;
}

// Strict dotted-quad over exactly [p, end): decimal octets 0-255, no leading
// zeros (RFC 3986 dec-octet; "192.168.000.001" is a reg-name, as in inet_pton)
static int spanIsIpv4(const char *p, const char *end, uint32_t *out)
{
  uint32_t ipv4 = 0;
  for (int i = 0; i < 4; i++) {
    if (i != 0) {
      if (p == end || *p != '.')
        return 0;
      p++;
    }
    const char *octetStart = p;
    uint32_t octet = 0;
    while (p != end && isDigit(*p)) {
      octet = octet*10u + static_cast<uint32_t>(*p - '0');
      p++;
      if (p - octetStart > 3)
        return 0;
    }
    if (p == octetStart || octet > 255)
      return 0;
    if (p - octetStart > 1 && *octetStart == '0')
      return 0;
    ipv4 |= octet << (8*i);
  }
  if (p != end)
    return 0;
  *out = ipv4;
  return 1;
}

// host = full-span IPv4address or reg-name (RFC 3986: IPv4 wins only when the
// whole span matches); empty span emits nothing; returns consumed length
static size_t uriParseRegnameHost(const char **ptr, uriParseCb callback, void *arg)
{
  const char *b = *ptr, *p = *ptr;
  for (;;) {
    if (isUnreserved(*p) || isSubDelims(*p)) {
      p++;
    } else if (isPctEncoded(&p)) {
      continue;
    } else {
      break;
    }
  }

  if (p != b) {
    URIComponent component;
    uint32_t ipv4;
    if (spanIsIpv4(b, p, &ipv4)) {
      component.type = uriCtHostIPv4;
      component.u32 = ipv4;
    } else {
      component.type = uriCtHostDNS;
      component.raw.data = b;
      component.raw.size = static_cast<size_t>(p-b);
    }
    callback(&component, arg);
  }

  *ptr = p;
  return static_cast<size_t>(p-b);
}


ParserResultTy uriParsePath(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg)
{
  URIComponent component;
  const char *begin = *ptr;
  const char *p = *ptr;
  const char *lastElement = p;
  while (p != end) {
    if (isPChar(&p)) {
      continue;
    } else if (*p == '/') {
      if (p != *ptr) {
        component.type = uriCtPathElement;
        component.raw.data = lastElement;
        component.raw.size = static_cast<size_t>(p-lastElement);
        if (!callback(&component, arg))
          return ParserResultCancelled;
        *ptr = p;
      }
      p++;
      lastElement = p;
    } else {
      break;
    }
  }

  if (p != *ptr) {
    if (p == end && !uriOnly)
      return ParserResultNeedMoreData;

    // send last fragment
    component.type = uriCtPathElement;
    component.raw.data = lastElement;
    component.raw.size = static_cast<size_t>(p-lastElement);
    if (!callback(&component, arg))
      return ParserResultCancelled;
    *ptr = p;

    // send entire path
    component.type = uriCtPath;
    component.raw.data = begin;
    component.raw.size = static_cast<size_t>(p-begin);
    if (!callback(&component, arg))
      return ParserResultCancelled;
  }
  
  return ParserResultOk;
}

static int uriParseAuthority(const char **ptr, uriParseCb callback, void *arg)
{
  const char *p = *ptr;

  // userinfo: lookahead over its charset (reg-name + ':'), present only if it ends at '@'
  const char *scan = p;
  for (;;) {
    if (isUnreserved(*scan) || isSubDelims(*scan) || *scan == ':') {
      scan++;
    } else if (isPctEncoded(&scan)) {
      continue;
    } else {
      break;
    }
  }
  if (*scan == '@') {
    URIComponent component;
    component.type = uriCtUserInfo;
    component.raw.data = p;
    component.raw.size = static_cast<size_t>(scan-p);
    callback(&component, arg);
    p = scan + 1;
  }

  if (*p == '[') {
    p++;
    if (!uriParseIpLiteral(&p, callback, arg))
      return 0;
  } else {
    uriParseRegnameHost(&p, callback, arg);
  }

  if (*p == ':') {
    p++;
    int32_t i32 = 0;
    while (isDigit(*p)) {
      i32 = i32*10 + (*p - '0');
      p++;
    }
    URIComponent component;
    component.type = uriCtPort;
    component.i32 = i32;
    callback(&component, arg);
  }

  if (*p == '/') {
    if (uriParsePath(&p, p+strlen(p), true, callback, arg) != ParserResultOk)
      return 0;
  }

  *ptr = p;
  return 1;
}


static int uriParseHierPart(const char **ptr, uriParseCb callback, void *arg)
{
  const char *p = *ptr;

  if (p[0] == '/' && p[1] == '/') {
    // authority form; a failed authority leaves *ptr untouched
    p += 2;
    if (!uriParseAuthority(&p, callback, arg))
      return 0;
    int result = 1;
    if (*p == '/')
      result = (uriParsePath(&p, p+strlen(p), true, callback, arg) == ParserResultOk);
    *ptr = p;
    return result;
  }

  // with or without a leading '/', everything else is a single path
  int result = (uriParsePath(&p, p+strlen(p), true, callback, arg) == ParserResultOk);
  *ptr = p;
  return result;
}

ParserResultTy uriParseQuery(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg)
{
  URIComponent component;
  const char *begin = *ptr;
  const char *p = *ptr;
  const char *lastName = p;
  const char *lastValue = nullptr;
  size_t lastNameSize = 0;
  while (p != end) {
    if (*p == '=') {
      lastNameSize = static_cast<size_t>(p - lastName);
      p++;
      lastValue = p;
    } else if (*p == '&') {
      if (lastValue) {
        component.type = uriCtQueryElement;
        component.raw.data = lastName;
        component.raw.size = lastNameSize;
        component.raw2.data = lastValue;
        component.raw2.size = static_cast<size_t>(p - lastValue);
        if (!callback(&component, arg))
          return ParserResultCancelled;
        *ptr = p;
      }
      p++;
      lastName = p;
    } else if (isPChar(&p)) {
      continue;
    } else if (*p == '/' || *p == '?') {
      p++;
      continue;
    } else {
      if (p != *ptr) {
        break;
      }
    }
  }

  if (p != *ptr) {
    if (p == end && !uriOnly)
      return ParserResultNeedMoreData;

    if (lastValue) {
      component.type = uriCtQueryElement;
      component.raw.data = lastName;
      component.raw.size = lastNameSize;
      component.raw2.data = lastValue;
      component.raw2.size = static_cast<size_t>(p - lastValue);
      if (!callback(&component, arg))
        return ParserResultCancelled;
    }

    component.type = uriCtQuery;
    component.raw.data = begin;
    component.raw.size = static_cast<size_t>(p-begin);
    if (!callback(&component, arg))
      return ParserResultCancelled;
    *ptr = p;
  }

  return ParserResultOk;
}

ParserResultTy uriParseFragment(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg)
{
  const char *p = *ptr;
  while (p != end) {
    if (isPChar(&p)) {
      continue;
    } else if (*p == '/' || *p == '?') {
      p++;
      continue;
    } else {
      break;
    }
  }
  
  if (p != *ptr) {
    if (p == end && !uriOnly)
      return ParserResultNeedMoreData;

    URIComponent component;
    component.type = uriCtFragment;
    component.raw.data = *ptr;
    component.raw.size = static_cast<size_t>(p-*ptr);
    if (!callback(&component, arg))
      return ParserResultCancelled;
    *ptr = p;
  }

  return ParserResultOk;
}

int uriParse(const char *uri, uriParseCb callback, void *arg)
{ 
  const char *ptr = uri;
  if (!uriParseScheme(&ptr, callback, arg))
    return 0;
  if (*ptr++ != ':')
    return 0;
  if (!uriParseHierPart(&ptr, callback, arg))
    return 0;
  
  if (*ptr == '?') {
    ptr++;
    if (uriParseQuery(&ptr, ptr + strlen(ptr), true, callback, arg) != ParserResultOk)
      return 0;
  }
  
  if (*ptr == '#') {
    ptr++;
    if (uriParseFragment(&ptr, ptr + strlen(ptr), true, callback, arg) != ParserResultOk)
      return 0;
  }
  
  return 1;
}

// host[:port] only (no scheme/userinfo/path), the whole string must match;
// uriCtPort is emitted only when the port is present
int uriParseHostPort(const char *hostport, uriParseCb callback, void *arg)
{
  const char *p = hostport;

  if (*p == '[') {
    p++;
    if (!uriParseIpLiteral(&p, callback, arg))
      return 0;
  } else {
    if (uriParseRegnameHost(&p, callback, arg) == 0)
      return 0;
  }

  if (*p == ':') {
    p++;
    if (!isDigit(*p))
      return 0;
    uint32_t port = 0;
    while (isDigit(*p)) {
      port = port*10u + static_cast<uint32_t>(*p - '0');
      p++;
      if (port > 65535)
        return 0;
    }
    URIComponent component;
    component.type = uriCtPort;
    component.i32 = static_cast<int32_t>(port);
    callback(&component, arg);
  }

  return *p == 0;
}

static char decodeHex(char s)
{
  if (s >= '0' && s <= '9')
    return s - '0';
  else if (s >= 'A' && s <= 'F')
    return s - 'A' + 10;
  else if (s >= 'a' && s <= 'f')
    return s - 'a' + 10;
  else
    return -1;
}

static char encodeHex(char s)
{
  if (s >= 0 && s <= 9)
    return '0' + s;
  else
    return 'A' + s - 10;
}  
  

static void uriPctDecode(const char *ptr, size_t size, std::string &out)
{
  out.clear();
  const char *p = ptr, *e = ptr+size;
  while (p < e) {
    if (*p == '%' && (e-p) >= 3 && isHexDigit(*(p+1)) && isHexDigit(*(p+2))) {
      int s = (decodeHex(*(p+1)) << 4) + decodeHex(*(p+2));
      out.push_back(static_cast<char>(s));
      p += 3;
    } else {
      out.push_back(*p++);
    }
  }
}

static void uriPctEncode(const char *ptr, size_t size, const char *extra, std::string &out)
{
  const char *p = ptr, *e = ptr+size;
  while (p < e) {
    if (isUnreserved(*p) || isSubDelims(*p) || strchr(extra, *p)) {
      out.push_back(*p);
    } else {
      out.push_back('%');
      out.push_back(encodeHex(*p >> 4));
      out.push_back(encodeHex(*p & 0xF));
    }
    p++;
  }
  
}

static int stdCb(URIComponent *component, void *arg)
{
  URI *data = static_cast<URI*>(arg);
  switch (component->type) {
    case uriCtSchema :
      data->schema.assign(component->raw.data, component->raw.size);
      break;
    case uriCtUserInfo :
      uriPctDecode(component->raw.data, component->raw.size, data->userInfo);
      break;
    case uriCtHostIPv4 :
      data->hostType = URI::HostTypeIPv4;
      data->ipv4 = component->u32;
      break;
    case uriCtHostIPv6 :
      data->hostType = URI::HostTypeIPv6;
      memcpy(data->ipv6, component->ipv6, sizeof(data->ipv6));
      break;
    case uriCtHostDNS :
      data->hostType = URI::HostTypeDNS;
      uriPctDecode(component->raw.data, component->raw.size, data->domain);
      break;
    case uriCtPort :
      data->port = component->i32;
      break;
    case uriCtPath :
      uriPctDecode(component->raw.data, component->raw.size, data->path);
      break;
    case uriCtQuery :
      uriPctDecode(component->raw.data, component->raw.size, data->query);
      break;
    case uriCtFragment :
      uriPctDecode(component->raw.data, component->raw.size, data->fragment);
      break;      
  }

  return 1;
}

int uriParse(const char *uri, URI *data)
{
  data->schema.clear();
  data->userInfo.clear();
  data->hostType = URI::HostTypeNone;
  data->port = 0;
  data->path.clear();
  data->query.clear();
  data->fragment.clear();
  
  return uriParse(uri, stdCb, data);
}


int uriParseHostPort(const char *hostport, URI *data, uint16_t defaultPort)
{
  data->schema.clear();
  data->userInfo.clear();
  data->hostType = URI::HostTypeNone;
  data->port = defaultPort;
  data->path.clear();
  data->query.clear();
  data->fragment.clear();

  return uriParseHostPort(hostport, stdCb, data);
}


void URI::build(std::string &out)
{
  out.clear();
  if (!schema.empty()) {
    out += schema;
    out += ":";
  }
  
  if (!userInfo.empty() || hostType != URI::HostTypeNone)
    out += "//";
  if (!userInfo.empty()) {
    uriPctEncode(userInfo.c_str(), userInfo.length(), ":", out);
    out.push_back('@');
  }
  
  switch (hostType) {
    case URI::HostTypeIPv4 : {
      char x1[16];
      char x2[16];
      char x3[16];
      char x4[16];

      xitoa((ipv4 >>  0) & 0xFF, x1);
      xitoa((ipv4 >>  8) & 0xFF, x2);
      xitoa((ipv4 >> 16) & 0xFF, x3);
      xitoa( ipv4 >> 24, x4);      
      
      out += x1;
      out.push_back('.');
      out += x2;
      out.push_back('.');
      out += x3;
      out.push_back('.');
      out += x4;      
      break;
    }
    case URI::HostTypeIPv6 : {
      out.push_back('[');
      out.push_back(']');
      break;
    }    
    
    case URI::HostTypeDNS : {
      uriPctEncode(domain.c_str(), domain.length(), "", out);
      break;
    }
  }
  
  if (port != -1) {
    char x1[16];
    xitoa(port, x1);
    out.push_back(':');
    out += x1;
  }
    
  if (!path.empty())
    uriPctEncode(path.c_str(), path.length(), "/:@", out);
    
  if (!query.empty()) {
    out.push_back('?');
    uriPctEncode(query.c_str(), query.length(), "/:@?", out);
  }
  
  if (!fragment.empty()) {
    out.push_back('#');
    uriPctEncode(fragment.c_str(), fragment.length(), "/:@?", out);
  }
}
