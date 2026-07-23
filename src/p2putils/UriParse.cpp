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

enum UriCharacterResult {
  UriCharacterAccepted,
  UriCharacterNeedMore,
  UriCharacterInvalid
};

static UriCharacterResult consumePctEncoded(const char **ptr, const char *end)
{
  const char *p = *ptr;
  if (*p != '%')
    return UriCharacterInvalid;

  p++;
  if (p == end)
    return UriCharacterNeedMore;
  if (!isHexDigit(*p))
    return UriCharacterInvalid;

  p++;
  if (p == end)
    return UriCharacterNeedMore;
  if (!isHexDigit(*p))
    return UriCharacterInvalid;

  *ptr = p + 1;
  return UriCharacterAccepted;
}

// NUL-terminated counterpart used by the full-URI authority parser. Check
// one byte at a time so "%" and "%A" never read past the terminator, without
// a preliminary strlen pass over the remaining URI.
static int consumePctEncodedZ(const char **ptr)
{
  const char *p = *ptr;
  if (*p != '%')
    return 0;
  if (!isHexDigit(*++p))
    return 0;
  if (!isHexDigit(*++p))
    return 0;
  *ptr = p + 1;
  return 1;
}

// ASCII bitmap for RFC 3986 pchar without pct-encoded, which is handled by
// the bounded slow path below each scanner.
static inline int isPChar(char s)
{
  const unsigned value = static_cast<unsigned char>(s);
  if (value >= 128)
    return 0;
  const uint64_t mask = value < 64
      ? UINT64_C(0x2fff7fd200000000)
      : UINT64_C(0x47fffffe87ffffff);
  return (mask >> (value & 63)) & 1;
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
    } else if (consumePctEncodedZ(&p)) {
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
    if (isPChar(*p)) {
      ++p;
      continue;
    }
    if (*p == '%') {
      UriCharacterResult result = consumePctEncoded(&p, end);
      if (result == UriCharacterAccepted)
        continue;
      if (result == UriCharacterNeedMore)
        return ParserResultNeedMoreData;
    }
    if (*p == '/') {
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

  if (p == end && !uriOnly)
    return ParserResultNeedMoreData;

  if (p != *ptr) {
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
    } else if (consumePctEncodedZ(&scan)) {
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
    const char *digits = ++p;
    uint32_t port = 0;
    while (isDigit(*p)) {
      port = port*10u + static_cast<uint32_t>(*p - '0');
      p++;
      if (port > 65535)
        return 0;
    }
    if (p != digits) {
      URIComponent component;
      component.type = uriCtPort;
      component.i32 = static_cast<int32_t>(port);
      callback(&component, arg);
    }
  }

  *ptr = p;
  return 1;
}

static const char *uriStringEnd(const char *p, const char **cachedEnd)
{
  if (!*cachedEnd)
    *cachedEnd = p + strlen(p);
  return *cachedEnd;
}

static int uriParseHierPart(const char **ptr, const char **cachedEnd,
                            uriParseCb callback, void *arg)
{
  const char *p = *ptr;

  if (p[0] == '/' && p[1] == '/') {
    // authority form; a failed authority leaves *ptr untouched
    p += 2;
    if (!uriParseAuthority(&p, callback, arg))
      return 0;
    int result = 1;
    if (*p == '/')
      result = (uriParsePath(&p, uriStringEnd(p, cachedEnd), true,
                             callback, arg) == ParserResultOk);
    *ptr = p;
    return result;
  }

  // with or without a leading '/', everything else is a single path
  int result = (uriParsePath(&p, uriStringEnd(p, cachedEnd), true,
                             callback, arg) == ParserResultOk);
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
      lastValue = nullptr;
      lastNameSize = 0;
    } else {
      if (isPChar(*p)) {
        ++p;
        continue;
      }
      if (*p == '%') {
        UriCharacterResult result = consumePctEncoded(&p, end);
        if (result == UriCharacterAccepted)
          continue;
        if (result == UriCharacterNeedMore)
          return ParserResultNeedMoreData;
      }
      if (*p == '/' || *p == '?') {
        p++;
        continue;
      } else {
        break;
      }
    }
  }

  if (p == end && !uriOnly)
    return ParserResultNeedMoreData;

  if (p != *ptr) {
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
    if (isPChar(*p)) {
      ++p;
      continue;
    }
    if (*p == '%') {
      UriCharacterResult result = consumePctEncoded(&p, end);
      if (result == UriCharacterAccepted)
        continue;
      if (result == UriCharacterNeedMore)
        return ParserResultNeedMoreData;
    }
    if (*p == '/' || *p == '?') {
      p++;
      continue;
    } else {
      break;
    }
  }
  
  if (p == end && !uriOnly)
    return ParserResultNeedMoreData;

  if (p != *ptr) {
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
  const char *end = nullptr;
  if (!uriParseScheme(&ptr, callback, arg))
    return 0;
  if (*ptr++ != ':')
    return 0;
  if (!uriParseHierPart(&ptr, &end, callback, arg))
    return 0;
  
  if (*ptr == '?') {
    ptr++;
    if (uriParseQuery(&ptr, uriStringEnd(ptr, &end), true,
                      callback, arg) != ParserResultOk)
      return 0;
  }
  
  if (*ptr == '#') {
    ptr++;
    if (uriParseFragment(&ptr, uriStringEnd(ptr, &end), true,
                         callback, arg) != ParserResultOk)
      return 0;
  }
  
  return *ptr == 0;
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
  out.assign(ptr, size);
  size_t read = out.find('%');
  if (read == std::string::npos)
    return;

  size_t write = read;
  while (read < size) {
    if (out[read] == '%' && size-read >= 3 &&
        isHexDigit(out[read+1]) && isHexDigit(out[read+2])) {
      int s = (decodeHex(out[read+1]) << 4) + decodeHex(out[read+2]);
      out[write++] = static_cast<char>(s);
      read += 3;
    } else {
      out[write++] = out[read++];
    }
  }
  out.resize(write);
}

static void uriPctEncode(const char *ptr, size_t size, const char *extra, std::string &out)
{
  const char *p = ptr, *e = ptr+size;
  while (p < e) {
    // encode from the unsigned value: high bytes must not sign-extend, and
    // NUL must not match the extra string terminator
    if (*p && (isUnreserved(*p) || isSubDelims(*p) || strchr(extra, *p))) {
      out.push_back(*p);
    } else {
      unsigned char c = static_cast<unsigned char>(*p);
      out.push_back('%');
      out.push_back(encodeHex(c >> 4));
      out.push_back(encodeHex(c & 0xF));
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
  data->port = -1;
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
      // RFC 5952: lowercase hex without leading zeros; the longest run of
      // two or more zero groups becomes "::", the leftmost one on a tie
      int zeroStart = -1;
      int zeroSize = 1;
      for (int i = 0; i < 8; i++) {
        if (ipv6[i] != 0)
          continue;
        int j = i;
        while (j < 8 && ipv6[j] == 0)
          j++;
        if (j - i > zeroSize) {
          zeroStart = i;
          zeroSize = j - i;
        }
        i = j - 1;
      }

      out.push_back('[');
      for (int i = 0; i < 8; ) {
        if (i == zeroStart) {
          out.push_back(':');
          out.push_back(':');
          i += zeroSize;
          continue;
        }

        static const char digits[] = "0123456789abcdef";
        int shift = 12;
        while (shift && !((ipv6[i] >> shift) & 0xF))
          shift -= 4;
        for (; shift >= 0; shift -= 4)
          out.push_back(digits[(ipv6[i] >> shift) & 0xF]);

        i++;
        if (i < 8 && i != zeroStart)
          out.push_back(':');
      }
      out.push_back(']');
      break;
    }
    
    case URI::HostTypeDNS : {
      uriPctEncode(domain.c_str(), domain.length(), "", out);
      break;
    }
  }
  
  if (port >= 0) {
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
