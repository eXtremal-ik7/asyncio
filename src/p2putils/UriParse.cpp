#include "asyncioconfig.h"
#include "macro.h"
#include "p2putils/uriParse.h"
#include "p2putils/strExtras.h"
#include <string.h>

int isLetter(char s)
{
  return (s >= 'A' && s <= 'Z') || (s >= 'a' && s <= 'z');
}

int isDigit(char s)
{
  return (s >= '0' && s <= '9');
}

int isHexDigit(char s)
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


int uriParseScheme(const char **ptr, uriParseCb callback, void *arg)
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

int uriParseIpLiteral(const char **ptr, uriParseCb callback, void *arg)
{
  __UNUSED(ptr); __UNUSED(callback); __UNUSED(arg);
  return 0;
}

int uriParseIpv4(const char **ptr, uriParseCb callback, void *arg)
{
  const char *p = *ptr;
  uint32_t u32 = 0;
  uint32_t ipv4 = 0;
  int segments = 0;
  for (;;) {
    if (isDigit(*p)) {
      u32 = u32*10 + static_cast<uint32_t>(*p - '0');
      p++;
    } else if (*p == '.' && segments < 3) {
      ipv4 |= (u32 << (8*segments));
      u32 = 0;
      segments++;
      p++;
    } else {
      if (segments != 3)
        return 0;
      ipv4 |= (u32 << (8*segments));
      URIComponent component;
      component.type = uriCtHostIPv4;
      component.u32 = ipv4;
      callback(&component, arg);
      break;
    }
  }
  
  *ptr = p;
  return 1;
}


ParserResultTy uriParsePath(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg)
{
  URIComponent component;
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
    component.raw.data = *ptr;
    component.raw.size = static_cast<size_t>(p-*ptr);
    if (!callback(&component, arg))
      return ParserResultCancelled;
  }
  
  return ParserResultOk;
}

int uriParseAuthority(const char **ptr, uriParseCb callback, void *arg)
{
  enum {
    StBegin = 0,
    StUserinfoOrRegname,
    StWaitHost,
    StWaitRegname,
    StWaitPort
  };
  
  int state;
  const char *b = *ptr, *p = *ptr;

  if (*p == '[') {
    p++;
    if (!uriParseIpLiteral(&p, callback, arg))
      return 0;
    state = StWaitPort;
  } else if (isDigit(*p)) {
    if (!uriParseIpv4(&p, callback, arg))
      return 0;
    state = StWaitPort;
  } else if (isUnreserved(*p) || isSubDelims(*p)) {
    state = StUserinfoOrRegname;
    p++;
  } else if (isPctEncoded(&p)) {
    state = StUserinfoOrRegname;
  } else {
    state = StWaitPort;
  }
    
  if (state == StUserinfoOrRegname) {
    state = StWaitPort;
    const char *firstColon = nullptr;
    for (;;) {
      if (isUnreserved(*p) || isSubDelims(*p)) {
        p++;
      } else if (isPctEncoded(&p)) {
        continue;
      } else if (*p == ':') {
        if (!firstColon)
          firstColon = p;        
        p++;
      } else if (*p == '@') {
        p++;
        state = StWaitHost;
        break;
      } else {
        break;
      }
    }
    
    if (state == StWaitPort) {
      // host found
      if (firstColon)
        p = firstColon;
      
      URIComponent component;
      component.type = uriCtHostDNS;
      component.raw.data = b;
      component.raw.size = static_cast<size_t>(p-b);
      callback(&component, arg);
    } else if (state == StWaitHost) {
      // user info found
      URIComponent component;
      component.type = uriCtUserInfo;
      component.raw.data = b;
      component.raw.size = static_cast<size_t>(p-b-1);
      callback(&component, arg);      
    }
  }
  
  b = p;
  if (state == StWaitHost) {
    if (*p == '[') {
      p++;
      if (!uriParseIpLiteral(&p, callback, arg))
        return 0;
      state = StWaitPort;
    } else if (isDigit(*p)) {
      if (!uriParseIpv4(&p, callback, arg))
        return 0;
      state = StWaitPort;
    } else if (isUnreserved(*p) || isSubDelims(*p)) {
      state = StWaitRegname;
      p++;
    } else if (isPctEncoded(&p)) {
      state = StWaitRegname;
    } else {
      state = StWaitPort;
    }
    
    if (state == StWaitRegname) {
      for (;;) {
        if (isUnreserved(*p) || isSubDelims(*p)) {
          p++;
        } else if (isPctEncoded(&p)) {
          continue;
        } else {
          // host found
          URIComponent component;
          component.type = uriCtHostDNS;
          component.raw.data = b;
          component.raw.size = static_cast<size_t>(p-b);
          callback(&component, arg);
        }
      }
    }
    
    state = StWaitPort;    
  }
  
  if (state == StWaitPort && *p == ':') {
    p++;
    int32_t i32 = 0;
    for (;;) {
      if (isDigit(*p)) {
        i32 = i32*10 + (*p - '0');
      } else {
        break;
      }
        
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


int uriParseHierPart(const char **ptr, uriParseCb callback, void *arg)
{
  int result = 1;
  const char *p = *ptr;
  if (*p == '/') {
    if (*(p+1) == '/') {
      p += 2;
      if (!uriParseAuthority(&p, callback, arg))
        return 0;
      if (*p == '/') {
        result = (uriParsePath(&p, p+strlen(p), true, callback, arg) == ParserResultOk);
      }
    } else {
      result = (uriParsePath(&p, p+strlen(p), true, callback, arg) == ParserResultOk);
    }
  } else {
    result = (uriParsePath(&p, p+strlen(p), true, callback, arg) == ParserResultOk);
  }
  
  *ptr = p;
  return result;
}

ParserResultTy uriParseQuery(const char **ptr, const char *end, bool uriOnly, uriParseCb callback, void *arg)
{
  URIComponent component;
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
    component.raw.data = *ptr;
    component.raw.size = static_cast<size_t>(p-*ptr);
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
    if (!uriParseQuery(&ptr, ptr + strlen(ptr), true, callback, arg))
      return 0;
  }
  
  if (*ptr == '#') {
    ptr++;
    if (!uriParseFragment(&ptr, ptr + strlen(ptr), true, callback, arg))
      return 0;
  }
  
  return 1;
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
