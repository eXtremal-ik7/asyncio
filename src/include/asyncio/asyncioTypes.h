#ifndef __ASYNCTYPES_H_
#define __ASYNCTYPES_H_

#include "asyncioconfig.h"
#include <stdint.h>
#include <string.h>

#if defined(OS_WINDOWS)
#define _WINSOCK_DEPRECATED_NO_WARNINGS
#include <winsock2.h>
// sockaddr_in6 and socklen_t come from ws2ipdef.h/ws2tcpip.h, not winsock2.h
#include <ws2tcpip.h>
#include <mswsock.h>
#include <windows.h>
#include <limits.h>
typedef HANDLE iodevTy;
typedef SOCKET socketTy;
typedef int socketLenTy;
// Winsock buffer lengths are ULONG: clamp each chunk so a >4Gb remainder
// never truncates to len == 0 (a phantom EOF/empty transfer)
static inline ULONG wsaChunkSize(size_t remaining)
{
  return remaining > ULONG_MAX ? ULONG_MAX : (ULONG)remaining;
}
#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif
#elif defined(OS_COMMONUNIX)
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sched.h>
typedef int iodevTy;
typedef int socketTy;
typedef socklen_t socketLenTy;
#define INVALID_SOCKET -1
#endif

// Thread local storage
#ifdef _MSC_VER
#define __tls __declspec(thread)
#else
#define __tls __thread
#endif

typedef struct HostAddress {
  union {
    uint32_t ipv4;
    uint16_t ipv6[8];
  };
  uint16_t port;     // host byte order
  uint16_t family;   // AF_* value
} HostAddress;

/* Convert HostAddress to sockaddr. Returns the sockaddr size. */
static inline socklen_t hostAddressToSockaddr(const HostAddress *host, struct sockaddr_storage *sa)
{
  memset(sa, 0, sizeof(*sa));
  if (host->family == AF_INET) {
    struct sockaddr_in *sin = (struct sockaddr_in*)sa;
    sin->sin_family = AF_INET;
    sin->sin_addr.s_addr = host->ipv4;
    sin->sin_port = htons(host->port);
    return sizeof(struct sockaddr_in);
  } else {
    struct sockaddr_in6 *sin6 = (struct sockaddr_in6*)sa;
    sin6->sin6_family = AF_INET6;
    memcpy(&sin6->sin6_addr, host->ipv6, sizeof(sin6->sin6_addr));
    sin6->sin6_port = htons(host->port);
    return sizeof(struct sockaddr_in6);
  }
}

/* Extract HostAddress from sockaddr. */
static inline void sockaddrToHostAddress(const struct sockaddr_storage *sa, HostAddress *host)
{
  host->family = sa->ss_family;
  if (sa->ss_family == AF_INET) {
    const struct sockaddr_in *sin = (const struct sockaddr_in*)sa;
    host->ipv4 = sin->sin_addr.s_addr;
    host->port = ntohs(sin->sin_port);
  } else {
    const struct sockaddr_in6 *sin6 = (const struct sockaddr_in6*)sa;
    memcpy(host->ipv6, &sin6->sin6_addr, sizeof(host->ipv6));
    host->port = ntohs(sin6->sin6_port);
  }
}

/* Fill HostAddress from a bare IPv4 ("127.0.0.1") or IPv6 ("::1") literal;
   no port, no brackets - "host:port" strings belong to the URI parser level.
   Sets family and address bytes, resets port to 0, so the structure is fully
   defined after a successful call.
   Returns 1 on success, 0 if the string is not a valid address literal. */
static inline int hostAddressFromAscii(const char *cp, HostAddress *host)
{
  struct in_addr a4;
  struct in6_addr a6;
  if (inet_pton(AF_INET, cp, &a4) == 1) {
    host->family = AF_INET;
    host->ipv4 = a4.s_addr;
    host->port = 0;
    return 1;
  }
  if (inet_pton(AF_INET6, cp, &a6) == 1) {
    host->family = AF_INET6;
    memcpy(host->ipv6, &a6, sizeof(host->ipv6));
    host->port = 0;
    return 1;
  }
  return 0;
}

#endif //__ASYNCTYPES_H_
