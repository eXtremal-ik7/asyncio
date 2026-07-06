#include "asyncio/asyncio.h"
#include "asyncio/socket.h"
#include "asyncio/timer.h"

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <map>
#include <vector>

#if !defined(OS_WINDOWS)
#include <netdb.h>
#endif

static option longOpts[] = {
  {"interval", required_argument, nullptr, 'i'},
  {"count", required_argument, nullptr, 'c'},
  {"ipv4", no_argument, nullptr, '4'},
  {"ipv6", no_argument, nullptr, '6'},
  {"help", no_argument, nullptr, 0},
  {nullptr, 0, nullptr, 0}
};

static const char shortOpts[] = "i:c:46";
static const char *gTarget = nullptr;
static double gInterval = 1.0;
static unsigned gCount = 4;
static int gFamily = AF_UNSPEC;


static const uint8_t ICMP_ECHO = 8;
static const uint8_t ICMP_ECHOREPLY = 0;
static const uint8_t ICMP6_ECHO = 128;
static const uint8_t ICMP6_ECHOREPLY = 129;


#pragma pack(push, 1)
  struct ip {
    uint8_t ip_verlen;
    uint8_t ip_tos;
    uint16_t ip_len;
    uint16_t ip_id;
    uint16_t ip_fragoff;
    uint8_t ip_ttl;
    uint8_t ip_proto;
    uint16_t ip_chksum;
    uint32_t ip_src_addr;
    uint32_t ip_dst_addr;
  };

  struct icmp {
    uint8_t icmp_type;
    uint8_t icmp_code;
    uint16_t icmp_cksum;
    uint32_t icmp_id;
    uint16_t icmp_seq;
  };
#pragma pack(pop)

__NO_PADDING_BEGIN
struct ICMPClientData {
  asyncBase *base;
  aioObject *rawSocket;
  HostAddress remoteAddress;
  icmp data;
  uint32_t id;
  uint8_t buffer[1024];
  std::map<unsigned, timeMark> times;
};
__NO_PADDING_END

uint16_t InternetChksum(uint16_t *lpwIcmpData, uint16_t wDataLength)
{
  uint32_t lSum;
  uint16_t wOddByte;
  uint16_t wAnswer;

  lSum = 0L;

  while (wDataLength > 1) {
    lSum += *lpwIcmpData++;
    wDataLength -= 2;
  }

  if (wDataLength == 1)  {
    wOddByte = 0;
    *(reinterpret_cast<uint8_t*>(&wOddByte)) = *reinterpret_cast<uint8_t*>(lpwIcmpData);
    lSum += wOddByte;
  }

  lSum = (lSum >> 16) + (lSum & 0xffff);
  lSum += (lSum >> 16);
  wAnswer = static_cast<uint16_t>(~lSum);
  return(wAnswer);
}


void printHelpMessage(const char *appName)
{
  printf("Usage: %s help:\n"
    "%s <options> address\n"
    "General options:\n"
    "  --count or -c packets count\n"
    "  --interval or -i interval between packets sending\n"
    "  --ipv4 or -4 use IPv4 only\n"
    "  --ipv6 or -6 use IPv6 only\n",
    appName, appName);
}


void readCb(AsyncOpStatus status, aioObject *rawSocket, HostAddress address, size_t transferred, void *arg)
{
  __UNUSED(address);
  ICMPClientData *client = static_cast<ICMPClientData*>(arg);
  // raw IPv4 sockets deliver the IP header in front of the ICMP message,
  // raw IPv6 sockets deliver the ICMPv6 message alone (RFC 3542)
  bool isIpv6 = client->remoteAddress.family == AF_INET6;
  size_t ipHeaderSize = isIpv6 ? 0 : sizeof(ip);
  uint8_t echoReplyType = isIpv6 ? ICMP6_ECHOREPLY : ICMP_ECHOREPLY;
  if (status == aosSuccess && transferred >= (ipHeaderSize + sizeof(icmp))) {
    icmp *receivedIcmp = reinterpret_cast<icmp*>(client->buffer + ipHeaderSize);

    if (receivedIcmp->icmp_type == echoReplyType) {
      std::map<unsigned, timeMark>::iterator F = client->times.find(receivedIcmp->icmp_id);
      if (F != client->times.end()) {
        double diff = static_cast<double>(usDiff(F->second, getTimeMark()));
        fprintf(stdout,
                " * [%u] response from %s %0.4lgms\n",
                static_cast<unsigned>(receivedIcmp->icmp_id),
                gTarget,
                diff / 1000.0);
        client->times.erase(F);
      }
    }
  }

  // -c limit: all requests are sent and every one is answered or timed out
  if (gCount != 0 && client->id >= gCount && client->times.empty()) {
    postQuitOperation(client->base);
    return;
  }

  aioReadMsg(rawSocket, client->buffer, sizeof(client->buffer), afNone, 0, readCb, client);
}

void pingTimerCb(aioUserEvent *event, void *arg)
{
  __UNUSED(event);
  ICMPClientData *clientData = static_cast<ICMPClientData*>(arg);
  clientData->id++;
  clientData->data.icmp_id = clientData->id;
  clientData->data.icmp_cksum = 0;
  // the ICMPv6 checksum needs a pseudo-header with source/destination
  // addresses, so the kernel always computes it for raw ICMPv6 sockets
  if (clientData->remoteAddress.family != AF_INET6)
    clientData->data.icmp_cksum =
      InternetChksum(reinterpret_cast<uint16_t*>(&clientData->data), sizeof(icmp));


  aioWriteMsg(clientData->rawSocket,
              &clientData->remoteAddress,
              &clientData->data, sizeof(icmp),
              afNone, 1000000, nullptr, nullptr);
  clientData->times[clientData->id] = getTimeMark();
}


void printTimerCb(aioUserEvent *event, void *arg)
{
  __UNUSED(event);
  std::vector<uint32_t> forDelete;
  ICMPClientData *clientData = static_cast<ICMPClientData*>(arg);
  for (std::map<unsigned, timeMark>::iterator I = clientData->times.begin(),
       IE = clientData->times.end(); I != IE; ++I) {
    uint64_t diff = usDiff(I->second, getTimeMark());
    if (diff > 1000000) {
      fprintf(stdout, " * [%u] timeout\n", static_cast<unsigned>(I->first));
      forDelete.push_back(I->first);
    }
  }

  for (size_t i = 0; i < forDelete.size(); i++)
    clientData->times.erase(forDelete[i]);

  // -c limit: all requests are sent and every one is answered or timed out
  if (gCount != 0 && clientData->id >= gCount && clientData->times.empty())
    postQuitOperation(clientData->base);
}


int main(int argc, char **argv)
{
  int res, index = 0;
  while ((res = getopt_long(argc, argv, shortOpts, longOpts, &index)) != -1) {
    switch(res) {
      case 0 :
        if (strcmp(longOpts[index].name, "help") == 0) {
          printHelpMessage(argv[0]);
          return 0;
        }
        break;
      case 'c' :
        gCount = static_cast<unsigned>(atoi(optarg));
        break;
      case 'i' :
        gInterval = atof(optarg);
        break;
      case '4' :
        gFamily = AF_INET;
        break;
      case '6' :
        gFamily = AF_INET6;
        break;
      case ':' :
        fprintf(stderr, "Error: option %s missing argument\n",
                longOpts[index].name);
        break;
      case '?' :
        fprintf(stderr, "Error: invalid option %s\n", argv[optind-1]);
        break;
      default :
        break;
    }
  }

  if (optind >= argc) {
    fprintf(stderr, "You must specify target address, see help\n");
    return 1;
  }

  gTarget = argv[optind];
  initializeAsyncIo(aiNone);

  HostAddress remoteAddress;
  {
    struct addrinfo hints;
    struct addrinfo *result = nullptr;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = gFamily;
    int err = getaddrinfo(gTarget, nullptr, &hints, &result);
    if (err != 0 || !result) {
      fprintf(stderr,
              " * cannot retrieve address of %s (%s)\n",
              gTarget,
              err != 0 ? gai_strerror(err) : "no addresses returned");
      return 1;
    }

    struct sockaddr_storage sa;
    memset(&sa, 0, sizeof(sa));
    memcpy(&sa, result->ai_addr, result->ai_addrlen);
    sockaddrToHostAddress(&sa, &remoteAddress);
    freeaddrinfo(result);
  }

  char addressText[INET6_ADDRSTRLEN];
  inet_ntop(remoteAddress.family,
            remoteAddress.family == AF_INET6 ?
              static_cast<const void*>(remoteAddress.ipv6) :
              static_cast<const void*>(&remoteAddress.ipv4),
            addressText, sizeof(addressText));

  // ICMPv6 cannot reach v4-mapped ::ffff:a.b.c.d addresses; macOS getaddrinfo
  // returns them for AF_INET6 requests when the host has no global IPv6
  if (remoteAddress.family == AF_INET6 &&
      remoteAddress.ipv6[0] == 0 && remoteAddress.ipv6[1] == 0 &&
      remoteAddress.ipv6[2] == 0 && remoteAddress.ipv6[3] == 0 &&
      remoteAddress.ipv6[4] == 0 && remoteAddress.ipv6[5] == 0xffff) {
    fprintf(stderr,
            " * %s has no reachable IPv6 address (resolver returned v4-mapped %s)\n",
            gTarget, addressText);
    return 1;
  }

  // route check and source address discovery, the trick system ping uses:
  // connect() on a UDP socket only does a routing lookup - no datagram is
  // sent - and getsockname() reports the source address the kernel picked
  HostAddress localAddress;
  {
    HostAddress probeAddress = remoteAddress;
    probeAddress.port = 1025;
    struct sockaddr_storage sa;
    socketLenTy addrlen = hostAddressToSockaddr(&probeAddress, &sa);
    socketTy probeSocket = socketCreate(remoteAddress.family, SOCK_DGRAM, IPPROTO_UDP, 0);
    if (connect(probeSocket, (struct sockaddr*)&sa, addrlen) != 0) {
      fprintf(stderr, " * connect: %s is unreachable (no route)\n", addressText);
      socketClose(probeSocket);
      return 1;
    }
    socketLenTy nameLen = sizeof(sa);
    getsockname(probeSocket, (struct sockaddr*)&sa, &nameLen);
    sockaddrToHostAddress(&sa, &localAddress);
    localAddress.port = 0;  // raw sockets have no port
    socketClose(probeSocket);

    // a link-local source needs a scope id that HostAddress cannot carry yet;
    // fall back to the wildcard address and let the kernel pick the source
    // for every packet, the way this example always worked
    if (localAddress.family == AF_INET6 &&
        (localAddress.ipv6[0] & htons(0xffc0)) == htons(0xfe80))
      memset(localAddress.ipv6, 0, sizeof(localAddress.ipv6));
  }

  fprintf(stdout, "PING %s (%s)\n", gTarget, addressText);

  socketTy S = socketCreate(remoteAddress.family, SOCK_RAW,
                            remoteAddress.family == AF_INET6 ?
                              static_cast<int>(IPPROTO_ICMPV6) :
                              static_cast<int>(IPPROTO_ICMP), 1);
  socketReuseAddr(S);
  if (socketBind(S, &localAddress) != 0) {
    char sourceText[INET6_ADDRSTRLEN];
    inet_ntop(localAddress.family,
              localAddress.family == AF_INET6 ?
                static_cast<const void*>(localAddress.ipv6) :
                static_cast<const void*>(&localAddress.ipv4),
              sourceText, sizeof(sourceText));
    fprintf(stderr, " * bind to %s error (raw sockets require root/administrator)\n", sourceText);
    exit(1);
  }

  ICMPClientData client;
  client.id = 0;
  client.remoteAddress = remoteAddress;

  client.data.icmp_type = remoteAddress.family == AF_INET6 ? ICMP6_ECHO : ICMP_ECHO;
  client.data.icmp_code = 0;
  client.data.icmp_seq = 0;

  asyncBase *base = createAsyncBase(amOSDefault);
  client.base = base;
  aioUserEvent *pingTimer = newUserEvent(base, 0, pingTimerCb, &client);
  aioUserEvent *printTimer = newUserEvent(base, 0, printTimerCb, &client);
  client.rawSocket = newSocketIo(base, S);

  aioReadMsg(client.rawSocket, client.buffer, sizeof(client.buffer), afNone, 0, readCb, &client);
  userEventStartTimer(printTimer, 100000, -1);
  // the timer counter stops sending after gCount packets (0 means no limit)
  userEventStartTimer(pingTimer, static_cast<uint64_t>(gInterval*1000000.0), static_cast<int>(gCount));
  asyncLoop(base);
  return 0;
}
