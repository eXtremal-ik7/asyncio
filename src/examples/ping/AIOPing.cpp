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
  {"help", no_argument, nullptr, 0},
  {nullptr, 0, nullptr, 0}
};

static const char shortOpts[] = "i:c:";
static const char *gTarget = nullptr;
static double gInterval = 1.0;
static unsigned gCount = 4;


static const uint8_t ICMP_ECHO = 8;
static const uint8_t ICMP_ECHOREPLY = 0;


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
    "  --interval or -i interval between packets sending\n",
    appName, appName);
}


void readCb(AsyncOpStatus status, aioObject *rawSocket, HostAddress address, size_t transferred, void *arg)
{
  __UNUSED(address);
  ICMPClientData *client = static_cast<ICMPClientData*>(arg);
  if (status == aosSuccess && transferred >= (sizeof(ip) + sizeof(icmp))) {
    uint8_t *ptr = static_cast<uint8_t*>(client->buffer);
    icmp *receivedIcmp = reinterpret_cast<icmp*>(ptr + sizeof(ip));

    if (receivedIcmp->icmp_type == ICMP_ECHOREPLY) {     
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
  
  aioReadMsg(rawSocket, client->buffer, sizeof(client->buffer), afNone, 0, readCb, client);
}

void pingTimerCb(aioUserEvent *event, void *arg)
{
  __UNUSED(event);
  ICMPClientData *clientData = static_cast<ICMPClientData*>(arg);
  clientData->id++;
  clientData->data.icmp_id = clientData->id;
  clientData->data.icmp_cksum = 0;
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
}


int main(int argc, char **argv)
{
  int res, index = 0;
  while ((res = getopt_long(argc, argv, shortOpts, longOpts, &index)) != -1) {
    switch(res) {
      case 0 :
        if (strcmp(longOpts[index].name, "help") == 0) {
          printHelpMessage("modstress");
          return 0;
        }
        break;
      case 'c' :
        gCount = static_cast<unsigned>(atoi(optarg));
        break;
      case 'i' :
        gInterval = atof(optarg);
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
  HostAddress localAddress;  

  hostent *host = gethostbyname(gTarget);
  if (!host) {
    fprintf(stderr,
            " * cannot retrieve address of %s (gethostbyname failed)\n",
            argv[optind]);
    return 1;
  }

  u_long addr = host->h_addr ? *reinterpret_cast<u_long*>(host->h_addr) : 0;
  if (!addr) {
    fprintf(stderr,
            " * cannot retrieve address of %s (gethostbyname returns 0)\n",
            argv[optind]);
    return 1;
  }

  localAddress.family = AF_INET;
  localAddress.ipv4 = INADDR_ANY;
  localAddress.port = 0;
  socketTy S = socketCreate(AF_INET, SOCK_RAW, IPPROTO_ICMP, 1);
  socketReuseAddr(S);
  if (socketBind(S, &localAddress) != 0) {
    fprintf(stderr, " * bind error\n");
    exit(1);
  }

  ICMPClientData client;
  client.id = 0;
  client.remoteAddress.family = AF_INET;
  client.remoteAddress.ipv4 = static_cast<uint32_t>(addr);

  client.data.icmp_type = ICMP_ECHO;
  client.data.icmp_code = 0;
  client.data.icmp_seq = 0;

  asyncBase *base = createAsyncBase(amOSDefault);
  aioUserEvent *pingTimer = newUserEvent(base, 0, pingTimerCb, &client);
  aioUserEvent *printTimer = newUserEvent(base, 0, printTimerCb, &client);
  client.rawSocket = newSocketIo(base, S);

  aioReadMsg(client.rawSocket, client.buffer, sizeof(client.buffer), afNone, 0, readCb, &client);
  userEventStartTimer(printTimer, 100000, -1);
  userEventStartTimer(pingTimer, static_cast<uint64_t>(gInterval*1000000.0), -1);
  asyncLoop(base);
  return 0;
}
