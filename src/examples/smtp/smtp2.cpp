#include "asyncio/asyncio.h"
#include "asyncio/socket.h"
#include "asyncio/smtp.h"

#include "p2putils/uriParse.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if !defined(OS_WINDOWS)
#include <netdb.h>
#endif

struct Context {
  asyncBase *Base;
};

void responseCb(AsyncOpStatus status, unsigned code, SMTPClient *client, void *arg)
{
  Context *context = static_cast<Context*>(arg);
  if (status != aosSuccess) {
    SmtpOpStatus smtpStatus = static_cast<SmtpOpStatus>(status);
    if (smtpStatus == smtpInvalidFormat)
      fprintf(stderr, "SMTP Protocol mismatch\n");
    else if (smtpStatus == smtpError)
      fprintf(stderr, "SMTP Error code: %u; text: %s\n", code, smtpClientGetResponse(client) ? smtpClientGetResponse(client) : "?");
    else
      fprintf(stderr, "Error %i\n", status);

    postQuitOperation(context->Base);
    return;
  }

  const char *response = smtpClientGetResponse(client);
  if (response) {
    fprintf(stdout, "--> %s\n", response);
    fflush(stdout);
  }

  postQuitOperation(context->Base);
  return;
}

int main(int argc, char **argv)
{
  if (argc != 10) {
    fprintf(stderr, "usage: %s <server:port> <type> <client host> <login> <password> <from> <to> <subject> <text>\n", argv[0]);
    return 1;
  }

  Context context;
  const char *server = argv[1];
  const char *type = argv[2];
  const char *clientHost = argv[3];
  const char *login = argv[4];
  const char *password = argv[5];
  const char *from = argv[6];
  const char *to = argv[7];
  const char *subject = argv[8];
  const char *text = argv[9];

  HostAddress smtpAddress;
  SmtpServerType serverType = smtpServerPlain;

  // Build HostAddress for server
  {
    char *colonPos = (char*)strchr(server, ':');
    if (colonPos == nullptr) {
      fprintf(stderr, "Invalid server %s\nIt must have address:port format\n", server);
      return 1;
    }

    *colonPos = 0;
    hostent *host = gethostbyname(server);
    if (!host) {
      fprintf(stderr, " * cannot retrieve address of %s (gethostbyname failed)\n", server);
    }

    u_long addr = host->h_addr ? *reinterpret_cast<u_long*>(host->h_addr) : 0;
    if (!addr) {
      fprintf(stderr, " * cannot retrieve address of %s (gethostbyname returns 0)\n", server);
      return 1;
    }

    smtpAddress.family = AF_INET;
    smtpAddress.ipv4 = static_cast<uint32_t>(addr);
    smtpAddress.port = htons(atoi(colonPos + 1));
  }

  // Analyze type
  bool startTls = false;
  if (strcmp(type, "plain") == 0) {
    serverType = smtpServerPlain;
  } else if (strcmp(type, "smtps") == 0) {
    serverType = smtpServerSmtps;
  } else if (strcmp(type, "starttls") == 0) {
    serverType = smtpServerPlain;
    startTls = true;
  } else {
    fprintf(stderr, "Invalid server type\nAvailable types: plain, smtps, starttls\n");
    return 1;
  }


  initializeSocketSubsystem();
  asyncBase *base = createAsyncBase(amOSDefault);

  HostAddress localHost;
  localHost.ipv4 = INADDR_ANY;
  localHost.family = AF_INET;
  localHost.port = 0;
  SMTPClient *client = smtpClientNew(base, localHost, serverType);

  context.Base = base;
  aioSmtpSendMail(client, smtpAddress, startTls, clientHost, login, password, from, to, subject, text, afNone, 5000000, responseCb, &context);
  asyncLoop(base);
  return 0;
}
