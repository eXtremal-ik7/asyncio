#include "smtpargs.h"

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"
#include "asyncio/smtp.h"

#include <stdio.h>
#include <stdlib.h>

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
  SmtpArgs args;
  int parseResult = parseSmtpArgs(argc, argv, args);
  if (parseResult != 0)
    return parseResult;

  Context context;
  initializeAsyncIo(aiNone);
  asyncBase *base = createAsyncBase(amOSDefault, 1);

  HostAddress localHost;
  localHost.ipv4 = INADDR_ANY;
  localHost.family = AF_INET;
  localHost.port = 0;
  SMTPClient *client = smtpClientNew(base, localHost, args.serverType);

  context.Base = base;
  aioSmtpSendMail(client, args.serverAddress, args.startTls, args.clientHost, args.login, args.password,
                  args.from, args.to, args.subject, args.text, afNone, 5000000, responseCb, &context);
  asyncLoop(base);
  return 0;
}
