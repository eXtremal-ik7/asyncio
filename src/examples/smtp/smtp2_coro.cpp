#include "smtpargs.h"

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"
#include "asyncio/smtp.h"

#include <stdio.h>
#include <stdlib.h>

struct Context {
  asyncBase *Base;
  SMTPClient *Client;
  SmtpArgs Args;
};

void sendMailCoro(void *arg)
{
  Context *context = static_cast<Context*>(arg);
  int result = ioSmtpSendMail(context->Client,
                 context->Args.serverAddress,
                 context->Args.startTls,
                 context->Args.clientHost,
                 context->Args.login,
                 context->Args.password,
                 context->Args.from,
                 context->Args.to,
                 context->Args.subject,
                 context->Args.text,
                 afNone,
                 5000000);

  int code = smtpClientGetResultCode(context->Client);
  const char *response = smtpClientGetResponse(context->Client);
  if (result != 0) {
    int status = -result;
    if (status == smtpInvalidFormat)
      fprintf(stderr, "SMTP Protocol mismatch\n");
    else if (status == smtpError)
      fprintf(stderr, "SMTP Error code: %u; text: %s\n", code, response ? response : "?");
    else
      fprintf(stderr, "Error %i\n", status);
  } else if (response) {
    fprintf(stdout, "--> %s\n", response);
    fflush(stdout);
  }

  postQuitOperation(context->Base);
}

int main(int argc, char **argv)
{
  Context context;
  int parseResult = parseSmtpArgs(argc, argv, context.Args);
  if (parseResult != 0)
    return parseResult;

  initializeAsyncIo(aiNone);
  asyncBase *base = createAsyncBase(amOSDefault, 1);

  HostAddress localHost;
  localHost.ipv4 = INADDR_ANY;
  localHost.family = AF_INET;
  localHost.port = 0;
  context.Client = smtpClientNew(base, localHost, context.Args.serverType);

  context.Base = base;
  coroutineTy *coro = coroutineNew(sendMailCoro, &context, 0x10000);
  coroutineCall(coro);
  asyncLoop(base);
  return 0;
}
