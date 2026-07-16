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

static void doSmtp(SMTPClient *client, int result, bool *acc)
{
  if (*acc) {
    *acc = result == 0;
    int code = smtpClientGetResultCode(client);
    const char *response = smtpClientGetResponse(client);
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
  }
}

void sendMailCoro(void *arg)
{
  bool acc = true;
  Context *context = static_cast<Context*>(arg);
  std::string ehlo = (std::string)"EHLO " + context->Args.clientHost;
  std::string from = (std::string)"MAIL From: <" + context->Args.from + ">";
  std::string to = (std::string)"RCPT To: <" + context->Args.to + ">";
  std::string text = (std::string)
    "From: " + context->Args.from + "\r\n" +
    "To: " + context->Args.to + "\r\n" +
    "Subject: " + context->Args.subject + "\r\n" +
    context->Args.text + "\r\n.";

  // Workflow like Haskell's MayBe
  // TCP connect
  doSmtp(context->Client, ioSmtpConnect(context->Client, context->Args.serverAddress, 5000000), &acc);
  // EHLO <localhost>
  doSmtp(context->Client, ioSmtpCommand(context->Client, ehlo.c_str(), afNone, 5000000), &acc);
  if (context->Args.startTls) {
    // STARTTLS
    doSmtp(context->Client, ioSmtpStartTls(context->Client, afNone, 5000000), &acc);
    // EHLO <localhost>
    doSmtp(context->Client, ioSmtpCommand(context->Client, ehlo.c_str(), afNone, 5000000), &acc);
  }
  // AUTH LOGIN
  doSmtp(context->Client, ioSmtpLogin(context->Client, context->Args.login, context->Args.password, afNone, 5000000), &acc);
  // MAIL From
  doSmtp(context->Client, ioSmtpCommand(context->Client, from.c_str(), afNone, 5000000), &acc);
  // RCPT To
  doSmtp(context->Client, ioSmtpCommand(context->Client, to.c_str(), afNone, 5000000), &acc);
  // DATA
  doSmtp(context->Client, ioSmtpCommand(context->Client, "DATA", afNone, 5000000), &acc);
  // <email text>
  doSmtp(context->Client, ioSmtpCommand(context->Client, text.c_str(), afNone, 5000000), &acc);

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
