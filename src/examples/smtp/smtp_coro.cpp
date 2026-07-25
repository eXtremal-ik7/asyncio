#include "smtpargs.h"

#include "asyncio/asyncio.h"
#include "asyncio/coroutine.h"
#include "asyncio/socket.h"
#include "asyncio/smtp.h"

#include <stdio.h>
#include <stdlib.h>

struct Context {
  asyncBase *Base;
  SMTPClient *Client;
  SmtpArgs Args;
};

static void doSmtp(int status, SMTPResult *result, bool *acc)
{
  if (*acc) {
    *acc = status == 0;
    if (status != 0) {
      int opStatus = -status;
      if (opStatus == smtpInvalidFormat)
        fprintf(stderr, "SMTP Protocol mismatch\n");
      else if (opStatus == smtpError)
        fprintf(stderr, "SMTP Error code: %u; text: %s\n", result->code, result->response ? result->response : "?");
      else
        fprintf(stderr, "Error %i\n", opStatus);
    } else if (result->response) {
      fprintf(stdout, "--> %s\n", result->response);
      fflush(stdout);
    }
  }
  smtpResultFree(result);
}

void sendMailCoro(void *arg)
{
  bool acc = true;
  Context *context = static_cast<Context*>(arg);
  std::string ehlo = (std::string) "EHLO " + context->Args.clientHost;
  std::string from = (std::string) "MAIL From: <" + context->Args.from + ">";
  std::string to = (std::string) "RCPT To: <" + context->Args.to + ">";
  std::string text = (std::string) "From: " + context->Args.from + "\r\n" + "To: " + context->Args.to + "\r\n" +
                     "Subject: " + context->Args.subject + "\r\n" + context->Args.text + "\r\n.";
  SMTPResult result = {};
  int status;

  // Workflow like Haskell's MayBe TCP connect
  status = ioSmtpConnect(context->Client, context->Args.serverAddress, &result, 5000000);
  doSmtp(status, &result, &acc);
  // EHLO <localhost>
  status = ioSmtpCommand(context->Client, ehlo.c_str(), &result, afNone, 5000000);
  doSmtp(status, &result, &acc);
  if (context->Args.startTls) {
    // STARTTLS
    status = ioSmtpStartTls(context->Client, &result, afNone, 5000000);
    doSmtp(status, &result, &acc);
    // EHLO <localhost>
    status = ioSmtpCommand(context->Client, ehlo.c_str(), &result, afNone, 5000000);
    doSmtp(status, &result, &acc);
  }
  // AUTH LOGIN
  status = ioSmtpLogin(context->Client, context->Args.login, context->Args.password, &result, afNone, 5000000);
  doSmtp(status, &result, &acc);
  // MAIL From
  status = ioSmtpCommand(context->Client, from.c_str(), &result, afNone, 5000000);
  doSmtp(status, &result, &acc);
  // RCPT To
  status = ioSmtpCommand(context->Client, to.c_str(), &result, afNone, 5000000);
  doSmtp(status, &result, &acc);
  // DATA
  status = ioSmtpCommand(context->Client, "DATA", &result, afNone, 5000000);
  doSmtp(status, &result, &acc);
  // <email text>
  status = ioSmtpCommand(context->Client, text.c_str(), &result, afNone, 5000000);
  doSmtp(status, &result, &acc);

  postQuitOperation(context->Base);
}

int main(int argc, char**argv)
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
