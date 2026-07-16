#include "smtpargs.h"

#include "asyncio/asyncio.h"
#include "asyncio/socket.h"
#include "asyncio/smtp.h"

#include <stdio.h>
#include <stdlib.h>

enum SendEmailStateTy {
  stStartTls = 0,
  stEhlo2,
  stAuth,
  stFrom,
  stTo,
  stDataMessage,
  stData,
  stLast
};

struct Context {
  asyncBase *Base;
  SendEmailStateTy State;
  SmtpArgs Args;
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

  switch (context->State) {
    case stStartTls : {
      context->State = stEhlo2;
      aioSmtpStartTls(client, afNone, 5000000, responseCb, arg);
      break;
    }

    case stEhlo2 : {
      context->State = stAuth;
      std::string ehlo = "EHLO ";
      ehlo.append(context->Args.clientHost);
      aioSmtpCommand(client, ehlo.c_str(), afNone, 5000000, responseCb, arg);
      fprintf(stdout, "<-- %s\n", ehlo.c_str());
      break;
    }

    case stAuth : {
      context->State = stFrom;
      aioSmtpLogin(client, context->Args.login, context->Args.password, afNone, 5000000, responseCb, arg);
      break;
    }

    case stFrom : {
      context->State = stTo;
      std::string from = (std::string)"MAIL From: <" + context->Args.from + ">";
      aioSmtpCommand(client, from.c_str(), afNone, 5000000, responseCb, arg);
      fprintf(stdout, "<-- %s\n", from.c_str());
      break;
    }

    case stTo : {
      context->State = stDataMessage;
      std::string to = (std::string)"RCPT To: <" + context->Args.to + ">";
      aioSmtpCommand(client, to.c_str(), afNone, 5000000, responseCb, arg);
      fprintf(stdout, "<-- %s\n", to.c_str());
      break;
    }

    case stDataMessage : {
      context->State = stData;
      aioSmtpCommand(client, "DATA", afNone, 5000000, responseCb, arg);
      fprintf(stdout, "<-- %s\n", "DATA");
      break;
    }

    case stData : {
      context->State = stLast;
      std::string text = (std::string)
        "From: " + context->Args.from + "\r\n" +
        "To: " + context->Args.to + "\r\n" +
        "Subject: " + context->Args.subject + "\r\n" +
        context->Args.text + "\r\n.\r\n";
      aioSmtpCommand(client, text.c_str(), afNone, 5000000, responseCb, arg);
      fprintf(stdout, "<-- %s\n", text.c_str());
      break;
    }

    case stLast : {
      postQuitOperation(context->Base);
      break;
    }
  }
}

void connectCb(AsyncOpStatus status, SMTPClient *client, void *arg)
{
  Context *context = static_cast<Context*>(arg);
  if (status != aosSuccess) {
    fprintf(stderr, "Error %i\n", status);
    postQuitOperation(context->Base);
    return;
  }

  const char *response = smtpClientGetResponse(client);
  if (response) {
    fprintf(stdout, "--> %s\n", response);
    fflush(stdout);
  }

  std::string ehlo = "EHLO ";
  ehlo.append(context->Args.clientHost);
  aioSmtpCommand(client, ehlo.c_str(), afNone, 5000000, responseCb, arg);
  fprintf(stdout, "<-- %s\n", ehlo.c_str());
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
  SMTPClient *client = smtpClientNew(base, localHost, context.Args.serverType);

  context.Base = base;
  context.State = context.Args.startTls ? stStartTls : stAuth;
  aioSmtpConnect(client, context.Args.serverAddress, 5000000, connectCb, &context);

  asyncLoop(base);
  return 0;
}
