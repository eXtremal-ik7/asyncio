// Shared command-line front end of the SMTP examples. Every example accepts
// the same nine arguments and needs the same preparation - resolve the
// server address and select the transport mode - so main() of each example
// shows only its own I/O style (callbacks / sendMail / coroutines).
#pragma once

#include "asyncio/socket.h"
#include "asyncio/smtp.h"

#include "p2putils/uriParse.h"

#include <stdio.h>
#include <string.h>

#if !defined(OS_WINDOWS)
#include <netdb.h>
#endif

struct SmtpArgs {
  HostAddress serverAddress;
  SmtpServerType serverType;
  bool startTls;
  const char *clientHost;
  const char *login;
  const char *password;
  const char *from;
  const char *to;
  const char *subject;
  const char *text;
};

// Returns 0 on success; otherwise prints the diagnostic and returns the
// process exit code.
inline int parseSmtpArgs(int argc, char **argv, SmtpArgs &args)
{
  if (argc != 10) {
    fprintf(stderr, "usage: %s <server:port> <type> <client host> <login> <password> <from> <to> <subject> <text>\n", argv[0]);
    return 1;
  }

  const char *server = argv[1];
  const char *type = argv[2];
  args.clientHost = argv[3];
  args.login = argv[4];
  args.password = argv[5];
  args.from = argv[6];
  args.to = argv[7];
  args.subject = argv[8];
  args.text = argv[9];

  // Build HostAddress for server
  {
    URI uri;
    if (!uriParseHostPort(server, &uri, 0) || uri.port == 0) {
      fprintf(stderr, "Invalid server %s\nIt must have address:port format\n", server);
      return 1;
    }

    args.serverAddress.family = AF_INET;
    args.serverAddress.port = static_cast<uint16_t>(uri.port);
    if (uri.hostType == URI::HostTypeIPv4) {
      args.serverAddress.ipv4 = uri.ipv4;
    } else if (uri.hostType == URI::HostTypeDNS) {
      hostent *host = gethostbyname(uri.domain.c_str());
      if (!host || !host->h_addr) {
        fprintf(stderr, " * cannot retrieve address of %s (gethostbyname failed)\n", uri.domain.c_str());
        return 1;
      }
      memcpy(&args.serverAddress.ipv4, host->h_addr, sizeof(args.serverAddress.ipv4));
    } else {
      fprintf(stderr, "IPv6 address is not supported by this example\n");
      return 1;
    }
  }

  // Analyze type
  args.serverType = smtpServerPlain;
  args.startTls = false;
  if (strcmp(type, "plain") == 0) {
    args.serverType = smtpServerPlain;
  } else if (strcmp(type, "smtps") == 0) {
    args.serverType = smtpServerSmtps;
  } else if (strcmp(type, "starttls") == 0) {
    args.serverType = smtpServerPlain;
    args.startTls = true;
  } else {
    fprintf(stderr, "Invalid server type\nAvailable types: plain, smtps, starttls\n");
    return 1;
  }

  return 0;
}
