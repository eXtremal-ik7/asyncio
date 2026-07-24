#ifndef __ASYNCIO_SMTP_H_
#define __ASYNCIO_SMTP_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "asyncio/asyncio.h"

typedef enum SmtpServerType {
  smtpServerPlain = 0,
  smtpServerSmtps
} SmtpServerType;

typedef enum SmtpOpStatus {
  smtpInvalidFormat = aosLast,
  smtpError
} SmtpOpStatus;

typedef struct SMTPClient SMTPClient;

typedef void smtpConnectCb(AsyncOpStatus, struct SMTPClient*, void*);
typedef void smtpResponseCb(AsyncOpStatus, unsigned code, struct SMTPClient*, void*);

SMTPClient *smtpClientNew(asyncBase *base, HostAddress localAddress, SmtpServerType type);
void smtpClientDelete(SMTPClient *client);
int smtpClientGetResultCode(SMTPClient *client);
const char *smtpClientGetResponse(SMTPClient *client);

void aioSmtpConnect(SMTPClient *client, HostAddress address, uint64_t usTimeout, smtpConnectCb callback, void *arg);
void aioSmtpStartTls(SMTPClient *client, AsyncFlags flags, uint64_t usTimeout, smtpResponseCb callback, void *arg);
void aioSmtpLogin(SMTPClient *client,
                  const char *login,
                  const char *password,
                  AsyncFlags flags,
                  uint64_t usTimeout,
                  smtpResponseCb callback,
                  void *arg);
void aioSmtpCommand(SMTPClient *client, const char *command, AsyncFlags flags, uint64_t usTimeout, smtpResponseCb callback, void *arg);

int ioSmtpConnect(SMTPClient *client, HostAddress address, uint64_t usTimeout);
int ioSmtpStartTls(SMTPClient *client, AsyncFlags flags, uint64_t usTimeout);
int ioSmtpLogin(SMTPClient *client, const char *login, const char *password, AsyncFlags flags, uint64_t usTimeout);
int ioSmtpCommand(SMTPClient *client, const char *command, AsyncFlags flags, uint64_t usTimeout);

// Single functions for sent email
void aioSmtpSendMail(SMTPClient *client,
                     HostAddress smtpServerAddress,
                     int startTls,
                     const char *localHost,
                     const char *login,
                     const char *password,
                     const char *from,
                     const char *to,
                     const char *subject,
                     const char *text,
                     AsyncFlags flags,
                     uint64_t usTimeout,
                     smtpResponseCb callback,
                     void *arg);

int ioSmtpSendMail(SMTPClient *client,
                    HostAddress smtpServerAddress,
                    int startTls,
                    const char *localHost,
                    const char *login,
                    const char *password,
                    const char *from,
                    const char *to,
                    const char *subject,
                    const char *text,
                    AsyncFlags flags,
                    uint64_t usTimeout);

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_SMTP_H_
