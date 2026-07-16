#include "asyncio/device.h"
#include <stdlib.h>

iodevTy serialPortOpen(const char *name)
{
  HANDLE hFile =
    CreateFile(name, GENERIC_READ|GENERIC_WRITE, 0, NULL,
               OPEN_EXISTING, FILE_FLAG_OVERLAPPED, NULL);
  if (hFile != INVALID_HANDLE_VALUE)
    return hFile;
  else
    return 0;
}

void serialPortClose(iodevTy port)
{
  CloseHandle(port);
}

int serialPortSetConfig(iodevTy port,
                        int speed,
                        int dataBits,
                        int stopBits,
                        int parity)
{
  DCB dcb;
  if (GetCommState(port, &dcb)) {
    // Supported baud rates; an unknown request falls back to 9600.
    static const struct {
      int rate;
      DWORD value;
    } baudTable[] = {
      {110, CBR_110},
      {300, CBR_300},
      {600, CBR_600},
      {1200, CBR_1200},
      {2400, CBR_2400},
      {4800, CBR_4800},
      {9600, CBR_9600},
      {19200, CBR_19200},
      {38400, CBR_38400},
      {57600, CBR_57600},
      {115200, CBR_115200},
    };

    dcb.BaudRate = CBR_9600;
    for (size_t i = 0; i < sizeof(baudTable) / sizeof(baudTable[0]); i++) {
      if (baudTable[i].rate == speed) {
        dcb.BaudRate = baudTable[i].value;
        break;
      }
    }

    dcb.ByteSize = dataBits;
    if (stopBits == 1)
      dcb.StopBits = ONESTOPBIT;
    else
      dcb.StopBits = TWOSTOPBITS;

    if (parity == 'N') {
      dcb.Parity = NOPARITY;
      dcb.fParity = FALSE;
    } else if (parity == 'E') {
      dcb.Parity = EVENPARITY;
      dcb.fParity = TRUE;
    } else {
      dcb.Parity = ODDPARITY;
      dcb.fParity = TRUE;
    }

    dcb.fTXContinueOnXoff = TRUE;
    dcb.fOutX = FALSE;
    dcb.fInX = FALSE;
    dcb.fBinary = TRUE;
    dcb.fAbortOnError = FALSE;
    serialPortFlush(port);
    if (SetCommState(port, &dcb))
      return 1;
  }

  return 0;
}

void serialPortFlush(iodevTy port)
{
  PurgeComm(port, PURGE_RXCLEAR | PURGE_TXCLEAR |
                  PURGE_RXABORT | PURGE_TXABORT);
}

int pipeCreate(struct pipeTy *pipePtr, int isAsync)
{
  char pipeName[] = "\\\\.\\pipe\\00000000000000000000000000000000";

  for (unsigned i = 0; i < 32; i++) {
    for (unsigned i = 0; i < 32; i++)
      pipeName[i+9] = 'a' + (rand() % ('z'-'a'));

    HANDLE hPipe = CreateNamedPipe(pipeName,
                                   PIPE_ACCESS_DUPLEX | (isAsync ? FILE_FLAG_OVERLAPPED : 0),
                                   PIPE_TYPE_BYTE | PIPE_READMODE_BYTE,
                                   PIPE_UNLIMITED_INSTANCES,
                                   4096,
                                   4096,
                                   0,
                                   NULL);

    if (hPipe == INVALID_HANDLE_VALUE) {
      continue;
    }

    HANDLE hPipe2 = CreateFile(pipeName,
                               GENERIC_READ | GENERIC_WRITE,
                               0,
                               NULL,
                               OPEN_EXISTING,
                               FILE_FLAG_OVERLAPPED,
                               NULL);

    if (hPipe2 == INVALID_HANDLE_VALUE) {
      CloseHandle(hPipe);
      continue;
    }

    pipePtr->read = hPipe;
    pipePtr->write = hPipe2;
    return 0;
  }

  return -1;
}

void pipeClose(struct pipeTy pipePtr)
{
  CloseHandle(pipePtr.write);
  CloseHandle(pipePtr.read);
}

int deviceSyncRead(iodevTy hDevice, void *buffer, size_t size, int waitAll, size_t *bytesTransferred)
{
  *bytesTransferred = 0;
  return 0;
}

int deviceSyncWrite(iodevTy hDevice, const void *buffer, size_t size, int waitAll, size_t *bytesTransferred)
{
  *bytesTransferred = 0;
  return 0;
}
