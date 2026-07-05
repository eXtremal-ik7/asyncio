#include "asyncio/device.h"
#include "asyncioImpl.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <string.h>
#include <termios.h>
#include <time.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/types.h>

int sigpipeIgnored = 0;

void sigpipeGuardEnter(struct SigpipeGuard *guard)
{
  sigset_t pipeMask;
  sigset_t pendingSet;
  sigemptyset(&pipeMask);
  sigaddset(&pipeMask, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &pipeMask, &guard->savedMask);
  // Snapshot after blocking: from here on only our own write can add a
  // pending SIGPIPE to this thread.
  sigpending(&pendingSet);
  guard->wasPending = sigismember(&pendingSet, SIGPIPE);
}

void sigpipeGuardLeave(struct SigpipeGuard *guard, int consumeSigpipe)
{
  int savedErrno = errno;
  if (consumeSigpipe && !guard->wasPending) {
#ifndef F_SETNOSIGPIPE
    // Only reachable on platforms without per-fd suppression (Linux,
    // FreeBSD): needSigpipeGuard is always zero elsewhere, and Darwin has
    // no sigtimedwait to compile.
    sigset_t pipeMask;
    struct timespec zeroTimeout = {0, 0};
    sigemptyset(&pipeMask);
    sigaddset(&pipeMask, SIGPIPE);
    while (sigtimedwait(&pipeMask, 0, &zeroTimeout) == -1 && errno == EINTR)
      continue;
#endif
  }
  pthread_sigmask(SIG_SETMASK, &guard->savedMask, 0);
  errno = savedErrno;
}


iodevTy serialPortOpen(const char *name)
{
  int port = open(name, O_RDWR | O_NOCTTY | O_NONBLOCK);
  return port != -1 ?
    port : 0;
}

void serialPortClose(iodevTy port)
{
  close(port);
}

int serialPortSetConfig(iodevTy port,
                        int speed,
                        int dataBits,
                        int stopBits,
                        int parity)
{
  struct termios tios;
  memset(&tios, 0, sizeof(tios));

  speed_t localSpeed;
  switch (speed) {
    case 110:
      localSpeed = B110;
      break;
    case 300:
      localSpeed = B300;
      break;
    case 600:
      localSpeed = B600;
      break;
    case 1200:
      localSpeed = B1200;
      break;
    case 2400:
      localSpeed = B2400;
      break;
    case 4800:
      localSpeed = B4800;
      break;
    case 9600:
      localSpeed = B9600;
      break;
    case 19200:
      localSpeed = B19200;
      break;
    case 38400:
      localSpeed = B38400;
      break;
    case 57600:
      localSpeed = B57600;
      break;
    case 115200:
      localSpeed = B115200;
      break;
#ifndef OS_QNX
    case 230400:
      localSpeed = B230400;
      break;
#ifndef OS_DARWIN
    case 460800:
      localSpeed = B460800;
      break;
    case 921600:
      localSpeed = B921600;
      break;
#endif
#endif
    default :
      localSpeed = B9600;
  }

  if (cfsetispeed(&tios, localSpeed) == -1 ||
      cfsetospeed(&tios, localSpeed) == -1)
    return 0;

  tios.c_cflag |= (CREAD | CLOCAL);

  tios.c_cflag &= ~CSIZE;
  switch (dataBits) {
    case 5:
      tios.c_cflag |= CS5;
      break;
    case 6:
      tios.c_cflag |= CS6;
      break;
    case 7:
      tios.c_cflag |= CS7;
      break;
    case 8:
    default:
      tios.c_cflag |= CS8;
      break;
  }

  if (stopBits == 1)
    tios.c_cflag &=~ CSTOPB;
  else
    tios.c_cflag |= CSTOPB;

  if (parity == 'N') {
    tios.c_cflag &=~ PARENB;    
  } else if (parity == 'E') {
    tios.c_cflag |= PARENB;
    tios.c_cflag &=~ PARODD;
  } else {
    tios.c_cflag |= PARENB;
    tios.c_cflag |= PARODD;
  }

  tios.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);

  if (parity == 'N') {
    tios.c_iflag &= ~INPCK;
  } else {
    tios.c_iflag |= INPCK;
  }

  tios.c_oflag &= ~OPOST;
  tios.c_cc[VMIN] = 0;
  tios.c_cc[VTIME] = 0;

  tcflush(port, TCIOFLUSH);
  if (tcsetattr(port, TCSANOW, &tios) < 0) {
    return 0;
  } else {
    return 1;
  }
}

void serialPortFlush(iodevTy port)
{
  tcflush(port, TCIOFLUSH);
}

int pipeCreate(struct pipeTy *pipePtr, int isAsync)
{
  int fd[2];
  int result = pipe(fd);
  pipePtr->read = fd[0];
  pipePtr->write = fd[1];
  if (result == 0 && isAsync) {
    int rstate = fcntl(fd[0], F_GETFL);
    int wstate = fcntl(fd[1], F_GETFL);
    fcntl(fd[0], F_SETFL, O_NONBLOCK | rstate);
    fcntl(fd[1], F_SETFL, O_NONBLOCK | wstate);
  }

  return result;
}

void pipeClose(struct pipeTy pipePtr)
{
  close(pipePtr.read);
  close(pipePtr.write);
}

int deviceSyncRead(iodevTy hDevice, void *buffer, size_t size, int waitAll, size_t *bytesTransferred)
{
  if (!waitAll) {
    ssize_t result = read(hDevice, buffer, size);
    if (result > 0) {
      *bytesTransferred = (size_t)result;
      return 1;
    } else {
      return 0;
    }
  } else {
    size_t transferred = 0;
    ssize_t result;
    while (transferred != size && (result = read(hDevice, (uint8_t*)buffer + transferred, size - transferred)) > 0)
      transferred += (size_t)result;
    *bytesTransferred = transferred;
    return transferred == size;
  }
}

int deviceSyncWrite(iodevTy hDevice, const void *buffer, size_t size, int waitAll, size_t *bytesTransferred)
{
  if (!waitAll) {
    ssize_t result = write(hDevice, buffer, size);
    if (result > 0) {
      *bytesTransferred = (size_t)result;
      return 1;
    } else {
      return 0;
    }
  } else {
    size_t transferred = 0;
    ssize_t result;
    while (transferred != size && (result = write(hDevice, (uint8_t*)buffer + transferred, size - transferred)) > 0)
      transferred += (size_t)result;
    *bytesTransferred = transferred;
    return transferred == size;
  }
}
