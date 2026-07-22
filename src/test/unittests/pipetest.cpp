#include "unittest.h"

#include "asyncio/device.h"

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <thread>

#ifdef OS_COMMONUNIX
#include <sys/resource.h>
#include <unistd.h>
#endif

void test_pipe_writecb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  EXPECT_EQ(status, aosSuccess);
  if (status != aosSuccess)
    postQuitOperation(ctx->base);
}

void test_pipe_readcb(AsyncOpStatus status, aioObject*, size_t transferred, void *arg)
{
  TestContext *ctx = static_cast<TestContext*>(arg);
  reqStruct *req = reinterpret_cast<reqStruct*>(ctx->serverBuffer);
  EXPECT_EQ(status, aosSuccess);
  EXPECT_EQ(transferred, sizeof(reqStruct));
  if (status == aosSuccess && transferred == sizeof(reqStruct)) {
    EXPECT_EQ(req->a, 11);
    EXPECT_EQ(req->b, 77);
  }

  postQuitOperation(ctx->base);
}

TEST(pipe, test_pipe)
{
  TestContext context(gBase);
  pipeTy unnamedPipe;
  int result = pipeCreate(&unnamedPipe, 1);
  EXPECT_EQ(result, 0);
  if (result == 0) {
    reqStruct req;
    context.pipeRead = newDeviceIo(gBase, unnamedPipe.read);
    context.pipeWrite = newDeviceIo(gBase, unnamedPipe.write);
    req.a = 11;
    req.b = 77;
    aioRead(context.pipeRead, context.serverBuffer, sizeof(req), afWaitAll, 1000000, test_pipe_readcb, &context);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    aioWrite(context.pipeWrite, &req, sizeof(req), afWaitAll, 0, test_pipe_writecb, &context);
    asyncLoop(gBase);
    deleteAioObject(context.pipeRead);
    deleteAioObject(context.pipeWrite);
  }
}

// The read half of the device sync fast path: bytes already buffered in the
// pipe complete aioRead right on the calling thread - the loop never runs in
// this test, and the fire-and-forget return value is the only completion
// channel, deliberately independent of the callback budget (api.h). POSIX
// devices have always taken this path; on Windows it pins deviceSyncRead
// probing the pipe instead of unconditionally deferring to the IOCP.
TEST(pipe, test_pipe_buffered_data_completes_read_inline)
{
  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);

  const char payload[] = {'p', 'i', 'n', 'g'};
#ifdef OS_WINDOWS
  // the write end stays a bare handle (no aioObject, no IOCP association),
  // so a plain event-driven overlapped write lands the bytes deterministically
  OVERLAPPED overlapped;
  memset(&overlapped, 0, sizeof(overlapped));
  overlapped.hEvent = CreateEvent(NULL, TRUE, FALSE, NULL);
  ASSERT_NE(overlapped.hEvent, (HANDLE)NULL);
  DWORD bytesWritten = 0;
  if (!WriteFile(unnamedPipe.write, payload, sizeof(payload), &bytesWritten, &overlapped)) {
    ASSERT_EQ(GetLastError(), (DWORD)ERROR_IO_PENDING);
    ASSERT_EQ(WaitForSingleObject(overlapped.hEvent, 5000), WAIT_OBJECT_0);
    ASSERT_TRUE(GetOverlappedResult(unnamedPipe.write, &overlapped, &bytesWritten, FALSE));
  }
  ASSERT_EQ(bytesWritten, sizeof(payload));
  CloseHandle(overlapped.hEvent);
#else
  ASSERT_EQ(write(unnamedPipe.write, payload, sizeof(payload)),
            (ssize_t)sizeof(payload));
#endif

  aioObject *pipeRead = newDeviceIo(gBase, unnamedPipe.read);
  char buffer[8] = {};
  ssize_t result = aioRead(pipeRead, buffer, sizeof(payload), afNone, 1000000,
                           nullptr, nullptr);

  EXPECT_EQ(result, (ssize_t)sizeof(payload))
    << "buffered pipe data must complete the read synchronously";
  EXPECT_EQ(memcmp(buffer, payload, sizeof(payload)), 0);

  deleteAioObject(pipeRead);
#ifdef OS_WINDOWS
  CloseHandle(unnamedPipe.write);
#else
  close(unnamedPipe.write);
#endif
}
#ifdef OS_COMMONUNIX
// Same contract for the device path: writing to a pipe whose reader is gone
// must not kill the process. write() has no MSG_NOSIGNAL equivalent, so the
// shield depends on the platform and init flags, and each variant below pins
// one of them:
// - aiNone on Darwin/NetBSD: F_SETNOSIGPIPE set by newDeviceIo (per-fd);
// - aiNone on Linux/FreeBSD: SIGPIPE masked around the write itself
//   (sigpipeGuard, libpq pattern) - there is no per-fd opt-out there;
// - aiIgnoreSigpipe anywhere: process-wide SIG_IGN, masking is skipped.
// Fully deterministic: the kernel tracks pipe readers synchronously, so the
// first write() after the last reader closed raises SIGPIPE, no timing
// involved.
static void writeTwiceAfterPipeReaderClosed(AsyncInitFlags initFlags)
{
  alarm(30);  // if anything blocks, fail the death test instead of hanging it
  initializeAsyncIo(initFlags);

  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);
  close(unnamedPipe.read);

  aioObject *object = newDeviceIo(createAsyncBase(amOSDefault, 1), unnamedPipe.write);
  const char payload[] = "ping";
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  aioWrite(object, payload, sizeof(payload), afNone, 0, nullptr, nullptr);
  exit(0);
}

TEST(PipeDeathTest, write_after_reader_closed)
{
  EXPECT_EXIT(writeTwiceAfterPipeReaderClosed(aiNone), ::testing::ExitedWithCode(0), "");
}

TEST(PipeDeathTest, ignore_sigpipe_flag)
{
  EXPECT_EXIT(writeTwiceAfterPipeReaderClosed(aiIgnoreSigpipe), ::testing::ExitedWithCode(0), "");
}
#endif

#ifdef OS_COMMONUNIX
void test_pipe_reader_close_writecb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  ErrorWakeupContext *ctx = static_cast<ErrorWakeupContext*>(arg);
  ctx->status = status;
  ctx->callbackFired = true;
  postQuitOperation(ctx->base);
}

TEST(pipe, test_pipe_reader_close_wakes_parked_write)
{
  ErrorWakeupContext context(gBase);
  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);

  // Fill the pipe so the write below parks instead of completing inline
  char chunk[4096];
  memset(chunk, 0, sizeof(chunk));
  while (write(unnamedPipe.write, chunk, sizeof(chunk)) > 0)
    continue;
  ASSERT_EQ(errno, EAGAIN);

  aioObject *pipeWrite = newDeviceIo(gBase, unnamedPipe.write);
  char payload = 'x';
  aioWrite(pipeWrite, &payload, sizeof(payload), afNone, 1000000, test_pipe_reader_close_writecb, &context);

  close(unnamedPipe.read);

  asyncLoop(gBase);
  EXPECT_TRUE(context.callbackFired);
  EXPECT_NE(context.status, aosSuccess);
  EXPECT_NE(context.status, aosTimeout) << "the reader closing did not wake the parked write";
  deleteAioObject(pipeWrite);
}

// An idle object must not cost CPU, whatever state its fd is in. epoll
// registers the fd with an empty event mask right at object creation
// (epollNewAioObject) and going idle re-arms it with an empty mask, but
// EPOLLERR/EPOLLHUP cannot be masked out: the kernel reports them for as
// long as the condition holds. With no operation parked there is nothing to
// consume the condition, the event translates to an empty mask and is
// swallowed, so every epoll_wait returns immediately and the loop thread
// burns a full core until the object is deleted. Probed on the real kernel
// (scratchpad/acceptprobe/epollstorm.c): an fd registered with events=0
// floods EPOLLERR every call, while an EPOLLONESHOT-disarmed fd stays fully
// silent - so a parked operation does not storm and this is specifically
// about idle objects. A pipe whose reader is gone is the deterministic worst
// case: the error condition is permanent, nothing can consume it. kqueue
// registers filters per operation and iocp has no readiness reporting, so
// the contract holds there as is; the burn is epoll-only.
// The loop runs in its own thread so it can spin while this thread sleeps
// and meters the process CPU clock; the idle baseline over the window is
// microseconds, four orders of magnitude under the threshold.
TEST(pipe, test_error_on_idle_object_keeps_loop_asleep)
{
  asyncBase *base = createAsyncBase(amOSDefault, 1);
  std::thread loopThread([base]() { asyncLoop(base); });

  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);
  aioObject *pipeWrite = newDeviceIo(base, unnamedPipe.write);
  close(unnamedPipe.read);  // permanent error condition on an idle fd

  // let the loop thread face the condition before metering starts
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  rusage before, after;
  ASSERT_EQ(getrusage(RUSAGE_SELF, &before), 0);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  ASSERT_EQ(getrusage(RUSAGE_SELF, &after), 0);

  auto cpuMicroseconds = [](const rusage &usage) {
    return static_cast<uint64_t>(usage.ru_utime.tv_sec + usage.ru_stime.tv_sec) * 1000000 +
           static_cast<uint64_t>(usage.ru_utime.tv_usec + usage.ru_stime.tv_usec);
  };
  EXPECT_LT(cpuMicroseconds(after) - cpuMicroseconds(before), 200000u)
    << "the loop thread spins on the error condition of an idle object";

  deleteAioObject(pipeWrite);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  postQuitOperation(base);
  loopThread.join();
}
#endif

#ifdef OS_WINDOWS
// Both halves of the device EOF contract: a pipe whose peer is gone completes
// the read with aosDisconnected, whether ReadFile refuses right at submit
// (peer closed first) or the close finishes an already parked overlapped read.
void test_pipe_eof_readcb(AsyncOpStatus status, aioObject*, size_t, void *arg)
{
  ErrorWakeupContext *ctx = static_cast<ErrorWakeupContext*>(arg);
  ctx->status = status;
  ctx->callbackFired = true;
  postQuitOperation(ctx->base);
}

TEST(pipe, test_pipe_read_after_writer_closed)
{
  ErrorWakeupContext context(gBase);
  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);

  aioObject *pipeRead = newDeviceIo(gBase, unnamedPipe.read);
  CloseHandle(unnamedPipe.write);

  char payload;
  aioRead(pipeRead, &payload, sizeof(payload), afNone, 1000000, test_pipe_eof_readcb, &context);
  asyncLoop(gBase);
  EXPECT_TRUE(context.callbackFired);
  EXPECT_EQ(context.status, aosDisconnected);
  deleteAioObject(pipeRead);
}

TEST(pipe, test_pipe_writer_close_wakes_parked_read)
{
  ErrorWakeupContext context(gBase);
  pipeTy unnamedPipe;
  ASSERT_EQ(pipeCreate(&unnamedPipe, 1), 0);

  aioObject *pipeRead = newDeviceIo(gBase, unnamedPipe.read);
  char payload;
  aioRead(pipeRead, &payload, sizeof(payload), afNone, 1000000, test_pipe_eof_readcb, &context);
  std::this_thread::sleep_for(std::chrono::milliseconds(10));

  CloseHandle(unnamedPipe.write);

  asyncLoop(gBase);
  EXPECT_TRUE(context.callbackFired);
  EXPECT_EQ(context.status, aosDisconnected);
  deleteAioObject(pipeRead);
}
#endif
