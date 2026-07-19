#include "unittest.h"
#include "asyncioconfig.h"

#include <cstdio>
#include <cstring>

#ifdef BUILD_SANITIZE_ADDRESS
extern "C" const char *__asan_default_options()
{
  // Exercise suspended-coroutine fake-stack cleanup in every ASan unit run.
  // ASAN_OPTIONS may still override this default when investigating a failure.
  return "detect_stack_use_after_return=1";
}
#endif

asyncBase *gBase = nullptr;

namespace {

// The suite drives one global base through many tests, and a test routinely
// stops its message loop with postQuitOperation. The quit is sticky by
// contract, so rearm the base before every test; the point between tests is
// quiescent (no test leaves a loop thread running behind), which is exactly
// what resetQuitOperation requires.
class ResetQuitListener : public ::testing::EmptyTestEventListener {
  void OnTestStart(const ::testing::TestInfo&) override
  {
    resetQuitOperation(gBase);
  }
};

} // namespace

int main(int argc, char **argv)
{
  AsyncMethod method = amOSDefault;
  if (argc >= 2) {
    if (strcmp(argv[1], "default") == 0) {
      method = amOSDefault;
    } else if (strcmp(argv[1], "epoll") == 0) {
      method = amEPoll;
    } else if (strcmp(argv[1], "kqueue") == 0) {
      method = amKQueue;
    } else if (strcmp(argv[1], "iocp") == 0) {
      method = amIOCP;
    } else {
      fprintf(stderr, "ERROR: unknown method %s, default used\n", argv[1]);
    }
  }

  initializeAsyncIo(aiNone);

  gBase = createAsyncBase(method, 4);

  ::testing::InitGoogleTest(&argc, argv);
  ::testing::UnitTest::GetInstance()->listeners().Append(new ResetQuitListener);
  return RUN_ALL_TESTS();
}
