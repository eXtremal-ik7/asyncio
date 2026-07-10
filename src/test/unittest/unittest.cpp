#include "unittest.h"

#include <cstdio>
#include <cstring>

asyncBase *gBase = nullptr;

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

  gBase = createAsyncBase(method);

  ::testing::InitGoogleTest(&argc, argv);
//  rlpxSocketNew(nullptr, nullptr, nullptr, nullptr);
  return RUN_ALL_TESTS();
}
