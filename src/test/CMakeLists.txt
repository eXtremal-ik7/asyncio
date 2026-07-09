add_subdirectory(unittest)
add_subdirectory(udptest)
add_subdirectory(queuetest)
add_subdirectory(lifetimetest)

if (ASYNCIO_ENABLE_ZMTP)
  add_subdirectory(connectstress)
  add_subdirectory(zmtptest)
endif()

# Mutates the system clock: off by default, and gated again at runtime on an
# opt-in env var plus admin/root privileges (see clocksteptest.cpp).
option(ASYNCIO_ENABLE_CLOCKSTEP_TEST "Build the clock-step timeout regression test (requires admin + opt-in to run)" OFF)
if (ASYNCIO_ENABLE_CLOCKSTEP_TEST)
  add_subdirectory(clocksteptest)
endif()
