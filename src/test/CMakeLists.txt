add_subdirectory(unittest)
add_subdirectory(udptest)
add_subdirectory(queuetest)
add_subdirectory(lifetimetest)

if (ASYNCIO_ENABLE_ZMTP)
  add_subdirectory(zmtptest)
endif()
