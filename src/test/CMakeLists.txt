add_subdirectory(unittest)
add_subdirectory(udptest)
add_subdirectory(queuetest)

if (ASYNCIO_ENABLE_ZMTP)
  add_subdirectory(zmtptest)
endif()
