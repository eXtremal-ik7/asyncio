add_executable(udptest udptest.cpp)
target_link_libraries(udptest asyncio-0.5 ${PLATFORM_LIBRARIES})

if (ASYNCIO_ENABLE_ZMTP)
  add_executable(zmtptest zmtptest.cpp)
  target_include_directories(zmtptest PRIVATE ${ZMQ_INCLUDE_DIRECTORY})
  target_link_libraries(
    zmtptest
    asyncio-0.5
    asyncioextras-0.5
    ZeroMQ::libzmq-static
    ${PLATFORM_LIBRARIES}
  )
  if (WIN32)
    target_link_libraries(zmtptest iphlpapi)
  endif()
endif()
