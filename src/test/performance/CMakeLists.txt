set(CMAKE_CXX_STANDARD 17)

if (WIN32)
  set(PLATFORM_LIBRARIES ws2_32 mswsock)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

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
