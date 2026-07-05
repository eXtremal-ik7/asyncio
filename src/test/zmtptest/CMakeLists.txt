include_directories(${ZMQ_INCLUDE_DIRECTORY})

if (WIN32)
  set(LIBRARIES asyncio-0.5 asyncioextras-0.5 ws2_32 mswsock ZeroMQ::libzmq-static iphlpapi)
else()
  set(LIBRARIES asyncio-0.5 asyncioextras-0.5 ZeroMQ::libzmq-static)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

add_executable(zmtptest
  zmtptest.cpp
)

target_link_libraries(zmtptest ${LIBRARIES})
