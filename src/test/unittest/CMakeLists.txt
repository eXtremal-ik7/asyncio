cxxpm_add_package(gtest default)
set (CMAKE_CXX_STANDARD 17)

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
endif(MSVC)

set(SOURCES unittest.cpp uriparsetest.cpp)
set(LIBRARIES asyncio-0.5 asyncioextras-0.5 p2putils ${GTEST_LIBRARIES})
include_directories(${GTEST_INCLUDE_DIRS})

if (ASYNCIO_ENABLE_ZMTP)
  set(SOURCES ${SOURCES} zmtptest.cpp)
  set(LIBRARIES ${LIBRARIES} ZeroMQ::libzmq-static)
  if (WIN32)
    set(LIBRARIES ${LIBRARIES} iphlpapi)
  endif()
endif()

if (WIN32)
  set(LIBRARIES ${LIBRARIES} ws2_32 mswsock)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

add_executable(unittest ${SOURCES})
target_link_libraries(unittest ${LIBRARIES})
