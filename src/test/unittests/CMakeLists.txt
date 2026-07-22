cxxpm_add_package(gtest default)

if(MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /D_SILENCE_TR1_NAMESPACE_DEPRECATION_WARNING")
endif(MSVC)

set(
  SOURCES
  unittest.cpp
  testsupport.cpp
  pipetest.cpp
  sockettest.cpp
  ssltest.cpp
  httptest.cpp
  lifetimetest.cpp
  coroutinetest.cpp
  uriparsetest.cpp
  httpparsetest.cpp
  httpheadertabletest.cpp
  base64test.cpp
  coretest.cpp
  timertest.cpp
  eventtest.cpp
  looptest.cpp
  clocksteptest.cpp
)

set(LIBRARIES asyncio-0.5 asyncioextras-0.5 p2putils ${GTEST_LIBRARIES})
include_directories(${GTEST_INCLUDE_DIRS})

# asyncio/http.h pulls in socketSSL.h; http.c drags socketSSL.c into the link
if (ASYNCIO_ENABLE_SSL)
  include_directories(${OPENSSL_INCLUDE_DIRECTORY})
  set(LIBRARIES ${LIBRARIES} ${OPENSSL_LIBRARIES})
  if (WIN32)
    # static libcrypto uses the system certificate store
    set(LIBRARIES ${LIBRARIES} crypt32)
  endif()
endif()

if (ASYNCIO_ENABLE_ZMTP)
  set(SOURCES ${SOURCES} zmtptest.cpp zmtpprototest.cpp)
  set(LIBRARIES ${LIBRARIES} ZeroMQ::libzmq-static)
  if (WIN32)
    set(LIBRARIES ${LIBRARIES} iphlpapi)
  endif()
endif()

# PLATFORM_LIBRARIES comes from test/CMakeLists.txt (empty outside Windows)
set(LIBRARIES ${LIBRARIES} ${PLATFORM_LIBRARIES})

add_executable(unittest ${SOURCES})
if(MSVC)
  target_compile_definitions(unittest PRIVATE NOMINMAX)
endif()
target_include_directories(
  unittest
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../asyncio
    ${CMAKE_CURRENT_SOURCE_DIR}/../../p2putils
)
target_link_libraries(unittest ${LIBRARIES})
