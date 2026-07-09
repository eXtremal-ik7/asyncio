cxxpm_add_package(gtest default)
set (CMAKE_CXX_STANDARD 17)

# GTest::gtest is a GLOBAL imported target and carries its own include dirs, so
# it works regardless of whether the gtest package convenience variables were
# populated in this scope (they are only set on the first cxxpm_add_package).
# This binary provides its own main(), so gtest_main is intentionally omitted.
set(LIBRARIES asyncio-0.5 GTest::gtest)

if (WIN32)
  set(LIBRARIES ${LIBRARIES} ws2_32 mswsock)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

add_executable(clocksteptest clocksteptest.cpp)
target_link_libraries(clocksteptest ${LIBRARIES})
