# Settings shared by every test target; variables and CMAKE_CXX_FLAGS set
# here propagate into each add_subdirectory scope.
set(CMAKE_CXX_STANDARD 17)

if (WIN32)
  set(PLATFORM_LIBRARIES ws2_32 mswsock)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

add_subdirectory(unittests)
add_subdirectory(performance)
add_subdirectory(stress)
