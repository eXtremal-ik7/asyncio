set (CMAKE_CXX_STANDARD 17)

if (WIN32)
  set(LIBRARIES asyncio-0.5 asyncioextras-0.5 ws2_32 mswsock)
else()
  set(LIBRARIES asyncio-0.5 asyncioextras-0.5)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

add_executable(connectstress
  connectstress.cpp
)

target_link_libraries(connectstress ${LIBRARIES})
