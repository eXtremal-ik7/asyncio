set(CMAKE_CXX_STANDARD 17)

if (WIN32)
  set(PLATFORM_LIBRARIES ws2_32 mswsock)
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif()

add_executable(queuetest queuetest.cpp)
target_link_libraries(queuetest asyncio-0.5 ${PLATFORM_LIBRARIES})

add_executable(lifetimetest lifetimetest.cpp)
target_link_libraries(lifetimetest asyncio-0.5 ${PLATFORM_LIBRARIES})

add_executable(eventstress eventstress.cpp)
target_link_libraries(eventstress asyncio-0.5 ${PLATFORM_LIBRARIES})

add_executable(timerwheelstress timerwheelstress.cpp)
target_include_directories(timerwheelstress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../asyncio)
target_link_libraries(timerwheelstress asyncio-0.5 ${PLATFORM_LIBRARIES})

if (ASYNCIO_ENABLE_ZMTP)
  add_executable(connectstress connectstress.cpp)
  target_link_libraries(connectstress asyncio-0.5 asyncioextras-0.5 ${PLATFORM_LIBRARIES})
endif()
