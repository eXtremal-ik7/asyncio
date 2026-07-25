add_executable(queuetest queuetest.cpp)
target_link_libraries(queuetest asyncio-0.6 ${PLATFORM_LIBRARIES})

add_executable(lifetimetest lifetimetest.cpp)
target_include_directories(lifetimetest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../asyncio)
target_link_libraries(lifetimetest asyncio-0.6 ${PLATFORM_LIBRARIES})

add_executable(eventstress eventstress.cpp)
target_link_libraries(eventstress asyncio-0.6 ${PLATFORM_LIBRARIES})

add_executable(timerwheelstress timerwheelstress.cpp)
target_include_directories(timerwheelstress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../asyncio)
target_link_libraries(timerwheelstress asyncio-0.6 ${PLATFORM_LIBRARIES})

if (ASYNCIO_ENABLE_ZMTP)
  add_executable(connectstress connectstress.cpp)
  target_include_directories(connectstress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../asyncio)
  target_link_libraries(connectstress asyncio-0.6 asyncioextras-0.6 ${PLATFORM_LIBRARIES})
endif()
