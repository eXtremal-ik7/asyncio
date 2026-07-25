set(AIOPingSources AIOPing.cpp)

if (MSVC)
  set(AIOPingSources ${AIOPingSources}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../msvc/getopt.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../../msvc/getopt1.c
  )
endif()

add_executable(aioping ${AIOPingSources})

target_link_libraries(aioping asyncio-0.6)
if (WIN32)
  target_link_libraries(aioping ws2_32 mswsock)
endif()
