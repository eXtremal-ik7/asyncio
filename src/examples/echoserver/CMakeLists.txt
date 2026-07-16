add_executable(EchoServer EchoServer.cpp)
add_executable(EchoServer_coro EchoServer_coro.cpp)

target_link_libraries(EchoServer asyncio-0.5)
target_link_libraries(EchoServer_coro asyncio-0.5)
if (WIN32)
  target_link_libraries(EchoServer ws2_32 mswsock)
  target_link_libraries(EchoServer_coro ws2_32 mswsock)
endif()
