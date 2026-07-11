# posix.s (coroutine context switch) needs the assembler on POSIX platforms only
if (NOT WIN32)
  enable_language(ASM)
endif()

set(Sources
  asyncio.c
  asyncioImpl.c
  timerWheel.c
  dynamicBuffer.c
  ringBuffer.c
  timer.c

  http.c
  smtp.c

  base64.c
)

if (ASYNCIO_ENABLE_SSL)
  include_directories(${OPENSSL_INCLUDE_DIRECTORY})
  set(Sources ${Sources} socketSSL.c)
endif()

if (WIN32)
  set(Sources ${Sources} iocp.c coroutineWin32.c deviceWin32.c socketWin32.c)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux")
  set(Sources ${Sources} epoll.c devicePosix.c socketPosix.c coroutinePosix.c ${ARCH_NAME}/posix.s)
elseif(CMAKE_SYSTEM_NAME STREQUAL "Android")
  # Same as Linux (epoll + POSIX); aarch64/x86_64 coroutine asm both present.
  set(Sources ${Sources} epoll.c devicePosix.c socketPosix.c coroutinePosix.c ${ARCH_NAME}/posix.s)
elseif (APPLE)
  set(Sources ${Sources} kqueue.c devicePosix.c socketPosix.c coroutinePosix.c ${ARCH_NAME}/posix.s)
elseif(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  set(Sources ${Sources} kqueue.c devicePosix.c socketPosix.c coroutinePosix.c ${ARCH_NAME}/posix.s)
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR CMAKE_SYSTEM_NAME STREQUAL "Android")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fPIC")
endif ()

add_library(asyncio-0.5 STATIC ${Sources})

# Build-time proof that the atomic128 layer stayed inline (nm/objdump over a
# dedicated anchor object). MSVC intrinsics cannot silently become library
# calls, and there is no binutils there to inspect the result with.
if (NOT MSVC)
  add_library(atomic128guard STATIC atomic128guard.c)
  add_custom_command(
    TARGET atomic128guard POST_BUILD
    COMMAND ${CMAKE_COMMAND}
      -DBINARY=$<TARGET_FILE:atomic128guard>
      -DNM=${CMAKE_NM}
      -DOBJDUMP=${CMAKE_OBJDUMP}
      -DARCH=${ARCH_NAME}
      -DTSAN=${BUILD_SANITIZE_THREAD}
      -P ${CMAKE_CURRENT_SOURCE_DIR}/../cmake/atomic128-guard.cmake
    COMMENT "atomic128 guard: checking for inline DWCAS"
    VERBATIM
  )
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  target_link_libraries(asyncio-0.5 rt)
endif()

install(
  TARGETS asyncio-0.5
  ARCHIVE DESTINATION lib
)
