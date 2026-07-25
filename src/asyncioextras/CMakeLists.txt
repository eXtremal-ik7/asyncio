set(Sources)

if (ASYNCIO_ENABLE_ZMTP)
  set(Sources ${Sources} zmtp.cpp)
endif()


if (ASYNCIO_ENABLE_BTC)
  include_directories(${OPENSSL_INCLUDE_DIRECTORY})
  set(Sources ${Sources} btc.cpp)
endif()

if (ASYNCIO_ENABLE_RLPX)
    set(Sources ${Sources} rlpx.cpp)
endif()

if (CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  add_definitions(-fPIC)
endif ()

if (NOT Sources)
  # Every protocol is disabled, but consumers link the target unconditionally:
  # a static library still needs one translation unit to exist.
  set(Sources ${CMAKE_CURRENT_BINARY_DIR}/asyncioextras-empty.cpp)
  file(WRITE ${Sources} "")
endif()

add_library(asyncioextras-0.6 STATIC ${Sources})
target_include_directories(asyncioextras-0.6 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../asyncio)

install(
  TARGETS asyncioextras-0.6
  ARCHIVE DESTINATION lib
)
