set(Sources
  asyncioextras.cpp
)

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

add_library(asyncioextras-0.5 STATIC ${Sources})
target_include_directories(asyncioextras-0.5 PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../asyncio)

install(
  TARGETS asyncioextras-0.5
  ARCHIVE DESTINATION lib
)
