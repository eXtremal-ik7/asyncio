if (CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  add_definitions(-fPIC)
endif ()

add_library(p2putils
  HttpParse.cpp
  HttpRequestParse.cpp
  HttpTokens.c
  UriParse.cpp
)

install(
  TARGETS p2putils
  ARCHIVE DESTINATION lib
)
