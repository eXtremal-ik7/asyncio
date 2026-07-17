include("cmake/cxx-pm.cmake")
cxxpm_initialize(
  https://github.com/eXtremal-ik7/cxx-pm/archive/refs/tags/0.0.5.tar.gz
  d520a56610477f1911d1a390775e2ce19c47dfaa098d9692de5340cb174e3c43
)

cmake_minimum_required(VERSION 3.11)
project(asyncio)

set (CMAKE_CXX_STANDARD 11)

# Library features (can be overridden by an embedding project)
option(ASYNCIO_ENABLE_SSL "SSL support (OpenSSL is required)" ON)
option(ASYNCIO_ENABLE_ZMTP "ZMTP (zmq) protocol support" ON)
option(ASYNCIO_ENABLE_BTC "Bitcoin network protocol support" ON)
option(ASYNCIO_ENABLE_RLPX "RPLx (Ethereum) network protocol support" ON)
option(ASYNCIO_BUILD_TESTS "Build tests" OFF)

# Whole-program build modes. The names are shared by convention across projects in one
# build tree (cache is common, so the same option in an embedding top-level project and
# here merges into a single switch); each project applies the flags to its own scope.
option(BUILD_SANITIZE_ADDRESS "Build with address sanitizer" OFF)
option(BUILD_SANITIZE_THREAD "Build with thread sanitizer" OFF)
option(BUILD_PROFILING "Build for profiling" OFF)

if (BUILD_SANITIZE_ADDRESS AND BUILD_SANITIZE_THREAD)
  message(FATAL_ERROR "BUILD_SANITIZE_ADDRESS and BUILD_SANITIZE_THREAD are mutually exclusive")
endif()

if (BUILD_SANITIZE_ADDRESS AND NOT CMAKE_C_FLAGS MATCHES "fsanitize=address")
  if (MSVC)
    # MSVC address sanitizer: /Zi + linker /DEBUG so ASAN symbolizes stacks;
    # /INCREMENTAL:NO because incremental linking is incompatible with asan
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} /fsanitize=address /Zi")
    # STL container annotations must match across all linked objects (LNK2038);
    # cxx-pm packages (zeromq etc.) are built without asan, so disable them here
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /fsanitize=address /Zi /D_DISABLE_STRING_ANNOTATION /D_DISABLE_VECTOR_ANNOTATION")
    set (CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} /DEBUG /INCREMENTAL:NO")
  else()
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  endif()
endif()

if (BUILD_SANITIZE_THREAD)
  if (MSVC)
    message(FATAL_ERROR "Thread sanitizer is not supported by MSVC")
  endif()
  if (NOT CMAKE_C_FLAGS MATCHES "fsanitize=thread")
    set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=thread")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
  endif()
endif()

if (BUILD_PROFILING)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -g -fno-inline -fno-omit-frame-pointer")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -fno-inline -fno-omit-frame-pointer")
endif()

if (ASYNCIO_ENABLE_SSL OR ASYNCIO_ENABLE_BTC)
  cxxpm_add_package(openssl default)
endif()

if (ASYNCIO_ENABLE_ZMTP)
  cxxpm_add_package(zeromq default)
endif()

set(CMAKE_DEBUG_POSTFIX d)

if (WIN32)
  if(MSVC)
    SET (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHsc")
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/msvc/include)
    if (MSVC_VERSION LESS 1800)
      # inttypes.h/stdint.h appeared in VS2013; older compilers use bundled shims,
      # for newer ones the shims must not shadow the system headers
      include_directories(${CMAKE_CURRENT_SOURCE_DIR}/msvc/include/c99)
    endif()
  endif(MSVC)
  set(OS_WINDOWS 1)
  add_definitions(
    -D__STDC_LIMIT_MACROS
    -D__STDC_FORMAT_MACROS
  )
elseif(CMAKE_SYSTEM_NAME STREQUAL "Linux" OR CMAKE_SYSTEM_NAME STREQUAL "Android")
  # Android is a Linux kernel (epoll + POSIX sockets); reuse the Linux paths.
  set(OS_LINUX 1)
  set(OS_COMMONUNIX 1)
  # glibc gates the accept4 declaration behind _GNU_SOURCE
  add_definitions(-D_GNU_SOURCE)
elseif (APPLE)
  set(OS_DARWIN 1)
  set(OS_COMMONUNIX 1)
elseif(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  set(OS_FREEBSD 1) 
  set(OS_COMMONUNIX 1)
elseif(CMAKE_SYSTEM_NAME STREQUAL "QNX")
  set(OS_QNX 1) 
  set(OS_COMMONUNIX 1)
endif()

if (CMAKE_OSX_ARCHITECTURES)
  set(ARCH ${CMAKE_OSX_ARCHITECTURES})
else()
  set(ARCH ${CMAKE_SYSTEM_PROCESSOR})
endif()

if (ARCH STREQUAL "i386" OR ARCH STREQUAL "i686")
  set(ARCH_X86 1)
  set(ARCH_NAME "x86")
elseif (ARCH STREQUAL "x86_64" OR ARCH STREQUAL "AMD64")
  set(ARCH_X86_64 1)
  set(ARCH_NAME "x86_64")
elseif (ARCH STREQUAL "aarch64" OR ARCH STREQUAL "arm64")
  set(ARCH_AARCH64 1)
  set(ARCH_NAME "aarch64")
else()
  message(FATAL_ERROR "Unsupported processor architecture")
endif()

if(CMAKE_SIZEOF_VOID_P EQUAL 8)
  set(OS_64 1)
elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
  set(OS_32 1)
endif()

# atomic128.h: cmpxchg16b must be inlined (GCC/clang do not enable it by
# default on x86-64; MSVC needs no flag). Every 64-bit x86 CPU since ~2006
# has cx16, Windows 8.1+ refuses to boot without it.
if (ARCH_X86_64 AND NOT MSVC)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcx16")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcx16")
endif()

if(MSVC)
  add_definitions(
    -D_CRT_SECURE_NO_DEPRECATE
    -D_CRT_SECURE_NO_WARNINGS
    -D_CRT_NONSTDC_NO_DEPRECATE
  )
else(MSVC)
  set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")
  set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")
endif(MSVC)

include(TestBigEndian)
TEST_BIG_ENDIAN(IS_BIGENDIAN)
configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/include/asyncioconfig.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/include/asyncioconfig.h
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_BINARY_DIR}/include
)

add_subdirectory(asyncio)
add_subdirectory(asyncioextras)
add_subdirectory(p2putils)
add_subdirectory(examples)

if (ASYNCIO_BUILD_TESTS)
  add_subdirectory(test)
endif()
