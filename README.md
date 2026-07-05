# asyncio

High-performance asynchronous network I/O library for C/C++.

> **Note:** not related to the Python [`asyncio`](https://docs.python.org/3/library/asyncio.html) module — this is an independent C/C++ library that only shares the name.

- Single event loop API over native OS backends: epoll (Linux, Android), kqueue (macOS, FreeBSD), I/O completion ports (Windows)
- Composable asynchronous operations with timeouts, cancellation, object/operation pooling and a synchronous fast path
- Every primitive comes in two flavors: callback API (`aio*`) and stackful coroutine API (`io*`; x86, x86_64, aarch64)
- TCP/UDP sockets, pipes/devices, timers, user events
- Protocols on top of the core: TLS (OpenSSL), HTTP client, SMTP client, Bitcoin network protocol, ZMTP (ZeroMQ); RLPx (Ethereum) is a work-in-progress stub
- Incremental zero-copy HTTP/URI parsers (`p2putils`)

## Dependencies

The build is driven by [cxx-pm](https://github.com/eXtremal-ik7/cxx-pm), a source-based C++ package manager. It bootstraps itself at the first CMake configure and downloads/builds third-party dependencies for the enabled features (OpenSSL for SSL/BTC, ZeroMQ for ZMTP, GTest for tests), so the first configure requires network access and can take a while. Built packages are cached in `~/.cxxpm` and shared between projects.

## Linux & macOS build

```
mkdir build && cd build
cmake ../src -DCMAKE_BUILD_TYPE=Release
make -j
```

Or the same with modern CMake from the repository root:

```
cmake -S src -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

Feature options (shown with defaults):

```
-DASYNCIO_ENABLE_SSL=ON    # TLS support (OpenSSL)
-DASYNCIO_ENABLE_BTC=ON    # Bitcoin network protocol
-DASYNCIO_ENABLE_ZMTP=ON   # ZMTP (ZeroMQ) protocol
-DASYNCIO_ENABLE_RLPX=ON   # RLPx (Ethereum) protocol (stub)
-DASYNCIO_BUILD_TESTS=OFF  # unit tests (GTest)
```

## Windows build with Visual Studio

- Install latest cmake from https://cmake.org/download
- Clone git repository
- Run 'x64 Native Tools Command Prompt for VS 20xx' from start menu
- Run 'cmake-gui' from terminal
- Select source ("src" subdirectory in git repo) and your build directory, run 'Configure' (can take a long time!) and 'Generate'.
- Open Visual Studio solution and build all targets

## Force x86_64 architecture on macOS with Apple Silicon

```
mkdir x86_64-Darwin && cd x86_64-Darwin
cmake ../src -DCMAKE_OSX_ARCHITECTURES=x86_64
make -j
```

## License

[MIT](LICENSE)
