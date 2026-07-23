#ifndef __XMSTREAM_H_
#define __XMSTREAM_H_

#include "p2putils/strExtras.h"
#include <new>
#include <stdint.h>
#include <string.h>
#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif

class xmstream {
private:
  uint8_t *_m;
  uint8_t *_p;
  size_t _size;
  size_t _msize;
  
  int _eof;
  int _own;
  
public:
  xmstream(void *data, size_t size) : _m((uint8_t*)data), _p((uint8_t*)data), _size(size), _msize(size), _eof(0), _own(0) {}
  xmstream(size_t size = 64) : _size(0), _msize(size), _eof(0), _own(1) {
    _m = size <= static_cast<size_t>(PTRDIFF_MAX)
           ? static_cast<uint8_t*>(operator new(size, std::nothrow))
           : nullptr;
    if (!_m)
      _msize = 0;
    _p = _m;
  }
  
  ~xmstream() {
    if (_own)
      operator delete(_m);
  }

  xmstream(const xmstream &s) {
    size_t sourceOffset = s.offsetOf();
    _size = s._size;
    _msize = s._msize;
    _own = s._own;
    _eof = s._eof;
    if (_own) {
      _m = static_cast<uint8_t*>(operator new(_msize, std::nothrow));
      if (_m) {
        if (_size)
          memcpy(_m, s._m, _size);
      } else {
        _size = 0;
        _msize = 0;
        _eof = 1;
      }
    } else {
      _m = s._m;
    }

    _p = _m ? _m + sourceOffset : nullptr;
  }

  xmstream(xmstream &&s) :
    _m(s._m),
    _p(s._p),
    _size(s._size),
    _msize(s._msize),
    _eof(s._eof),
    _own(s._own) {
    s._m = nullptr;
  }
  
  size_t offsetOf() const { return _m ? static_cast<size_t>(_p - _m) : 0; }
  size_t sizeOf() const { return _size; }
  size_t capacity() const { return _msize; }
  bool own() const { return _own; }
  size_t remaining() const { return _size - offsetOf(); }
  int eof() const { return _eof; }
  
  void *data() const { return _m; }
  template<typename T> T *data() const { return (T*)_m; }
  template<typename T> T *ptr() const { return (T*)_p; }

  void *capture() {
    void *data = _m;
    _m = nullptr;
    _msize = 0;
    _own = 0;
    return data;
  }
  
  void *reserve(size_t size) {
    _eof = 0;
    size_t offset = offsetOf();
    const size_t maxSize = static_cast<size_t>(PTRDIFF_MAX);
    if (offset > maxSize || size > maxSize - offset)
      return nullptr;

    size_t required = offset + size;
    if (required > _msize) {
      size_t newSize = required;
      if (_msize && _msize <= maxSize / 2) {
        size_t doubledSize = _msize * 2;
        if (doubledSize > newSize)
          newSize = doubledSize;
      }

      uint8_t *newData = static_cast<uint8_t*>(operator new(newSize, std::nothrow));
      if (!newData)
        return nullptr;

      if (_size)
        memcpy(newData, _m, _size);
      if (_own)
        operator delete(_m);
      _m = newData;
      _msize = newSize;
      _size = required;
      uint8_t *p = _m + offset;
      _p = p + size;
      _own = 1;
      return p;
    } else {
      void *p = _p;
      _p += size;
      _size = required < _size ? _size : required;
      return p;
    }
  }
  
  template<typename T> T *reserve(size_t num) {
    if (num > SIZE_MAX / sizeof(T))
      return nullptr;
    return static_cast<T*>(reserve(num*sizeof(T)));
  }
  
  // pointer move functions
  void reset() {
    _p = _m;
    _size = 0;
    _eof = 0;
  }

  void seekSet(size_t offset) {
    _p = _m + ((offset < _size) ? offset : _size);
  }
  
  // read functions
  template<typename T=uint8_t> T *seek(ssize_t num) {
    void *old = _p;
    _p += sizeof(T)*num;

    ssize_t newSize = _p - _m;
    if (newSize < 0) {
      _p = _m;
    } else if (static_cast<size_t>(newSize) > _size) {
      _p = _m + _size;
      _eof = 1;
      return nullptr;
    }

    return reinterpret_cast<T*>(old);
  }

  template<typename T=uint8_t> void seekEnd(size_t num, bool setEof = false) {
    size_t size = (num > SIZE_MAX / sizeof(T)) ? SIZE_MAX : sizeof(T)*num;
    _p = _m + (_size - std::min(size, _size));
    if (num == 0 && setEof)
      _eof = true;
  }
  
  void read(void *data, size_t size) {
    if (void *p = seek(size))
      memcpy(data, p, size);
  }
  
  // the cursor moves by arbitrary byte offsets, so scalar accesses go through
  // fixed-size memcpy: compiles to the same single unaligned load/store, but
  // without the alignment UB of dereferencing a misaligned T*
  template<typename T> T read() {
    T *p = seek<T>(1);
    T value;
    if (!p)
      return T();
    memcpy(&value, p, sizeof(T));
    return value;
  }

  template<typename T> T readle() {
    return xletoh<T>(read<T>());
  }

  template<typename T> T readbe() {
    return xbetoh<T>(read<T>());
  }

  // write functions
  bool write(const void *data, size_t size) {
    void *target = reserve(size);
    if (!target)
      return false;
    memcpy(target, data, size);
    return true;
  }
  template<typename T> bool write(const T& data) { return write(&data, sizeof(T)); }
  template<typename T> bool writele(T data) { T value = xhtole(data); return write(&value, sizeof(T)); }
  template<typename T> bool writebe(T data) { T value = xhtobe(data); return write(&value, sizeof(T)); }

  bool write(const char *data) {
    return write(data, strlen(data));
  }

  void truncate() {
    _size = _p - _m;
  }
};

#endif //__XMSTREAM_H_
