#ifndef __ASYNCIO_BASE64_H_
#define __ASYNCIO_BASE64_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

// Length helpers return the required BUFFER SIZE, including the NUL that
// base64Encode/base64Decode always append; the functions themselves return
// the payload length without it. base64GetDecodeLength counts the valid
// prefix of `in` and stays sane on malformed padding. The encode helper
// returns 0 when the required capacity cannot be represented by size_t.
size_t base64GetDecodeLength(const char *in);
size_t base64getEncodeLength(size_t len);
size_t base64Decode(uint8_t *out, const char *in);
size_t base64Encode(char *out, const uint8_t *in, size_t size);

#ifdef __cplusplus
}
#endif

#endif //__ASYNCIO_BASE64_H_
