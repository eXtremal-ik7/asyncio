/* ====================================================================
 * Copyright (c) 1995-1999 The Apache Group.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. All advertising materials mentioning features or use of this
 *    software must display the following acknowledgment:
 *    "This product includes software developed by the Apache Group
 *    for use in the Apache HTTP server project (http://www.apache.org/)."
 *
 * 4. The names "Apache Server" and "Apache Group" must not be used to
 *    endorse or promote products derived from this software without
 *    prior written permission. For written permission, please contact
 *    apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache"
 *    nor may "Apache" appear in their names without prior written
 *    permission of the Apache Group.
 *
 * 6. Redistributions of any form whatsoever must retain the following
 *    acknowledgment:
 *    "This product includes software developed by the Apache Group
 *    for use in the Apache HTTP server project (http://www.apache.org/)."
 *
 * THIS SOFTWARE IS PROVIDED BY THE APACHE GROUP ``AS IS'' AND ANY
 * EXPRESSED OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE APACHE GROUP OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Group and was originally based
 * on public domain software written at the National Center for
 * Supercomputing Applications, University of Illinois, Urbana-Champaign.
 * For more information on the Apache Group and the Apache HTTP server
 * project, please see <http://www.apache.org/>.
 *
 */

#include "asyncio/base64.h"

static const unsigned char base64DecodeTable[256] = {
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
  52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
  64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
  15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
  64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
  41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
  64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64
};

static const char base64EncodeTable[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

size_t base64GetDecodeLength(const char *in)
{
  // Count the valid prefix only - exactly what base64Decode consumes. No
  // padding subtraction: malformed '=' runs can no longer underflow the size
  const uint8_t *p = (const uint8_t*)in;
  while (base64DecodeTable[*p++] <= 63)
    continue;
  size_t n = (size_t)(p - (const uint8_t*)in) - 1;
  return n/4*3 + n%4*3/4 + 1;
}


size_t base64getEncodeLength(size_t len)
{
  size_t groups = len/3 + (len%3 != 0);
  if (groups > (SIZE_MAX-1)/4)
    return 0;
  return groups*4 + 1;
}

size_t base64Decode(uint8_t *out, const char *in)
{
  size_t bytesDecoded;
  size_t bytesRemaining;
  uint8_t *pOut = (uint8_t*)out;
  const uint8_t *pIn = (const uint8_t*)in;

  {
    const uint8_t *p = (const uint8_t*)in;
    while (base64DecodeTable[*(p++)] <= 63)
      continue;
    bytesRemaining = (p - (const unsigned char *) in) - 1;
    bytesDecoded = bytesRemaining/4*3 + bytesRemaining%4*3/4;
  }

  while (bytesRemaining > 4) {
    pOut[0] = (uint8_t) (base64DecodeTable[*pIn] << 2 | base64DecodeTable[pIn[1]] >> 4);
    pOut[1] = (uint8_t) (base64DecodeTable[pIn[1]] << 4 | base64DecodeTable[pIn[2]] >> 2);
    pOut[2] = (uint8_t) (base64DecodeTable[pIn[2]] << 6 | base64DecodeTable[pIn[3]]);
    pOut += 3;
    pIn += 4;
    bytesRemaining -= 4;
  }

  if (bytesRemaining > 1)
    *pOut++ = (uint8_t)(base64DecodeTable[*pIn] << 2 | base64DecodeTable[pIn[1]] >> 4);
  if (bytesRemaining > 2)
    *pOut++ = (uint8_t)(base64DecodeTable[pIn[1]] << 4 | base64DecodeTable[pIn[2]] >> 2);
  if (bytesRemaining > 3)
    *pOut++ = (uint8_t)(base64DecodeTable[pIn[2]] << 6 | base64DecodeTable[pIn[3]]);

  *pOut = '\0';
  return bytesDecoded;
}

size_t base64Encode(char *out, const uint8_t *in, size_t size)
{
  size_t i;
  char *p = out;
  if (size == 0) {
    *out = '\0';
    return 0;
  }
  for (i = 0; i + 2 < size; i += 3) {
    *p++ = base64EncodeTable[(in[i] >> 2) & 0x3F];
    *p++ = base64EncodeTable[((in[i] & 0x3) << 4) | (int)((in[i + 1] & 0xF0) >> 4)];
    *p++ = base64EncodeTable[((in[i + 1] & 0xF) << 2) | (int)((in[i + 2] & 0xC0) >> 6)];
    *p++ = base64EncodeTable[in[i + 2] & 0x3F];
  }

  if (i < size) {
    *p++ = base64EncodeTable[(in[i] >> 2) & 0x3F];
    if (i == (size - 1)) {
      *p++ = base64EncodeTable[((in[i] & 0x3) << 4)];
      *p++ = '=';
    } else {
      *p++ = base64EncodeTable[((in[i] & 0x3) << 4) | ((int) (in[i + 1] & 0xF0) >> 4)];
      *p++ = base64EncodeTable[((in[i + 1] & 0xF) << 2)];
    }

    *p++ = '=';
  }

  *p = 0;
  return p - out;
}
