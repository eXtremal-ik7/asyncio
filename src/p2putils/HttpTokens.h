#pragma once

#include "p2putils/CommonParse.h"

#ifdef __cplusplus
extern "C" {
#endif

// Recognizes an HTTP header field name / request method via the generated
// perfect hash tables (HttpTokenTableData.h), validating the RFC 9110 token
// charset, folding the case (header names only) and verifying the matched
// key, so a hash collision can not misidentify a token:
//  - Ok: *p points at the terminator (':' or ' '), *token is the hh*/hm*
//    value or hhUnknown/hmUnknown for a valid name missing from the table
//  - NeedMoreData: the terminator was not reached, *p is left untouched
//  - Error: empty name or a byte outside of the token charset
ParserResultTy httpHeaderNameLookup(const char **p, const char *end, int *token);
ParserResultTy httpMethodLookup(const char **p, const char *end, int *token);

#ifdef __cplusplus
}
#endif
