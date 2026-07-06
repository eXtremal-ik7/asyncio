// HTTP token table generator.
//
// Single source of truth for the known HTTP header names and request methods.
// Emits two files from the lists below:
//   <src>/include/p2putils/HttpParseCommon.h - public token enums
//   <src>/p2putils/HttpTokenTableData.h      - perfect hash tables (data only)
//
// The table is a CHD-style minimal perfect hash: a 64-bit FNV-1a hash of the
// (case-folded) name selects a displacement via its high bits, the displaced
// low bits select the single slot a name can live in. Displacements are
// searched here, at generation time, so a lookup probes exactly one slot.
//
// The generator is not wired into the build; run it manually after editing
// the lists:
//   c++ -std=c++17 -O2 -o httptokengen HttpTokenGen.cpp
//   ./httptokengen ../../..   (path to the repository "src" directory)
//
// Both output files are committed; test http.known_tokens_coverage checks
// that the committed tables, enums and the runtime lookup agree.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <numeric>
#include <string>
#include <vector>

// Header field names, alphabetical. Sources: RFC 9110/9111/9112 (core HTTP),
// RFC 6265 (cookies), WHATWG Fetch/CORS, RFC 6455 (WebSocket), W3C security
// and fetch-metadata headers, plus widely deployed de-facto X-* names.
// Enum identifiers are derived by dropping '-' and prepending "hh".
static const char *headerNames[] = {
  "Accept",
  "Accept-CH",
  "Accept-Charset",
  "Accept-Encoding",
  "Accept-Language",
  "Accept-Ranges",
  "Access-Control-Allow-Credentials",
  "Access-Control-Allow-Headers",
  "Access-Control-Allow-Methods",
  "Access-Control-Allow-Origin",
  "Access-Control-Expose-Headers",
  "Access-Control-Max-Age",
  "Access-Control-Request-Headers",
  "Access-Control-Request-Method",
  "Age",
  "Allow",
  "Alt-Svc",
  "Authentication-Info",
  "Authorization",
  "Cache-Control",
  "Connection",
  "Content-Disposition",
  "Content-Encoding",
  "Content-Language",
  "Content-Length",
  "Content-Location",
  "Content-Range",
  "Content-Security-Policy",
  "Content-Type",
  "Cookie",
  "Date",
  "ETag",
  "Expect",
  "Expires",
  "Forwarded",
  "From",
  "Host",
  "If-Match",
  "If-Modified-Since",
  "If-None-Match",
  "If-Range",
  "If-Unmodified-Since",
  "Keep-Alive",
  "Last-Modified",
  "Link",
  "Location",
  "Max-Forwards",
  "Origin",
  "Permissions-Policy",
  "Pragma",
  "Priority",
  "Proxy-Authenticate",
  "Proxy-Authentication-Info",
  "Proxy-Authorization",
  "Range",
  "Referer",
  "Referrer-Policy",
  "Retry-After",
  "Sec-CH-UA",
  "Sec-CH-UA-Mobile",
  "Sec-CH-UA-Platform",
  "Sec-Fetch-Dest",
  "Sec-Fetch-Mode",
  "Sec-Fetch-Site",
  "Sec-Fetch-User",
  "Sec-WebSocket-Accept",
  "Sec-WebSocket-Extensions",
  "Sec-WebSocket-Key",
  "Sec-WebSocket-Protocol",
  "Sec-WebSocket-Version",
  "Server",
  "Server-Timing",
  "Set-Cookie",
  "Strict-Transport-Security",
  "TE",
  "Trailer",
  "Transfer-Encoding",
  "Upgrade",
  "Upgrade-Insecure-Requests",
  "User-Agent",
  "Vary",
  "Via",
  "Warning",
  "WWW-Authenticate",
  "X-Content-Type-Options",
  "X-Forwarded-For",
  "X-Forwarded-Host",
  "X-Forwarded-Proto",
  "X-Frame-Options",
  "X-Real-IP",
  "X-Requested-With",
  "X-XSS-Protection"
};

// Request methods (RFC 9110, case-sensitive); order is part of the ABI
static const struct { const char *name; const char *enumName; } methodNames[] = {
  {"GET", "hmGet"},
  {"HEAD", "hmHead"},
  {"POST", "hmPost"},
  {"PUT", "hmPut"},
  {"DELETE", "hmDelete"},
  {"CONNECT", "hmConnect"},
  {"OPTIONS", "hmOptions"},
  {"TRACE", "hmTrace"},
  {"PATCH", "hmPatch"}
};

// RFC 9110 tchar: characters allowed in tokens
static bool isTokenChar(unsigned c)
{
  return (c >= 'a' && c <= 'z') ||
         (c >= 'A' && c <= 'Z') ||
         (c >= '0' && c <= '9') ||
         c == '!' || c == '#' || c == '$' || c == '%' || c == '&' ||
         c == '\'' || c == '*' || c == '+' || c == '-' || c == '.' ||
         c == '^' || c == '_' || c == '`' || c == '|' || c == '~';
}

static unsigned char foldChar(unsigned c, bool caseSensitive)
{
  if (!isTokenChar(c))
    return 0;
  if (!caseSensitive && c >= 'A' && c <= 'Z')
    return static_cast<unsigned char>(c + ('a' - 'A'));
  return static_cast<unsigned char>(c);
}

static uint64_t fnv1a(const std::string &name, bool caseSensitive)
{
  uint64_t hash = 0xcbf29ce484222325ull;
  for (char c : name) {
    unsigned char folded = foldChar(static_cast<unsigned char>(c), caseSensitive);
    if (folded == 0) {
      fprintf(stderr, "error: '%s' contains a character outside of the HTTP token charset\n", name.c_str());
      exit(1);
    }
    hash = (hash ^ folded) * 0x100000001b3ull;
  }
  return hash;
}

static std::string enumNameForHeader(const std::string &name)
{
  std::string result = "hh";
  for (char c : name) {
    if (c != '-')
      result.push_back(c);
  }
  return result;
}

struct Token {
  std::string name;
  std::string enumName;
  uint64_t hash;
};

struct PerfectHashTable {
  size_t size;            // slot count, power of two
  size_t groupCount;      // displacement count, power of two
  std::vector<uint16_t> displacement;
  std::vector<const Token*> slots;
};

// CHD: keys are grouped by the high hash bits, groups are placed one by one
// (largest first) by searching a displacement that maps every key of the
// group onto free slots; with the sizes tried here a solution practically
// always exists, and the search is exhaustive over all effective values
static bool buildPerfectHashTable(const std::vector<Token> &tokens, size_t size, size_t groupCount, PerfectHashTable *table)
{
  std::vector<std::vector<const Token*>> groups(groupCount);
  for (const Token &token : tokens)
    groups[(token.hash >> 32) & (groupCount - 1)].push_back(&token);

  std::vector<size_t> order(groupCount);
  std::iota(order.begin(), order.end(), 0);
  std::stable_sort(order.begin(), order.end(), [&groups](size_t l, size_t r) {
    return groups[l].size() > groups[r].size();
  });

  table->size = size;
  table->groupCount = groupCount;
  table->displacement.assign(groupCount, 0);
  table->slots.assign(size, nullptr);

  for (size_t group : order) {
    if (groups[group].empty())
      break;

    bool placed = false;
    for (size_t d = 0; d < size && !placed; d++) {
      std::vector<size_t> positions;
      for (const Token *token : groups[group]) {
        size_t pos = (static_cast<uint32_t>(token->hash) + d) & (size - 1);
        if (table->slots[pos] || std::find(positions.begin(), positions.end(), pos) != positions.end())
          break;
        positions.push_back(pos);
      }

      if (positions.size() == groups[group].size()) {
        for (size_t i = 0; i < positions.size(); i++)
          table->slots[positions[i]] = groups[group][i];
        table->displacement[group] = static_cast<uint16_t>(d);
        placed = true;
      }
    }

    if (!placed)
      return false;
  }

  return true;
}

static PerfectHashTable buildSmallestTable(const std::vector<Token> &tokens, const char *what)
{
  for (const Token &l : tokens) {
    for (const Token &r : tokens) {
      if (&l != &r && l.hash == r.hash) {
        fprintf(stderr, "error: full hash collision between '%s' and '%s', change the hash function\n",
                l.name.c_str(), r.name.c_str());
        exit(1);
      }
    }
  }

  size_t size = 1;
  while (size < tokens.size())
    size *= 2;

  for (; size <= 65536; size *= 2) {
    for (size_t groupCount = size; groupCount >= std::max<size_t>(size / 4, 1); groupCount /= 2) {
      PerfectHashTable table;
      if (buildPerfectHashTable(tokens, size, groupCount, &table)) {
        printf("%s: %zu tokens, %zu slots, %zu displacements\n", what, tokens.size(), size, groupCount);
        return table;
      }
    }
  }

  fprintf(stderr, "error: no perfect hash table found for %s\n", what);
  exit(1);
}

static void emitByteTable(std::ofstream &out, const char *name, bool caseSensitive)
{
  out << "static const unsigned char " << name << "[256] = {";
  for (unsigned c = 0; c < 256; c++) {
    if (c % 16 == 0)
      out << "\n  ";
    char buf[8];
    snprintf(buf, sizeof(buf), "0x%02x,", foldChar(c, caseSensitive));
    out << buf << (c % 16 != 15 ? " " : "");
  }
  out << "\n};\n";
}

static void emitTokenTable(std::ofstream &out, const char *prefix, const PerfectHashTable &table)
{
  out << "enum {\n";
  out << "  " << prefix << "TableSize = " << table.size << ",\n";
  out << "  " << prefix << "GroupCount = " << table.groupCount << "\n";
  out << "};\n\n";

  out << "static const uint16_t " << prefix << "Displacement[" << table.groupCount << "] = {";
  for (size_t i = 0; i < table.groupCount; i++) {
    if (i % 16 == 0)
      out << "\n  ";
    out << table.displacement[i] << "," << (i % 16 != 15 && i != table.groupCount-1 ? " " : "");
  }
  out << "\n};\n\n";

  out << "static const HttpTokenEntry " << prefix << "Table[" << table.size << "] = {\n";
  for (const Token *token : table.slots) {
    if (token) {
      char hash[32];
      snprintf(hash, sizeof(hash), "0x%016llxull", static_cast<unsigned long long>(token->hash));
      out << "  {\"" << token->name << "\", " << token->name.size() << ", " << token->enumName
          << ", " << hash << "},\n";
    } else {
      out << "  {0, 0, 0, 0},\n";
    }
  }
  out << "};\n";
}

int main(int argc, char **argv)
{
  if (argc != 2) {
    fprintf(stderr, "usage: %s <path-to-src-directory>\n", argv[0]);
    return 1;
  }

  std::vector<Token> headers;
  for (const char *name : headerNames)
    headers.push_back(Token{name, enumNameForHeader(name), fnv1a(name, false)});
  std::vector<Token> methods;
  for (const auto &method : methodNames)
    methods.push_back(Token{method.name, method.enumName, fnv1a(method.name, true)});

  for (size_t i = 1; i < headers.size(); i++) {
    if (!std::lexicographical_compare(headers[i-1].name.begin(), headers[i-1].name.end(),
                                      headers[i].name.begin(), headers[i].name.end(),
                                      [](char l, char r) { return foldChar(static_cast<unsigned char>(l), false) <
                                                                  foldChar(static_cast<unsigned char>(r), false); })) {
      fprintf(stderr, "error: header list is not sorted or contains a duplicate: '%s' >= '%s'\n",
              headers[i-1].name.c_str(), headers[i].name.c_str());
      return 1;
    }
  }

  PerfectHashTable headerTable = buildSmallestTable(headers, "headers");
  PerfectHashTable methodTable = buildSmallestTable(methods, "methods");

  const std::string root = argv[1];
  const std::string commonPath = root + "/include/p2putils/HttpParseCommon.h";
  const std::string dataPath = root + "/p2putils/HttpTokenTableData.h";

  {
    std::ofstream out(commonPath, std::ios::trunc);
    if (!out) {
      fprintf(stderr, "error: can not write %s\n", commonPath.c_str());
      return 1;
    }

    out << "// Generated by p2putils/tokengen, do not edit: token lists live in\n";
    out << "// tokengen/HttpTokenGen.cpp, see build instructions there\n";
    out << "#ifndef __LIBP2P_HTTPPARSECOMMON_H_\n";
    out << "#define __LIBP2P_HTTPPARSECOMMON_H_\n";
    out << "\n";
    out << "#ifdef __cplusplus\n";
    out << "extern \"C\" {\n";
    out << "#endif\n";
    out << "\n";
    out << "// Header name tokens, sorted alphabetically; hhUnknown is reported for names\n";
    out << "// missing from the table. Values are compiled into both the library and its\n";
    out << "// users: inserting a new name renumbers the tail, so a full rebuild of all\n";
    out << "// library consumers is required after any change here.\n";
    out << "enum {\n";
    out << "  hhUnknown = 0,\n";
    for (size_t i = 0; i < headers.size(); i++) {
      out << "  " << headers[i].enumName;
      if (i == 0)
        out << " = 1";
      out << (i + 1 < headers.size() ? "," : "") << "\n";
    }
    out << "};\n";
    out << "\n";
    out << "// Request method tokens (RFC 9110: method names are case-sensitive);\n";
    out << "// hmUnknown is reported for extension methods missing from the table\n";
    out << "enum {\n";
    out << "  hmUnknown = 0,\n";
    for (size_t i = 0; i < methods.size(); i++)
      out << "  " << methods[i].enumName << (i + 1 < methods.size() ? "," : "") << "\n";
    out << "};\n";
    out << "\n";
    out << "#ifdef __cplusplus\n";
    out << "}\n";
    out << "#endif\n";
    out << "\n";
    out << "#endif //__LIBP2P_HTTPPARSECOMMON_H_\n";
  }

  {
    std::ofstream out(dataPath, std::ios::trunc);
    if (!out) {
      fprintf(stderr, "error: can not write %s\n", dataPath.c_str());
      return 1;
    }

    out << "// Generated by p2putils/tokengen, do not edit: token lists live in\n";
    out << "// tokengen/HttpTokenGen.cpp, see build instructions there.\n";
    out << "//\n";
    out << "// CHD perfect hash tables: FNV-1a over the folded name, the high hash bits\n";
    out << "// select a displacement, the displaced low bits select the only slot the\n";
    out << "// name can occupy: slot = (low32(hash) + displacement[high32(hash) &\n";
    out << "// (GroupCount-1)]) & (TableSize-1). Byte tables map characters outside of\n";
    out << "// the RFC 9110 token charset to 0 and fold the case where appropriate.\n";
    out << "//\n";
    out << "// Include this file only from HttpTokens.c\n";
    out << "\n";
    out << "#include \"p2putils/HttpParseCommon.h\"\n";
    out << "#include <stdint.h>\n";
    out << "\n";
    out << "typedef struct HttpTokenEntry {\n";
    out << "  const char *name;\n";
    out << "  uint32_t length;\n";
    out << "  int32_t value;\n";
    out << "  uint64_t hash;\n";
    out << "} HttpTokenEntry;\n";
    out << "\n";
    out << "// lower case + charset validation, for header names\n";
    emitByteTable(out, "httpTokenFoldTable", false);
    out << "\n";
    out << "// charset validation only, for case-sensitive methods\n";
    emitByteTable(out, "httpTokenCharTable", true);
    out << "\n";
    emitTokenTable(out, "HttpHeader", headerTable);
    out << "\n";
    emitTokenTable(out, "HttpMethod", methodTable);
  }

  printf("written: %s\n", commonPath.c_str());
  printf("written: %s\n", dataPath.c_str());
  return 0;
}
