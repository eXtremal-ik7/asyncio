// HTTP token table generator.
//
// Single source of truth for the reserved HTTP header names and the request
// methods. Emits two files from the lists below:
//   <src>/include/p2putils/HttpParseCommon.h - public token enums
//   <src>/p2putils/HttpTokenTableData.h      - perfect hash tables (data only)
//
// The table is a CHD-style minimal perfect hash: a 64-bit FNV-1a hash of the
// (case-folded) name selects a displacement via its high bits, the displaced
// low bits select the single slot a name can live in. Displacements are
// searched here, at generation time, so a lookup probes exactly one slot.
// User-defined recognition tables are built at runtime with the same layout
// by httpHeaderTablePrepare, which also embeds the reserved names below.
//
// The generator is not wired into the build; run it manually after editing
// the lists:
//   c++ -std=c++17 -O2 -o httptokengen HttpTokenGen.cpp
//   ./httptokengen ../../..   (path to the repository "src" directory)
//
// Both output files are committed; test http.reserved_tokens checks that the
// committed tables, enums and the runtime lookup agree.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <numeric>
#include <string>
#include <vector>

// Reserved header names: the library recognizes these with every table (the
// built-in one and every user table built by httpHeaderTablePrepare, whose
// lists may not contain them). The parser interprets Content-Length and
// Transfer-Encoding (message framing); the rest are only delivered with
// their ids, reserving the name for a future library feature: adding a name
// here later silently blinds user code that string-matches it under an
// entryType == 0 gate, so the set errs on the generous side. Enum values are
// hhReservedBase + 1-based list position.
static const struct { const char *name; const char *enumName; const char *comment; } reservedHeaderNames[] = {
  {"Content-Length",    "hhContentLength",    "framing: the value is parsed as the body length"},
  {"Transfer-Encoding", "hhTransferEncoding", "framing: \"chunked\" detection"},
  {"Content-Encoding",  "hhContentEncoding",  "reserved for automatic decompression"},
  {"Connection",        "hhConnection",       "connection lifecycle: close / keep-alive / upgrade"},
  {"Keep-Alive",        "hhKeepAlive",        "keep-alive parameters"},
  {"Host",              "hhHost",             "server-side validation and routing"},
  {"Expect",            "hhExpect",           "server-side 100-continue"},
  {"Upgrade",           "hhUpgrade",          "protocol switching (websocket, h2c)"},
  {"Location",          "hhLocation",         "client-side redirects"}
};

// The recognition table of the httpParseDefault callback: the reserved names
// ride along as in every table, plus the names the callback itself consumes.
// The composition is an implementation detail of the callback - extending it
// is not a contract change; the ids are ordinary user-range ids private to
// this table (a user never sees them: the callback consumption is exposed
// through HTTPParseDefaultContext fields).
static const struct { const char *name; const char *enumName; } defaultParserHeaderNames[] = {
  {"Content-Type", "hpdContentType"}
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
  for (const auto &header : reservedHeaderNames)
    headers.push_back(Token{header.name, header.enumName, fnv1a(header.name, false)});
  std::vector<Token> defaultParserHeaders = headers;
  for (const auto &header : defaultParserHeaderNames)
    defaultParserHeaders.push_back(Token{header.name, header.enumName, fnv1a(header.name, false)});
  std::vector<Token> methods;
  for (const auto &method : methodNames)
    methods.push_back(Token{method.name, method.enumName, fnv1a(method.name, true)});

  PerfectHashTable headerTable = buildSmallestTable(headers, "reserved headers");
  PerfectHashTable defaultParserTable = buildSmallestTable(defaultParserHeaders, "default parser headers");
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
    out << "// Reserved header ids: names the library recognizes with every recognition\n";
    out << "// table - the built-in one (a NULL table argument) and every table built by\n";
    out << "// httpHeaderTablePrepare (user lists may not contain these names). The\n";
    out << "// parser interprets Content-Length and Transfer-Encoding (message framing),\n";
    out << "// the rest are delivered with these ids for the client or a future library\n";
    out << "// feature. Bit 30 keeps the values positive ints usable as C case labels\n";
    out << "// and clear of the user id range [1, 0x3FFFFFFF]; hhUnknown (0) is reported\n";
    out << "// for a valid name missing from the table.\n";
    out << "enum {\n";
    out << "  hhUnknown = 0,\n";
    out << "  hhReservedBase = 0x40000000,\n";
    for (size_t i = 0; i < headers.size(); i++) {
      const char *comment = reservedHeaderNames[i].comment;
      out << "  " << headers[i].enumName << (i + 1 < headers.size() ? "," : " ")
          << "  // " << comment << "\n";
    }
    out << "};\n";
    out << "\n";
    out << "// Ids of the recognition table owned by the httpParseDefault callback (the\n";
    out << "// asyncio http client installs that table via httpParseDefaultInit). The\n";
    out << "// composition of the table is an implementation detail of the callback and\n";
    out << "// may change in any release; the ids are ordinary user-range ids of that\n";
    out << "// table, not reserved ones.\n";
    out << "enum {\n";
    {
      const size_t count = sizeof(defaultParserHeaderNames)/sizeof(defaultParserHeaderNames[0]);
      for (size_t i = 0; i < count; i++)
        out << "  " << defaultParserHeaderNames[i].enumName << " = " << (i + 1)
            << (i + 1 < count ? "," : "") << "\n";
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
    out << "#include \"HttpTokens.h\"\n";
    out << "#include <stdint.h>\n";
    out << "\n";
    out << "// lower case + charset validation, for header names\n";
    emitByteTable(out, "httpTokenFoldTable", false);
    out << "\n";
    out << "// charset validation only, for case-sensitive methods\n";
    emitByteTable(out, "httpTokenCharTable", true);
    out << "\n";
    emitTokenTable(out, "HttpHeaderReserved", headerTable);
    out << "\n";
    emitTokenTable(out, "HttpParseDefault", defaultParserTable);
    out << "\n";
    emitTokenTable(out, "HttpMethod", methodTable);
  }

  printf("written: %s\n", commonPath.c_str());
  printf("written: %s\n", dataPath.c_str());
  return 0;
}
