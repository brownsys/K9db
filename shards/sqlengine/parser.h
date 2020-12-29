// Parser responsible for parsing sql statement using ANTLR grammar.

#ifndef SHARDS_SQLENGINE_PARSER_H_
#define SHARDS_SQLENGINE_PARSER_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"
// ANTLR runtime library.
// NOLINTNEXTLINE
#include "antlr4-runtime.h"
// C++ generated code from ANTLR Sqlite grammar.
#include "SQLiteLexer.h"
#include "SQLiteParser.h"
#include "shards/sqlast/ast.h"

namespace shards {
namespace sqlengine {
namespace parser {

class SQLParser : public antlr4::BaseErrorListener {
 public:
  SQLParser() {}

  absl::StatusOr<std::unique_ptr<sqlast::AbstractStatement>> Parse(
      const std::string &sql);

  void syntaxError(antlr4::Recognizer *recognizer,
                   antlr4::Token *offendingSymbol, size_t line,
                   size_t charPositionInLine, const std::string &msg,
                   std::exception_ptr e) override;

 private:
  bool error_;
  std::unique_ptr<sqlparser::SQLiteParser> parser_;
  std::unique_ptr<antlr4::CommonTokenStream> tokens_;
  std::unique_ptr<sqlparser::SQLiteLexer> lexer_;
  std::unique_ptr<antlr4::ANTLRInputStream> input_stream_;
};

}  // namespace parser
}  // namespace sqlengine
}  // namespace shards

#endif  // SHARDS_SQLENGINE_PARSER_H_
