// Parser responsible for parsing sql statement using ANTLR grammar.

#ifndef K9DB_SQLAST_PARSER_H_
#define K9DB_SQLAST_PARSER_H_

#include <memory>
#include <string>

#include "absl/status/statusor.h"
// ANTLR runtime library.
// NOLINTNEXTLINE
#include "antlr4-runtime.h"
// C++ generated code from ANTLR Sqlite grammar.
#include "SQLiteLexer.h"
#undef CHECK  // CHECK is defined by glog, but also used as a member name inside
              // ANTLR generated classes within SQLiteParser...
#include "SQLiteParser.h"
#include "k9db/sqlast/ast.h"

namespace k9db {
namespace sqlast {

class SQLParser : public antlr4::BaseErrorListener {
 public:
  SQLParser() = default;

  absl::StatusOr<std::unique_ptr<AbstractStatement>> Parse(
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

}  // namespace sqlast
}  // namespace k9db

#endif  // K9DB_SQLAST_PARSER_H_
