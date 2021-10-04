// Parser responsible for parsing sql statement using ANTLR grammar.

#ifndef PELTON_SQLAST_PARSER_H_
#define PELTON_SQLAST_PARSER_H_

#include <memory>
#include <shared_mutex>
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
#include "pelton/sqlast/ast.h"

namespace pelton {
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
  static std::shared_mutex MTX;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_PARSER_H_
