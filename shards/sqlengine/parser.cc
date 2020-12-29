// Parser responsible for parsing sql statement using ANTLR grammar.

#include "shards/sqlengine/parser.h"

#include <utility>

#include "absl/status/status.h"
#include "shards/sqlengine/visitors/ast.h"

namespace shards {
namespace sqlengine {
namespace parser {

absl::StatusOr<std::unique_ptr<sqlast::AbstractStatement>> SQLParser::Parse(
    const std::string &sql) {
  this->error_ = false;
  // Initialize ANTLR things.
  this->input_stream_ = std::make_unique<antlr4::ANTLRInputStream>(sql);
  this->lexer_ =
      std::make_unique<sqlparser::SQLiteLexer>(this->input_stream_.get());
  this->lexer_->addErrorListener(this);
  this->tokens_ =
      std::make_unique<antlr4::CommonTokenStream>(this->lexer_.get());
  this->parser_ =
      std::make_unique<sqlparser::SQLiteParser>(this->tokens_.get());
  this->parser_->addErrorListener(this);

  // Make sure the parsed statement is ok!
  auto *statement = this->parser_->sql_stmt();
  if (this->error_) {  // Syntax errors!
    return absl::InvalidArgumentError("SQL SYNTAX ERROR");
  }

  // Makes sure that all constructs used in the statement are supported!
  visitors::BuildAstVisitor ast;
  return ast.TransformStatement(statement);
}

void SQLParser::syntaxError(antlr4::Recognizer *recognizer,
                            antlr4::Token *offendingSymbol, size_t line,
                            size_t charPositionInLine, const std::string &msg,
                            std::exception_ptr e) {
  this->error_ = true;
}

}  // namespace parser
}  // namespace sqlengine
}  // namespace shards
