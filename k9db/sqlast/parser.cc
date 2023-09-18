// Parser responsible for parsing sql statement using ANTLR grammar.

#include "k9db/sqlast/parser.h"

#include <utility>

#include "absl/status/status.h"
#include "k9db/sqlast/hacky.h"
#include "k9db/sqlast/transformer.h"

namespace k9db {
namespace sqlast {

absl::StatusOr<std::unique_ptr<AbstractStatement>> SQLParser::Parse(
    const SQLCommand &sql) {
  auto hacky_result = HackyParse(sql);
  if (hacky_result.ok()) {
    return std::move(hacky_result.value());
  }

  this->error_ = false;

  // Initialize ANTLR things.
  this->input_stream_ = std::make_unique<antlr4::ANTLRInputStream>(sql.query());
  this->lexer_ =
      std::make_unique<sqlparser::SQLiteLexer>(this->input_stream_.get());
  this->lexer_->addErrorListener(this);
  this->tokens_ =
      std::make_unique<antlr4::CommonTokenStream>(this->lexer_.get());
  this->parser_ =
      std::make_unique<sqlparser::SQLiteParser>(this->tokens_.get());
  this->parser_->getInterpreter<antlr4::atn::ParserATNSimulator>()
      ->setPredictionMode(antlr4::atn::PredictionMode::SLL);
  this->parser_->addErrorListener(this);

  // Make sure the parsed statement is ok!
  auto *statement = this->parser_->sql_stmt();
  if (this->error_) {  // Syntax errors!
    return absl::InvalidArgumentError("SQL SYNTAX ERROR");
  }

  // Makes sure that all constructs used in the statement are supported!
  auto result = AstTransformer(sql).TransformStatement(statement);
  return result;
}

void SQLParser::syntaxError(antlr4::Recognizer *recognizer,
                            antlr4::Token *offendingSymbol, size_t line,
                            size_t charPositionInLine, const std::string &msg,
                            std::exception_ptr e) {
  this->error_ = true;
}

}  // namespace sqlast
}  // namespace k9db
