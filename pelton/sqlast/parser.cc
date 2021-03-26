// Parser responsible for parsing sql statement using ANTLR grammar.

#include "pelton/sqlast/parser.h"

#include <utility>

#include "absl/status/status.h"
#include "pelton/sqlast/transformer.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace sqlast {

absl::StatusOr<std::unique_ptr<AbstractStatement>> SQLParser::Parse(
    const std::string &sql) {
  this->error_ = false;

  // Initialize ANTLR things.
  perf::Start("ANTLR");
  this->input_stream_ = std::make_unique<antlr4::ANTLRInputStream>(sql);
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
  perf::End("ANTLR");

  // Makes sure that all constructs used in the statement are supported!
  perf::Start("Transformer");
  auto result = AstTransformer().TransformStatement(statement);
  perf::End("Transformer");
  return result;
}

void SQLParser::syntaxError(antlr4::Recognizer *recognizer,
                            antlr4::Token *offendingSymbol, size_t line,
                            size_t charPositionInLine, const std::string &msg,
                            std::exception_ptr e) {
  this->error_ = true;
}

}  // namespace sqlast
}  // namespace pelton
