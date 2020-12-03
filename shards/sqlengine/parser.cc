// Parser responsible for parsing sql statement using ANTLR grammar.

#include "shards/sqlengine/parser.h"

#include <iostream>

#include "shards/sqlengine/visitors/valid.h"

namespace shards {
namespace sqlengine {
namespace parser {

bool SQLParser::Parse(const std::string &sql) {
  // Initialize ANTLR things
  this->error_ = false;
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
  this->statement_ = this->parser_->sql_stmt();

  // Syntax errors!
  if (this->error_) {
    std::cout << "SQL SYNTAX ERROR!" << std::endl;
    return false;
  }

  // Make sure the constructs used in the statement are all supported!
  visitors::Valid valid;
  if (!statement_->accept(&valid).as<bool>()) {
    return false;
  }

  return true;
}

sqlparser::SQLiteParser::Sql_stmtContext *SQLParser::Statement() {
  return this->statement_;
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
