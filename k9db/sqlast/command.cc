// Represents an unparsed SQL command.
// This can be just a string (e.g. "SELECT * FROM table WHERE col = 'hi'"),
// or it can be a string with preparsed values, e.g.
// SELECT * FROM table WHERE col = <parsed value>.
// The later is the case for some prepared statements, which are defined using
// the form: SELECT * FROM table WHERE col = ?, with the concret value provided
// later.
// A pre-parsed value has no issues around SQL injection / escaping characters.

#include "k9db/sqlast/command.h"

namespace k9db {
namespace sqlast {

// Add stems.
void SQLCommand::AddStem(const std::string &stem) { this->query_.append(stem); }
void SQLCommand::AddChar(char c) { this->query_.push_back(c); }
void SQLCommand::AddArg(const std::string &arg) {
  // $<num> ---> pre parsed arg / bind parameter.
  this->query_.push_back('$');
  this->query_.push_back('_');
  this->query_.append(std::to_string(this->args_.size()));
  this->args_.push_back(arg);
}

}  // namespace sqlast
}  // namespace k9db
