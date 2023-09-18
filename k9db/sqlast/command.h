// Represents an unparsed SQL command.
// This can be just a string (e.g. "SELECT * FROM table WHERE col = 'hi'"),
// or it can be a string with preparsed values, e.g.
// SELECT * FROM table WHERE col = <parsed value>.
// The later is the case for some prepared statements, which are defined using
// the form: SELECT * FROM table WHERE col = ?, with the concret value provided
// later.
// A pre-parsed value has no issues around SQL injection / escaping characters.

#ifndef K9DB_SQLAST_COMMAND_H_
#define K9DB_SQLAST_COMMAND_H_

#include <string>
#include <utility>
#include <vector>

namespace k9db {
namespace sqlast {

class SQLCommand {
 public:
  // Constructors.
  SQLCommand() : query_(""), args_() {}
  explicit SQLCommand(const std::string &query) : query_(query), args_() {}
  explicit SQLCommand(std::string &&query)
      : query_(std::move(query)), args_() {}

  // Add stems.
  void AddStem(const std::string &stem);
  void AddChar(char c);
  void AddArg(const std::string &arg);

  // Accessors.
  const std::string &query() const { return this->query_; }
  const std::string &arg(size_t i) const { return this->args_.at(i); }
  const std::vector<std::string> &args() const { return this->args_; }

 private:
  std::string query_;
  std::vector<std::string> args_;
};

}  // namespace sqlast
}  // namespace k9db

#endif  // K9DB_SQLAST_COMMAND_H_
