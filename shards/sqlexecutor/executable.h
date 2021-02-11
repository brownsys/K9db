// Manages sqlite3 connections to the different shard/mini-databases.

#ifndef SHARDS_SQLEXECUTOR_EXECUTABLE_H_
#define SHARDS_SQLEXECUTOR_EXECUTABLE_H_

#include <sqlite3.h>

#include <cstring>
#include <functional>
#include <string>

namespace shards {
namespace sqlexecutor {

using Callback = std::function<int(void *, int, char **, char **)>;

// Abstract interface for a logical SQL executable statement.
class ExecutableStatement {
 public:
  ExecutableStatement(const std::string &shard_suffix,
                      const std::string &sql_statement)
      : shard_suffix_(shard_suffix), sql_statement_(sql_statement) {}

  virtual ~ExecutableStatement() {}

  virtual const std::string &shard_suffix() const;
  virtual const std::string &sql_statement() const;
  virtual bool Execute(::sqlite3 *connection, Callback callback, void *context,
                       char **errmsg) = 0;

 protected:
  std::string shard_suffix_;
  std::string sql_statement_;
};

// An executable simple SQL logical statement.
// Simple here means that:
// 1. The logical statement is singular (does not consist of several statements)
// 2. The statement is over a single shard (or the unsharded main DB).
// 3. The statement has no (data) output: it is either a Create, Insert, Update,
//    or Delete.
class SimpleExecutableStatement : public ExecutableStatement {
 public:
  SimpleExecutableStatement(const std::string &shard_suffix,
                            const std::string &sql_statement)
      : ExecutableStatement(shard_suffix, sql_statement) {}

  // Override interface.
  bool Execute(::sqlite3 *connection, Callback callback, void *context,
               char **errmsg) override;
};

// An executable select SQL statement.
class SelectExecutableStatement : public ExecutableStatement {
 public:
  SelectExecutableStatement(const std::string &shard_suffix,
                            const std::string &sql_statement,
                            size_t appended_column_index,
                            const std::string &appended_column_name,
                            const std::string &appended_column_value)
      : ExecutableStatement(shard_suffix, sql_statement),
        coli_(appended_column_index) {
    this->coln_ = new char[appended_column_name.size() + 1];
    this->colv_ = new char[appended_column_value.size() + 1];
    // NOLINTNEXTLINE
    strcpy(this->coln_, appended_column_name.c_str());
    // NOLINTNEXTLINE
    strcpy(this->colv_, appended_column_value.c_str());
  }

  ~SelectExecutableStatement() {
    delete[] this->coln_;
    delete[] this->colv_;
  }

  // Override interface.
  bool Execute(::sqlite3 *connection, Callback callback, void *context,
               char **errmsg) override;

 private:
  size_t coli_;
  char *coln_;
  char *colv_;

  inline static Callback *CALLBACK_NO_CAPTURE = nullptr;
  inline static SelectExecutableStatement *THIS_NO_CAPTURE = nullptr;
};

}  // namespace sqlexecutor
}  // namespace shards

#endif  // SHARDS_SQLEXECUTOR_EXECUTABLE_H_
