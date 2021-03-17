// A logical executable statement.
// Usually, this is a single sql statement that may need to be executed against
// one or more shards.
// The executable statement contains the actual SQL statement string,
// a description of which shard(s) to run the statement against, as well as
// what to do with the output of the statement.

#ifndef PELTON_SHARDS_SQLEXECUTOR_EXECUTABLE_H_
#define PELTON_SHARDS_SQLEXECUTOR_EXECUTABLE_H_

#include <sqlite3.h>

#include <memory>
#include <string>
#include <unordered_set>

#include "pelton/shards/state.h"
#include "pelton/shards/types.h"

namespace pelton {
namespace shards {
namespace sqlexecutor {

// Abstract interface for a logical SQL executable statement.
class ExecutableStatement {
 public:
  virtual ~ExecutableStatement() {}

  virtual bool Execute(SharderState *state, const Callback &callback,
                       void *context, char **errmsg) = 0;

  inline static const Callback *CALLBACK_NO_CAPTURE = nullptr;
};

// An executable simple SQL logical statement.
// Simple here means that the logical statement is singular (does not consist of
// several chained/composed statements).
class SimpleExecutableStatement : public ExecutableStatement {
 public:
  // Construct a simple statement executed against the default shard.
  static std::unique_ptr<SimpleExecutableStatement> DefaultShard(
      const std::string &stmt);

  // Construct a simple statement executed against a particular user shard.
  static std::unique_ptr<SimpleExecutableStatement> UserShard(
      const std::string &stmt, const std::string &shard_kind,
      const std::string &user_id);

  // Construct a simple statement executed against all user shards of a given
  // kind.
  static std::unique_ptr<SimpleExecutableStatement> AllShards(
      const std::string &stmt, const std::string &shard_kind);

  // Override interface.
  bool Execute(SharderState *state, const Callback &callback, void *context,
               char **errmsg) override;

 private:
  // Private constructors.
  SimpleExecutableStatement() = default;

  // Execute statement against given sqlite3 connection (after resolving shard).
  bool Execute(::sqlite3 *connection, const Callback &callback, void *context,
               char **errmsg);

  // The sql statement to execute.
  std::string sql_statement_;
  // True if this statement is meant to be executed against the default shard.
  // If true, all below attributes are ignored.
  bool default_shard_;
  // The shard king (e.g. user, patient, doctor, etc) this is meant to be
  // executed against.
  std::string shard_kind_;
  // True if this statement is meant to be executed against all user shards of a
  // given kind, has the same effect as user_ids_ = {all user ids}.
  // If true, the contents of user_ids_ are ignored.
  bool all_users_;
  // The users this executable statement is meant to be executed against.
  // E.g. if shard_kind_ is doctor, and user_ids_ is {1, 2}
  // then this statement is executed against shards doctor_1 and doctor_2.
  std::unordered_set<std::string> user_ids_;
};

}  // namespace sqlexecutor
}  // namespace shards
}  // namespace pelton

#endif  // PELTON_SHARDS_SQLEXECUTOR_EXECUTABLE_H_
