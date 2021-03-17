#include "pelton/shards/sqlexecutor/executable.h"

#include "glog/logging.h"

namespace pelton {
namespace shards {
namespace sqlexecutor {

// Construct a simple statement executed against the default shard.
std::unique_ptr<SimpleExecutableStatement>
SimpleExecutableStatement::DefaultShard(const std::string &stmt) {
  std::unique_ptr<SimpleExecutableStatement> statement(
      new SimpleExecutableStatement());
  statement->sql_statement_ = stmt;
  statement->default_shard_ = true;
  return statement;
}

// Construct a simple statement executed against a particular user shard.
std::unique_ptr<SimpleExecutableStatement> SimpleExecutableStatement::UserShard(
    const std::string &stmt, const std::string &shard_kind,
    const std::string &user_id) {
  std::unique_ptr<SimpleExecutableStatement> statement{
      new SimpleExecutableStatement()};
  statement->sql_statement_ = stmt;
  statement->default_shard_ = false;
  statement->shard_kind_ = shard_kind;
  statement->all_users_ = false;
  statement->user_ids_.insert(user_id);
  return statement;
}

// Construct a simple statement executed against all user shards of a given
// kind.
std::unique_ptr<SimpleExecutableStatement> SimpleExecutableStatement::AllShards(
    const std::string &stmt, const std::string &shard_kind) {
  std::unique_ptr<SimpleExecutableStatement> statement{
      new SimpleExecutableStatement()};
  statement->sql_statement_ = stmt;
  statement->default_shard_ = false;
  statement->shard_kind_ = shard_kind;
  statement->all_users_ = true;
  return statement;
}

// Override interface.
bool SimpleExecutableStatement::Execute(SharderState *state,
                                        const Callback &callback, void *context,
                                        char **errmsg) {
  // Execute against default shard.
  if (this->default_shard_) {
    ::sqlite3 *conn = state->connection_pool().GetDefaultConnection();
    return this->Execute(conn, callback, context, errmsg);
  }

  // Execute against particular user shards.
  if (!this->all_users_) {
    for (const std::string &user_id : this->user_ids_) {
      ::sqlite3 *conn =
          state->connection_pool().GetConnection(this->shard_kind_, user_id);
      if (!this->Execute(conn, callback, context, errmsg)) {
        return false;
      }
    }
    return true;
  }

  // Execute against all user shards.
  if (this->all_users_) {
    const auto &user_ids = state->UsersOfShard(this->shard_kind_);
    for (const std::string &user_id : user_ids) {
      ::sqlite3 *conn =
          state->connection_pool().GetConnection(this->shard_kind_, user_id);
      if (!this->Execute(conn, callback, context, errmsg)) {
        return false;
      }
    }
    return true;
  }

  LOG(FATAL) << "Illegal configuration for SimpleExecutableStatement";
}

bool SimpleExecutableStatement::Execute(::sqlite3 *connection,
                                        const Callback &callback, void *context,
                                        char **errmsg) {
  // Turn c++ style callback into a c-style function pointer.
  CALLBACK_NO_CAPTURE = &callback;
  auto cb = [](void *context, int colnum, char **colvals, char **colnames) {
    return (*CALLBACK_NO_CAPTURE)(context, colnum, colvals, colnames);
  };

  // Execute using c-style function pointer.
  const char *sql = this->sql_statement_.c_str();
  return ::sqlite3_exec(connection, sql, cb, context, errmsg) == SQLITE_OK;
}

}  // namespace sqlexecutor
}  // namespace shards
}  // namespace pelton
