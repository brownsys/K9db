// An abstract connection to some database.
#ifndef K9DB_SQL_CONNECTION_H_
#define K9DB_SQL_CONNECTION_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "k9db/dataflow/record.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/shard_name.h"

#define DEFAULT_SHARD "#default"

namespace k9db {
namespace sql {

using RecordAndStatus = std::pair<std::optional<dataflow::Record>, int>;
using ResultSetAndStatus = std::pair<SqlResultSet, int>;
using KeyPair = std::pair<util::ShardName, sqlast::Value>;

class Session {
 public:
  Session() = default;
  virtual ~Session() = default;

  // Commit and rollback.
  virtual void BeginTransaction(bool write) = 0;
  virtual void CommitTransaction() = 0;
  virtual void RollbackTransaction() = 0;

  // Insert related API.
  virtual bool Exists(const std::string &table_name,
                      const sqlast::Value &pk) const = 0;

  virtual bool Exists(const std::string &table_name, size_t column_index,
                      const sqlast::Value &val) const = 0;

  virtual int ExecuteInsert(const sqlast::Insert &sql,
                            const util::ShardName &shard_name) = 0;

  virtual RecordAndStatus ExecuteReplace(const sqlast::Replace &sql,
                                         const util::ShardName &shard_name) = 0;

  // Update.
  virtual SqlUpdateSet ExecuteUpdate(const sqlast::Update &sql) = 0;

  // Delete.
  virtual SqlDeleteSet ExecuteDelete(const sqlast::Delete &sql) = 0;

  // Selects.
  virtual SqlResultSet ExecuteSelect(const sqlast::Select &sql) const = 0;

  // Everything in a table.
  virtual SqlResultSet GetAll(const std::string &table_name) const = 0;

  // GDPR operations
  virtual SqlResultSet GetShard(const std::string &table_name,
                                util::ShardName &&shard_name) const = 0;
  virtual SqlResultSet DeleteShard(const std::string &table_name,
                                   util::ShardName &&shard_name) = 0;

  // Shard operations.
  virtual std::vector<dataflow::Record> GetDirect(
      const std::string &table_name, size_t column_index,
      const std::vector<KeyPair> &keys) const = 0;

  virtual std::vector<dataflow::Record> GetDirect(
      const std::string &table_name,
      const std::vector<KeyPair> &keys) const = 0;  // column_index = pk.

  virtual ResultSetAndStatus AssignToShards(
      const std::string &table_name, size_t column_index,
      const std::vector<sqlast::Value> &values,
      const std::unordered_set<util::ShardName> &targets) = 0;

  virtual int DeleteFromShard(const std::string &table_name,
                              const util::ShardName &shard_name,
                              const std::vector<dataflow::Record> &records,
                              const std::vector<bool> &move_to_default) = 0;

  virtual std::vector<size_t> CountShards(
      const std::string &table_name,
      const std::vector<sqlast::Value> &pk_values) const = 0;

  virtual std::unordered_set<util::ShardName> FindShards(
      const std::string &table_name, size_t column_index,
      const sqlast::Value &value) const = 0;
};

// Singular connection to the underyling database, which can open many sessions.
// Operations here have no concurrency control. Instead, they rely on global
// locking. These are schema operations.
class Connection {
 public:
  Connection() = default;
  virtual ~Connection() = default;

  // Opening a connection.
  virtual std::vector<std::string> Open(const std::string &db_name,
                                        const std::string &db_path) = 0;
  virtual void Close() = 0;

  // Schema statements.
  virtual bool ExecuteCreateTable(const sqlast::CreateTable &sql) = 0;
  virtual bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) = 0;
  virtual bool PersistCreateView(const sqlast::CreateView &sql) = 0;
  virtual int64_t GetMaximumValue(const std::string &table_name,
                                  const std::string &column_name) const = 0;

  // Opening a session.
  virtual std::unique_ptr<Session> OpenSession() = 0;

  // Index information for explain.
  virtual std::vector<std::string> GetIndices(const std::string &tbl) const = 0;
  virtual std::string GetIndex(const std::string &tbl,
                               const sqlast::BinaryExpression *const) const = 0;
};

}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_CONNECTION_H_
