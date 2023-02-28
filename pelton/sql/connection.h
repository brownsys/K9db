// An abstract connection to some database.
#ifndef PELTON_SQL_CONNECTION_H_
#define PELTON_SQL_CONNECTION_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/shard_name.h"

#define DEFAULT_SHARD "#default"

namespace pelton {
namespace sql {

using RecordAndStatus = std::pair<std::optional<dataflow::Record>, int>;
using ResultSetAndStatus = std::pair<SqlResultSet, int>;
using KeyPair = std::pair<util::ShardName, sqlast::Value>;

// A session provides atomic writes/reads.
class Session {
 public:
  Session() = default;
  virtual ~Session() = default;

  // Commit and rollback.
  virtual void BeginTransaction() = 0;
  virtual void CommitTransaction() = 0;
  virtual void RollbackTransaction() = 0;

  // Insert related API.
  virtual bool Exists(const std::string &table_name,
                      const sqlast::Value &pk) const = 0;

  virtual int ExecuteInsert(const sqlast::Insert &sql,
                            const util::ShardName &shard_name) = 0;

  virtual RecordAndStatus ExecuteReplace(const sqlast::Replace &sql,
                                         const util::ShardName &shard_name) = 0;

  // Update.
  virtual ResultSetAndStatus ExecuteUpdate(const sqlast::Update &sql) = 0;

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
      const std::vector<KeyPair> &keys, bool read) const = 0;

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
};

// Singular connection to the underyling database, which can open many sessions.
class Connection {
 public:
  Connection() = default;
  virtual ~Connection() = default;

  // Opening a connection.
  virtual void Open(const std::string &db_name) = 0;
  virtual void Close() = 0;

  // Schema statements.
  virtual bool ExecuteCreateTable(const sqlast::CreateTable &sql) = 0;
  virtual bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) = 0;

  // Opening a session.
  virtual std::unique_ptr<Session> OpenSession() = 0;

  // Index information for explain.
  virtual std::vector<std::string> GetIndices(const std::string &tbl) const = 0;
  virtual std::string GetIndex(const std::string &tbl,
                               const sqlast::BinaryExpression *const) const = 0;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTION_H_
