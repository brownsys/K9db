// An abstract connection to some database.
#ifndef PELTON_SQL_ABSTRACT_CONNECTION_H_
#define PELTON_SQL_ABSTRACT_CONNECTION_H_

#include <cstdint>
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

using ResultSetAndStatus = std::pair<SqlResultSet, int>;
using KeyPair = std::pair<util::ShardName, sqlast::Value>;

class AbstractConnection {
 public:
  AbstractConnection() = default;
  virtual ~AbstractConnection() = default;

  // Close the connection.
  virtual void Open(const std::string &db_name) = 0;
  virtual void Close() = 0;

  // Statements.
  virtual bool ExecuteCreateTable(const sqlast::CreateTable &sql) = 0;

  virtual bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) = 0;

  // Insert.
  virtual int ExecuteInsert(const sqlast::Insert &sql,
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
      const std::vector<KeyPair> &keys) const = 0;

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

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ABSTRACT_CONNECTION_H_
