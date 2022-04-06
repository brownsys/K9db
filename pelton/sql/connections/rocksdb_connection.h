// Connection to rocksdb.
#ifndef PELTON_SQL_CONNECTIONS_ROCKSDB_CONNECTION_H_
#define PELTON_SQL_CONNECTIONS_ROCKSDB_CONNECTION_H_

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/connection.h"
#include "pelton/sql/connections/rocksdb_index.h"
#include "pelton/sql/connections/rocksdb_util.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"
#include "rocksdb/db.h"

namespace pelton {
namespace sql {

using TableID = size_t;
using ShardID = std::string;

class SingletonRocksdbConnection {
 public:
  explicit SingletonRocksdbConnection(const std::string &db_name);

  // Close the connection.
  void Close();

  // Execute statement by type.
  bool ExecuteStatement(const sqlast::AbstractStatement *sql,
                        const std::string &shard_name);

  int ExecuteUpdate(const sqlast::AbstractStatement *sql,
                    const std::string &shard_name);

  SqlResultSet ExecuteQuery(const sqlast::AbstractStatement *sql,
                            const dataflow::SchemaRef &schema,
                            const std::vector<AugInfo> &augments,
                            const std::string &shard_name);

  SqlResultSet ExecuteQueryAll(const sqlast::AbstractStatement *sql,
                               const dataflow::SchemaRef &schema,
                               const std::vector<AugInfo> &augments);

 private:
  // Helpers.
  // Get record matching values in a value mapper (either by key, index, or it).
  std::vector<std::string> Get(const sqlast::AbstractStatement *stmt,
                               TableID table_id, const std::string &shard_name,
                               const ValueMapper &value_mapper);
  // Filter records by where clause in abstract statement.
  std::vector<std::string> Filter(const dataflow::SchemaRef &schema,
                                  const sqlast::AbstractStatement *sql,
                                  std::vector<std::string> &&rows);

  // Members.
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, TableID> tables_;
  std::unordered_map<TableID, std::unique_ptr<rocksdb::ColumnFamilyHandle>>
      handlers_;
  std::unordered_map<TableID, dataflow::SchemaRef> schemas_;
  std::unordered_map<TableID, size_t> primary_keys_;
  std::unordered_map<TableID, std::vector<size_t>> indexed_columns_;
  std::unordered_map<TableID, std::vector<RocksdbIndex>> indices_;
  std::unordered_map<TableID, std::atomic<int64_t>> auto_increment_counters_;
  unsigned char* global_key_;
};

class RocksdbConnection : public PeltonConnection {
 public:
  explicit RocksdbConnection(const std::string &db_name) {
    if (INSTANCE == nullptr) {
      INSTANCE = std::make_unique<SingletonRocksdbConnection>(db_name);
    }
    this->singleton_ = INSTANCE.get();
  }
  // Nothing to close / delete. Singleton survives.
  ~RocksdbConnection() { this->singleton_ = nullptr; }
  void Close() override { this->singleton_ = nullptr; }

  // Execute statement by type.
  bool ExecuteStatement(const sqlast::AbstractStatement *sql,
                        const std::string &shard_name) override {
    return this->singleton_->ExecuteStatement(sql, shard_name);
  }
  int ExecuteUpdate(const sqlast::AbstractStatement *sql,
                    const std::string &shard_name) override {
    return this->singleton_->ExecuteUpdate(sql, shard_name);
  }
  SqlResultSet ExecuteQuery(const sqlast::AbstractStatement *sql,
                            const dataflow::SchemaRef &schema,
                            const std::vector<AugInfo> &augments,
                            const std::string &shard_name) override {
    return this->singleton_->ExecuteQuery(sql, schema, augments, shard_name);
  }
  SqlResultSet ExecuteQueryAll(const sqlast::AbstractStatement *sql,
                               const dataflow::SchemaRef &schema,
                               const std::vector<AugInfo> &augments) override {
    return this->singleton_->ExecuteQueryAll(sql, schema, augments);
  }

  // Call to close the DB completely.
  static void CloseAll() {
    if (INSTANCE != nullptr) {
      INSTANCE->Close();
      INSTANCE = nullptr;
    }
  }

 private:
  static std::unique_ptr<SingletonRocksdbConnection> INSTANCE;
  SingletonRocksdbConnection *singleton_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTIONS_ROCKSDB_CONNECTION_H_
