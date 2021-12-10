// Connection to rocksdb.
#ifndef PELTON_SQL_CONNECTIONS_ROCKSDB_H__
#define PELTON_SQL_CONNECTIONS_ROCKSDB_H__

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/connection.h"
#include "pelton/sqlast/ast.h"
#include "rocksdb/db.h"

namespace pelton {
namespace sql {

class SingletonRocksdbConnection {
 public:
  explicit SingletonRocksdbConnection(const std::string &path);

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

 private:
  // Helpers.
  std::vector<std::string> LookupAndFilter(
      const std::string table_name, const sqlast::AbstractStatement *sql);
  std::optional<std::string> ReplacedRow(const std::string table_name,
                                         const sqlast::AbstractStatement *stmt);

  // Members.
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>>
      handlers_;
  std::unordered_map<std::string, dataflow::SchemaRef> schemas_;
  std::unordered_map<std::string, size_t> primary_keys_;
  std::unordered_map<std::string, std::unordered_set<size_t>> indicies_;
};

class RocksdbConnection : public PeltonConnection {
 public:
  explicit RocksdbConnection(const std::string &path) {
    if (INSTANCE == nullptr) {
      INSTANCE = std::make_unique<SingletonRocksdbConnection>(path);
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

#endif  // PELTON_SQL_CONNECTIONS_ROCKSDB_H__
