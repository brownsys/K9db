// Connection to rocksdb.
#ifndef PELTON_SQL_ROCKSDB_CONNECTION_H_
#define PELTON_SQL_ROCKSDB_CONNECTION_H_

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sql/result.h"
#include "pelton/sql/rocksdb/index.h"
#include "pelton/sql/rocksdb/util.h"
#include "pelton/sqlast/ast.h"
#include "rocksdb/db.h"

namespace pelton {
namespace sql {

using TableID = size_t;
using ShardID = std::string;

class RocksdbConnection : public AbstractConnection {
 public:
  // Constructor + destructor.
  RocksdbConnection() = default;
  ~RocksdbConnection() { this->Close(); }

  // Open/close connection.
  void Open(const std::string &db_name) override;
  void Close() override;

  // Statements.
  bool ExecuteCreateTable(const sqlast::CreateTable &sql) override;
  bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) override;

  // Updates.
  InsertResult ExecuteInsert(const sqlast::Insert &sql,
                             const std::string &shard_name) override;
  InsertResult ExecuteReplace(const sqlast::Insert &sql,
                              const std::string &shard_name) override;

  SqlResult ExecuteUpdate(const sqlast::Update &sql) override;
  SqlResult ExecuteDelete(const sqlast::Delete &sql) override;

  // Selects.
  SqlResultSet ExecuteSelect(const sqlast::Select &sql,
                             const dataflow::SchemaRef &out_schema,
                             const std::vector<AugInfo> &augments) override;

 private:
  // Helpers.
  std::pair<std::vector<std::string>, std::vector<std::string>> GetRecords(
      TableID table_id, const ValueMapper &value_mapper, bool return_shards);

  // Filter records by where clause in abstract statement.
  std::vector<std::string> Filter(const dataflow::SchemaRef &schema,
                                  const sqlast::AbstractStatement *sql,
                                  std::vector<std::string> &&rows);

  std::pair<bool, std::vector<std::pair<std::string, std::string>>> KeyFinder(
      TableID table_id, const ValueMapper &value_mapper);

  // Gets encryption key corresponding to input user.
  // Creates key if does not exist.
  unsigned char *GetUserKey(const std::string &shard_name);

  // Members.
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, TableID> tables_;
  std::unordered_map<TableID, std::unique_ptr<rocksdb::ColumnFamilyHandle>>
      handlers_;
  std::unordered_map<TableID, dataflow::SchemaRef> schemas_;
  std::unordered_map<TableID, size_t> primary_keys_;
  std::unordered_map<TableID, std::vector<size_t>> indexed_columns_;
  std::unordered_map<TableID, std::vector<RocksdbIndex>> indices_;
  std::unordered_map<TableID, std::atomic<uint64_t>> auto_increment_counters_;

  // Encryption keys.
  unsigned char *global_key_;
  std::unordered_map<std::string, unsigned char *> user_keys_;
  mutable shards::UpgradableMutex user_keys_mtx_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_CONNECTION_H_
