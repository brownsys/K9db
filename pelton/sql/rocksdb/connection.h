// Connection to rocksdb.
#ifndef PELTON_SQL_ROCKSDB_CONNECTION_H_
#define PELTON_SQL_ROCKSDB_CONNECTION_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sql/result.h"
#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/sql/rocksdb/table.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/shard_name.h"
#include "rocksdb/db.h"

namespace pelton {
namespace sql {

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

  // Insert.
  int ExecuteInsert(const sqlast::Insert &sql,
                    const util::ShardName &shard_name) override;

  RecordAndStatus ExecuteReplace(const sqlast::Replace &sql,
                                 const util::ShardName &shard_name) override;

  // Delete.
  SqlDeleteSet ExecuteDelete(const sqlast::Delete &sql) override;

  // Select.
  SqlResultSet ExecuteSelect(const sqlast::Select &sql) const override;

  // Update.
  ResultSetAndStatus ExecuteUpdate(const sqlast::Update &sql) override;

  // Everything in a table.
  SqlResultSet GetAll(const std::string &table_name) const override;

  // Shard-based operations for GDPR GET/FORGET.
  SqlResultSet DeleteShard(const std::string &table_name,
                           util::ShardName &&shard_name) override;
  SqlResultSet GetShard(const std::string &table_name,
                        util::ShardName &&shard_name) const override;

  // Shard-based operations for copying/moving/deleting records.
  std::vector<dataflow::Record> GetDirect(
      const std::string &table_name, size_t column_index,
      const std::vector<KeyPair> &keys) const override;

  ResultSetAndStatus AssignToShards(
      const std::string &table_name, size_t column_index,
      const std::vector<sqlast::Value> &values,
      const std::unordered_set<util::ShardName> &targets) override;

  int DeleteFromShard(const std::string &table_name,
                      const util::ShardName &shard_name,
                      const std::vector<dataflow::Record> &records,
                      const std::vector<bool> &move_to_default) override;

  std::vector<size_t> CountShards(
      const std::string &table_name,
      const std::vector<sqlast::Value> &pk_values) const override;

  // Index information for explain.
  std::vector<std::string> GetIndices(const std::string &tbl) const override;
  std::string GetIndex(const std::string &tbl,
                       const sqlast::BinaryExpression *const) const override;

 private:
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, RocksdbTable> tables_;
  EncryptionManager encryption_manager_;

  // The possible types the helper function below can return.
  using SelectRecord = dataflow::Record;
  struct DeleteRecord {
    std::string shard;
    EncryptedKey key;
    RocksdbSequence value;
    dataflow::Record record;
    // Move constructor only.
    DeleteRecord(std::string &&s, EncryptedKey &&k, RocksdbSequence &&v,
                 dataflow::Record &&r);
  };

  // Get records matching where condition.
  template <typename T, bool DEDUP>
  std::vector<T> GetRecords(const std::string &table_name,
                            const sqlast::BinaryExpression *const where) const;

  // Combine shard kind and user id to get a qualified shard name.
  std::string CombineShardName(const std::string &shard_kind,
                               const std::string &user_id) const;
  std::pair<std::string, std::string> SplitShardName(
      const std::string &shard_name) const;
};

}  // namespace sql
}  // namespace pelton

#include "pelton/sql/rocksdb/connection_helpers.cc"

#endif  // PELTON_SQL_ROCKSDB_CONNECTION_H_