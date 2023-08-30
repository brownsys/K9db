// Connection to rocksdb.
#ifndef K9DB_SQL_ROCKSDB_ROCKSDB_CONNECTION_H_
#define K9DB_SQL_ROCKSDB_ROCKSDB_CONNECTION_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "k9db/dataflow/record.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "k9db/sql/rocksdb/encryption.h"
#include "k9db/sql/rocksdb/metadata.h"
#include "k9db/sql/rocksdb/table.h"
#include "k9db/sql/rocksdb/transaction.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/shard_name.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"

namespace k9db {
namespace sql {
namespace rocks {

// Forward declaration so it can be declared a friend.
class RocksdbSession;

// A connection is responsible for storing global DB state (e.g. schemas, keys),
// and creating new sessions.
class RocksdbConnection : public Connection {
 public:
  // Constructor + destructor.
  RocksdbConnection() = default;
  ~RocksdbConnection() { this->Close(); }

  // Open/close connection.
  std::vector<std::string> Open(const std::string &db_name,
                                const std::string &db_path) override;
  void Close() override;

  // Schema statements.
  bool ExecuteCreateTable(const sqlast::CreateTable &sql) override;
  bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) override;
  bool PersistCreateView(const sqlast::CreateView &sql) override;
  int64_t GetMaximumValue(const std::string &table_name,
                          const std::string &column_name) const override;

  // Opening a session.
  std::unique_ptr<Session> OpenSession() override;

  // Index information for explain.
  std::vector<std::string> GetIndices(const std::string &tbl) const override;
  std::string GetIndex(const std::string &tbl,
                       const sqlast::BinaryExpression *const) const override;

 private:
  std::unique_ptr<rocksdb::TransactionDB> db_;
  std::unordered_map<std::string, RocksdbTable> tables_;
  EncryptionManager encryption_;
  RocksdbMetadata metadata_;

  friend RocksdbSession;
};

// A session executes queries against the state in the connection, and provides
// atomicity guarantees.
class RocksdbSession : public Session {
 public:
  // Constructor + destructor.
  explicit RocksdbSession(RocksdbConnection *connection)
      : conn_(connection), write_txn_(false), txn_(nullptr) {}

  ~RocksdbSession() = default;

  // Commit or rollback.
  void BeginTransaction(bool write) override;
  void CommitTransaction() override;
  void RollbackTransaction() override;

  // Insert.
  bool Exists(const std::string &table_name,
              const sqlast::Value &pk) const override;

  bool Exists(const std::string &table_name, size_t column_index,
              const sqlast::Value &val) const override;

  int ExecuteInsert(const sqlast::Insert &sql,
                    const util::ShardName &shard_name) override;

  RecordAndStatus ExecuteReplace(const sqlast::Replace &sql,
                                 const util::ShardName &shard_name) override;

  // Delete.
  SqlDeleteSet ExecuteDelete(const sqlast::Delete &sql) override;

  // Select.
  SqlResultSet ExecuteSelect(const sqlast::Select &sql) const override;

  // Update.
  SqlUpdateSet ExecuteUpdate(const sqlast::Update &sql) override;

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

  std::vector<dataflow::Record> GetDirect(
      const std::string &table_name,
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

  std::unordered_set<util::ShardName> FindShards(
      const std::string &table_name, size_t column_index,
      const sqlast::Value &value) const override;

 private:
  // The parent connection.
  RocksdbConnection *conn_;
  bool write_txn_;
  std::unique_ptr<RocksdbInterface> txn_;

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
                            const sqlast::BinaryExpression *const where,
                            int limit = -1) const;
};

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#include "k9db/sql/rocksdb/rocksdb_helpers.cc"

#endif  // K9DB_SQL_ROCKSDB_ROCKSDB_CONNECTION_H_
