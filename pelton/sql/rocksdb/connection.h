// Connection to rocksdb.
#ifndef PELTON_SQL_ROCKSDB_CONNECTION_H_
#define PELTON_SQL_ROCKSDB_CONNECTION_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sql/result.h"
#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/sql/rocksdb/table.h"
#include "pelton/sqlast/ast.h"
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
  bool ExecuteCreateTable(const sqlast::CreateTable &sql,
                          size_t copies) override;
  bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) override;

  // Insert.
  int ExecuteInsert(const sqlast::Insert &sql, const std::string &shard_name,
                    size_t copy) override;

  // Delete.
  SqlResultSet ExecuteDelete(const sqlast::Delete &sql) override;

  // Select.
  SqlResultSet ExecuteSelect(const sqlast::Select &sql) const override;
  SqlResultSet GetShard(const std::string &table_name,
                        const std::string &shard_name) const override;

  // Everything in a table.
  SqlResultSet GetAll(const std::string &table_name) const override;
  SqlResultSet GetAll(const std::string &table_name,
                      size_t copy) const override;

 private:
  std::unique_ptr<rocksdb::DB> db_;
  std::unordered_map<std::string, std::vector<RocksdbTable>> tables_;
  EncryptionManager encryption_manager_;

  // Get records matching where condition.
  std::vector<dataflow::Record> GetRecords(
      const std::string &table_name,
      const sqlast::BinaryExpression &where) const;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_CONNECTION_H_
