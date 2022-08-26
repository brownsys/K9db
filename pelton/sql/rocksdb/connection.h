// Connection to rocksdb.
#ifndef PELTON_SQL_ROCKSDB_CONNECTION_H_
#define PELTON_SQL_ROCKSDB_CONNECTION_H_

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/abstract_connection.h"
#include "pelton/sql/result.h"
#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/sql/rocksdb/index.h"
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
  bool ExecuteCreateTable(const sqlast::CreateTable &sql) override;
  bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) override;

  // Updates.
  InsertResult ExecuteInsert(const sqlast::Insert &sql,
                             const std::string &shard_name) override;
  InsertResult ExecuteReplace(const sqlast::Insert &sql,
                              const std::string &shard_name) override;

  /*
  SqlResult ExecuteUpdate(const sqlast::Update &sql) override;
  SqlResult ExecuteDelete(const sqlast::Delete &sql) override;

  // Selects.
  SqlResultSet ExecuteSelect(const sqlast::Select &sql,
                             const dataflow::SchemaRef &out_schema,
                             const std::vector<AugInfo> &augments) override;
  */

 private:
  /*
  // Helpers.
  std::pair<std::vector<std::string>, std::vector<std::string>> GetRecords(
      TableID table_id, const ValueMapper &value_mapper, bool return_shards);

  // Filter records by where clause in abstract statement.
  std::vector<std::string> Filter(const dataflow::SchemaRef &schema,
                                  const sqlast::AbstractStatement *sql,
                                  std::vector<std::string> &&rows);

  std::pair<bool, std::vector<std::pair<std::string, std::string>>> KeyFinder(
      TableID table_id, const ValueMapper &value_mapper);
  */

  // rocksdb DB instance.
  std::unique_ptr<rocksdb::DB> db_;

  // Table maps.
  std::unordered_map<std::string, dataflow::SchemaRef> schemas_;
  std::unordered_map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>>
      handlers_;
  std::unordered_map<std::string, std::vector<std::optional<RocksdbIndex>>>
      indices_;

  // Encryption manager.
  EncryptionManager encryption_manager_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_CONNECTION_H_
