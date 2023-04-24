// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include <utility>

#include "glog/logging.h"
#include "k9db/sql/rocksdb/filter.h"

namespace k9db {
namespace sql {
namespace rocks {

// Move constructor for DeleteRecord.
RocksdbSession::DeleteRecord::DeleteRecord(std::string &&s, EncryptedKey &&k,
                                           RocksdbSequence &&v,
                                           dataflow::Record &&r)
    : shard(std::move(s)),
      key(std::move(k)),
      value(std::move(v)),
      record(std::move(r)) {}

/*
 * DELETE STATEMENTS.
 */

SqlDeleteSet RocksdbSession::ExecuteDelete(const sqlast::Delete &sql) {
  CHECK(this->write_txn_) << "Delete called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  const std::string &table_name = sql.table_name();
  const sqlast::BinaryExpression *const where = sql.GetWhereClause();

  RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Find the records to be deleted.
  std::vector<DeleteRecord> records =
      this->GetRecords<DeleteRecord, false>(table_name, where);

  // Iterate over records to be deleted and delete them.
  size_t pk_col = schema.keys().at(0);
  SqlDeleteSet result;
  DedupMap<std::string> dedup_keys;
  for (DeleteRecord &element : records) {
    // Update indices.
    table.IndexDelete(element.shard, element.value, txn);

    // Delete record.
    table.Delete(element.key, txn);

    // Append record to result.
    if (!dedup_keys.Exists(element.value.At(pk_col).ToString())) {
      dedup_keys.Assign(result.AddRecord(std::move(element.record)));
    }
    result.AssignToShard(dedup_keys.Value(), std::move(element.shard));
  }

  return result;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
