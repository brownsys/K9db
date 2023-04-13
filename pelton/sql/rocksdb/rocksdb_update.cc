// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include <utility>

#include "glog/logging.h"
#include "pelton/sql/rocksdb/filter.h"

namespace pelton {
namespace sql {
namespace rocks {

/*
 * UPDATE STATEMENTS.
 */

ResultSetAndStatus RocksdbSession::ExecuteUpdate(const sqlast::Update &sql) {
  CHECK(this->write_txn_) << "Update called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  const std::string &table_name = sql.table_name();
  const sqlast::BinaryExpression *const where = sql.GetWhereClause();

  RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Turn SQL statement to a map of updates.
  dataflow::Record::UpdateMap updates;
  for (const auto &[col, v] : util::Zip(&sql.GetColumns(), &sql.GetValues())) {
    updates.emplace(col, v.get());
  }

  // Find the records to be updated.
  std::vector<DeleteRecord> records =
      this->GetRecords<DeleteRecord, false>(table_name, where);

  // Iterate over records to be updated and (1) mark them as deleted in the
  // returned result set (deduplicated), (2) update them in the DB, and (3) add
  // updated records to result set (deduplicated).
  size_t pk_col = schema.keys().at(0);
  int count = records.size();
  std::vector<dataflow::Record> vec;
  DedupSet<std::string> dedup_keys;
  for (DeleteRecord &element : records) {
    // Update record.
    dataflow::Record urecord = element.record.Update(updates);
    RocksdbSequence updated = RocksdbSequence::FromRecord(urecord);

    // Append record to result.
    if (!dedup_keys.Duplicate(element.value.At(pk_col).ToString())) {
      vec.push_back(std::move(element.record));
      vec.push_back(std::move(urecord));
    }

    // Update indices.
    table.IndexUpdate(element.shard, element.value, updated, txn);

    // Update table.
    EncryptedValue envalue = this->conn_->encryption_.EncryptValue(
        element.shard, std::move(updated));
    table.Put(element.key, envalue, txn);
  }

  return ResultSetAndStatus(SqlResultSet(schema, std::move(vec)), count);
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
