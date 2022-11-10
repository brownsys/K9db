// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include <utility>

#include "glog/logging.h"
#include "pelton/sql/rocksdb/filter.h"

namespace pelton {
namespace sql {

/*
 * UPDATE STATEMENTS.
 */

ResultSetAndStatus RocksdbConnection::ExecuteUpdate(const sqlast::Update &sql) {
  const std::string &table_name = sql.table_name();
  const sqlast::BinaryExpression *const where = sql.GetWhereClause();

  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Turn SQL statement to a map of updates.
  std::unordered_map<std::string, sqlast::Value> updates;
  for (const auto &[col, v] : util::Zip(&sql.GetColumns(), &sql.GetValues())) {
    updates.emplace(col, v);
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
    RocksdbSequence updated = element.value.Update(schema, updates);

    // Append record to result.
    if (!dedup_keys.Duplicate(element.value.At(pk_col).ToString())) {
      vec.push_back(std::move(element.record));
      vec.push_back(updated.DecodeRecord(schema, true));
    }

    // Update indices.
    table.IndexUpdate(element.shard, element.value, updated);

    // Update table.
    EncryptedValue envalue = this->encryption_manager_.EncryptValue(
        element.shard, std::move(updated));
    table.Put(element.key, envalue);
  }

  return ResultSetAndStatus(SqlResultSet(schema, std::move(vec)), count);
}

}  // namespace sql
}  // namespace pelton
