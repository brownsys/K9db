// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include <utility>

#include "glog/logging.h"
#include "pelton/sql/rocksdb/filter.h"

namespace pelton {
namespace sql {

// Move constructor for DeleteRecord.
RocksdbConnection::DeleteRecord::DeleteRecord(std::string &&s, EncryptedKey &&k,
                                              RocksdbSequence &&v,
                                              dataflow::Record &&r)
    : shard(std::move(s)),
      key(std::move(k)),
      value(std::move(v)),
      record(std::move(r)) {}

/*
 * DELETE STATEMENTS.
 */

SqlResultSet RocksdbConnection::ExecuteDelete(const sqlast::Delete &sql) {
  const std::string &table_name = sql.table_name();
  const sqlast::BinaryExpression *const where = sql.GetWhereClause();

  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Find the records to be deleted.
  std::vector<DeleteRecord> records =
      this->GetRecords<DeleteRecord, false>(table_name, where);

  // Iterate over records to be deleted and delete them.
  size_t pk_col = schema.keys().at(0);
  std::vector<dataflow::Record> result;
  DedupSet<std::string> dedup_keys;
  for (DeleteRecord &element : records) {
    // Update indices.
    table.IndexDelete(element.shard, element.value);

    // Delete record.
    table.Delete(element.key);

    // Append record to result.
    if (!dedup_keys.Duplicate(element.value.At(pk_col).ToString())) {
      result.push_back(std::move(element.record));
    }
  }

  return SqlResultSet(schema, std::move(result));
}

}  // namespace sql
}  // namespace pelton
