#include "glog/logging.h"
#include "pelton/sql/rocksdb/connection.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

/*
 * SELECT STATEMENTS.
 */
/*
SqlResultSet RocksdbConnection::ExecuteSelect(
    const sqlast::Select &stmt, const dataflow::SchemaRef &out_schema,
    const std::vector<AugInfo> &augments) {
  CHECK_LE(augments.size(), 1u) << "Too many augmentations";

  // Read table metadata.
  // TO-DO: check for redundancy with Get
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.VisitSelect(stmt);

  // Result components.
  std::vector<dataflow::Record> records;
  std::vector<std::string> pkeys;

  // Figure out the projections (including order).
  bool has_projection = stmt.GetColumns().at(0) != "*";
  std::vector<int> projections;
  if (has_projection) {
    projections = ProjectionSchema(db_schema, out_schema, augments);
  }

  // Look up all the rows.
  auto result = this->GetRecords(tid, value_mapper, false).first;
  result = this->Filter(db_schema, &stmt, std::move(result));
  for (std::string &row : result) {
    rocksdb::Slice slice(row);
    rocksdb::Slice key = ExtractColumn(row, pk);
    pkeys.push_back(key.ToString());
    records.push_back(DecodeRecord(slice, out_schema, augments, projections));
  }
  return SqlResultSet(out_schema, std::move(records), std::move(pkeys));
}
*/

}  // namespace sql
}  // namespace pelton
