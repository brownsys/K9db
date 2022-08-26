#include "glog/logging.h"
#include "pelton/sql/rocksdb/connection.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

/*
 * DELETE STATEMENTS.
 */
/*
SqlResult RocksdbConnection::ExecuteDelete(const sqlast::Delete &stmt) {
 // Read table metadata.
 const std::string &table_name = stmt.table_name();
 TableID tid = this->tables_.at(table_name);
 rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
 const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
 size_t pk = this->primary_keys_.at(tid);
 const auto &indexed_columns = this->indexed_columns_.at(tid);
 std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

 ValueMapper value_mapper(pk, indexed_columns, db_schema);
 value_mapper.VisitDelete(stmt);
 // Look up all affected rows.
 std::pair<std::vector<std::string>, std::vector<std::string>> result =
     this->GetRecords(tid, value_mapper, true);
 std::vector<std::string> shards = result.second;
 std::vector<std::string> rows = result.first;

 // Result components
 std::vector<dataflow::Record> records;
 std::vector<std::string> keys;

 rows = this->Filter(db_schema, &stmt, std::move(rows));
 auto rsize = rows.size();
 if (rsize > 5) {
   LOG(WARNING) << "Perf Warning: " << rsize << " rocksdb deletes in a loop "
                << stmt;
 }
 for (int i = 0; i < rsize; i++) {
   std::string &row = rows[i];
   std::string &shard_name = shards[i];

   rocksdb::Slice slice(row);
   // Get the key of the existing row.
   rocksdb::Slice keyslice = ExtractColumn(row, pk);
   if (stmt.returning()) {
     keys.push_back(keyslice.ToString());
     records.push_back(DecodeRecord(slice, db_schema, {}, {}));
   }
   // Delete the existing row by key.
   std::string skey =
       shard_name + __ROCKSSEP + keyslice.ToString() + __ROCKSSEP;
   PANIC(this->db_->Delete(rocksdb::WriteOptions(), handler,
                           EncryptKey(this->global_key_, skey)));

   // Update indices.
   for (size_t i = 0; i < indexed_columns.size(); i++) {
     RocksdbIndex &index = indices.at(i);
     size_t indexed_column = indexed_columns.at(i);
     index.Delete(ExtractColumn(slice, indexed_column), shard_name, keyslice);
   }
 }
 if (stmt.returning()) {
   return sql::SqlResult(
       sql::SqlResultSet(db_schema, std::move(records), std::move(keys)));
 } else {
   return SqlResult(rsize);
 }
 // TODO(babman): can do this better.
 // TODO(babman): need to handle returning deletes.
}
*/

}  // namespace sql
}  // namespace pelton
