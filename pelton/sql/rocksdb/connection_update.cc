#include "glog/logging.h"
#include "pelton/sql/rocksdb/connection.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

/*
 * UPDATE STATEMENTS.
 */
/*
// Execute Update statement
SqlResult RocksdbConnection::ExecuteUpdate(const sqlast::Update &stmt) {
  // ShardID sid = shard_name + __ROCKSSEP;

  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.VisitUpdate(stmt);

  // Turn update effects into an easy to process form.
  std::unordered_map<std::string, std::string> update;
  auto &columns = stmt.GetColumns();
  auto &values = stmt.GetValues();
  for (size_t i = 0; i < columns.size(); i++) {
    update.emplace(columns.at(i), values.at(i));
  }

  // Look up all affected rows.
  std::pair<std::vector<std::string>, std::vector<std::string>> result =
      this->GetRecords(tid, value_mapper, true);
  std::vector<std::string> shards = result.second;
  std::vector<std::string> rows = result.first;

  // Result components
  std::vector<dataflow::Record> records;
  std::vector<std::string> okeys;
  rows = this->Filter(db_schema, &stmt, std::move(rows));
  if (rows.size() > 5) {
    LOG(WARNING) << "Perf Warning: " << rows.size()
                 << " rocksdb updates in a loop " << stmt;
  }
  for (int i = 0; i < rows.size(); i++) {
    std::string &row = rows[i];
    std::string &shard_name = shards[i];
    // Compute updated row.
    std::string nrow = Update(update, db_schema, row);
    if (nrow == row) {  // No-op.
      continue;
    }
    rocksdb::Slice oslice(row);
    rocksdb::Slice nslice(nrow);
    rocksdb::Slice opk = ExtractColumn(oslice, pk);
    if (stmt.returning()) {
      okeys.push_back(opk.ToString());
      records.push_back(DecodeRecord(oslice, db_schema, {}, {}));
    }

    rocksdb::Slice npk = ExtractColumn(nslice, pk);
    // std::shard_name = ExtractColumn(shard, 0);
    ShardID sid = shard_name + __ROCKSSEP;
    std::string okey = sid + opk.ToString() + __ROCKSSEP;
    std::string nkey = sid + npk.ToString() + __ROCKSSEP;
    bool keychanged = !SlicesEq(opk, npk);
    // If the PK is unchanged, we do not need to delete, and can replace.
    if (keychanged) {
      PANIC(db_->Delete(rocksdb::WriteOptions(), handler,
                        EncryptKey(this->global_key_, okey)));
    }
    PANIC(db_->Put(rocksdb::WriteOptions(), handler,
                   EncryptKey(this->global_key_, nkey),
                   Encrypt(this->GetUserKey(shard_name), nrow)));
    // Update indices.
    for (size_t i = 0; i < indexed_columns.size(); i++) {
      RocksdbIndex &index = indices.at(i);
      size_t indexed_column = indexed_columns.at(i);
      rocksdb::Slice oval = ExtractColumn(oslice, indexed_column);
      rocksdb::Slice nval = ExtractColumn(nslice, indexed_column);
      if (!keychanged && SlicesEq(oval, nval)) {
        continue;
      }
      index.Delete(oval, shard_name, opk);
      index.Add(nval, shard_name, npk);
    }
  }
  // TODO(babman): need to handle returning updates.
  if (stmt.returning()) {
    return sql::SqlResult(
        sql::SqlResultSet(db_schema, std::move(records), std::move(okeys)));
  } else {
    return SqlResult(rows.size());
  }
}
*/

}  // namespace sql
}  // namespace pelton
