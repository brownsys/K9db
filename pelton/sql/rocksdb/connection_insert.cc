// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include "glog/logging.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace pelton {
namespace sql {

/*
 * INSERT STATEMENTS.
 */

int RocksdbConnection::ExecuteInsert(const sqlast::Insert &stmt,
                                     const util::ShardName &shard_name) {
  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Encode row.
  RocksdbRecord record = RocksdbRecord::FromInsert(stmt, schema, shard_name);

  // Ensure PK is unique.
  // CHECK(!table.Exists(record.GetPK())) << "Integrity error: PK exists";

  // Update indices.
  table.IndexAdd(shard_name.AsSlice(), record.Value());

  // Encrypt key and record value.
  EncryptedKey key =
      this->encryption_manager_.EncryptKey(std::move(record.Key()));
  EncryptedValue value = this->encryption_manager_.EncryptValue(
      shard_name.ByRef(), std::move(record.Value()));

  // Write to DB.
  table.Put(key, value);

  // Done.
  return 1;
}

}  // namespace sql
}  // namespace pelton
