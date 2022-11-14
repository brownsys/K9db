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

/*
 * REPLACE STATEMENTS.
 */

RecordAndStatus RocksdbConnection::ExecuteReplace(
    const sqlast::Replace &stmt, const util::ShardName &shard_name) {
  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Records to be returned for dataflow.
  int count = 0;
  std::optional<dataflow::Record> result;

  // PK of record to be replaced.
  size_t pkidx = table.PKColumn();
  const sqlast::Value &pkval = stmt.GetValue(schema.NameOf(pkidx), pkidx);
  std::vector<std::string> pkstr = EncodeValues(schema.TypeOf(pkidx), {pkval});

  // Do records with this PK already exist? Are any of them in the same target
  // shard?
  int same_shard_index = -1;
  std::vector<std::string> shards;
  std::vector<EncryptedKey> enkeys;
  IndexSet set = table.GetPKIndex().Get(std::move(pkstr));
  for (auto it = set.begin(); it != set.end();) {
    auto node = set.extract(it++);
    RocksdbIndexRecord &key = node.value();
    shards.push_back(key.GetShard().ToString());
    enkeys.push_back(this->encryption_manager_.EncryptKey(key.Release()));
    if (shards.back() == shard_name) {
      same_shard_index = shards.size() - 1;
    }
  }

  // Lookup any existing records.
  std::vector<std::optional<EncryptedValue>> envalues = table.MultiGet(enkeys);
  for (size_t i = 0; i < envalues.size(); i++) {
    std::optional<EncryptedValue> &opt = envalues.at(i);
    if (opt.has_value()) {
      // Decrypt record.
      std::string &shard = shards.at(i);
      RocksdbSequence value =
          this->encryption_manager_.DecryptValue(shard, std::move(*opt));
      if (!result.has_value()) {
        result = value.DecodeRecord(schema, false);
      }

      // If this existing record has the same shard as what we are inserting,
      // we do not need to delete it (or update the PK index).
      // We can just overwrite on top of it, but we will need to update other
      // indices if they exist.
      if (static_cast<int>(i) != same_shard_index) {
        count++;
        table.IndexDelete(shard, value);
        table.Delete(enkeys.at(i));
      } else {
        table.IndexDelete(shard, value, false);
      }
    }
  }

  // Encode row to be inserted.
  RocksdbRecord record = RocksdbRecord::FromInsert(stmt, schema, shard_name);

  // We do not need to update PK index if an existing record had the same shard.
  table.IndexAdd(shard_name.AsSlice(), record.Value(), same_shard_index == -1);

  // Encrypt record key and value.
  EncryptedKey key =
      same_shard_index > -1
          ? enkeys.at(same_shard_index)
          : this->encryption_manager_.EncryptKey(std::move(record.Key()));
  EncryptedValue value = this->encryption_manager_.EncryptValue(
      shard_name.ByRef(), std::move(record.Value()));

  // Write to DB.
  table.Put(key, value);
  count++;

  // Done.
  return RecordAndStatus(std::move(result), count);
}

}  // namespace sql
}  // namespace pelton
