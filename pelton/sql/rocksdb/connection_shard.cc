// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

namespace pelton {
namespace sql {

using ORecord = std::optional<dataflow::Record>;

// Shard-based operations for copying/moving/deleting records.
std::pair<ORecord, int> RocksdbConnection::AssignToShards(
    const std::string &table_name, const dataflow::Value &pk,
    const std::unordered_set<util::ShardName> &targets) {
  // Look up table info.
  RocksdbTable &table = this->tables_.at(table_name);

  // Find source then use the other API.
  std::string encoded_pk = EncodeValue(pk) + __ROCKSSEP;
  std::vector<rocksdb::Slice> lookup;
  lookup.emplace_back(encoded_pk);

  const RocksdbPKIndex &pk_index = table.GetPKIndex();
  IndexSet sources = pk_index.Get(lookup);
  if (sources.size() == 0) {
    return std::pair<ORecord, int>(ORecord(), 0);
  }

  // Choose any source.
  util::ShardName source(sources.begin()->GetShard().ToString());

  // Remove targets that already exist.
  std::unordered_set<util::ShardName> dedup = targets;
  for (const RocksdbIndexRecord &source : sources) {
    auto it = std::find(dedup.begin(), dedup.end(), source.GetShard());
    if (it != dedup.end()) {
      dedup.erase(it);
    }
  }

  return this->AssignToShards(table_name, source, pk, dedup);
}

std::pair<ORecord, int> RocksdbConnection::AssignToShards(
    const std::string &table_name, const util::ShardName &source,
    const dataflow::Value &pk,
    const std::unordered_set<util::ShardName> &targets) {
  const std::string &shard = source.String();

  // Look up table info.
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Construct and encrypt key.
  RocksdbSequence key;
  key.Append(source);
  key.Append(pk);
  EncryptedKey enkey = this->encryption_manager_.EncryptKey(std::move(key));

  // Get the record to copy/move.
  std::optional<EncryptedValue> envalue = table.Get(enkey);
  if (envalue.has_value()) {
    RocksdbSequence row =
        this->encryption_manager_.DecryptValue(shard, std::move(*envalue));

    // Now, we insert a copy of the record into each target.
    int count = 0;
    for (const util::ShardName &target : targets) {
      const std::string &tstr = target.String();
      // Update indices.
      table.IndexAdd(tstr, row);

      // Encrypt key and record value.
      RocksdbSequence target_key;
      target_key.Append(target);
      target_key.Append(pk);
      EncryptedKey target_enkey =
          this->encryption_manager_.EncryptKey(std::move(target_key));
      EncryptedValue target_envalue =
          this->encryption_manager_.EncryptValue(tstr, RocksdbSequence(row));

      // Write to DB.
      table.Put(target_enkey, target_envalue);
      count++;
    }

    // If record was in default shard, we need to delete it from there.
    if (source.ShardKind() == DEFAULT_SHARD &&
        source.UserID() == DEFAULT_SHARD) {
      // Delete.
      table.IndexDelete(shard, row);
      table.Delete(enkey);
      count++;
    }

    return std::make_pair(row.DecodeRecord(schema), count);
  }

  return std::pair<ORecord, int>(ORecord(), 0);
}

std::pair<ORecord, int> RocksdbConnection::DeleteFromShard(
    const std::string &table_name, const util::ShardName &source,
    const dataflow::Value &pk) {
  const std::string &shard = source.String();

  // Look up table info.
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Construct and encrypt key.
  RocksdbSequence key;
  key.Append(source);
  key.Append(pk);
  EncryptedKey enkey = this->encryption_manager_.EncryptKey(std::move(key));

  std::optional<EncryptedValue> envalue = table.Get(enkey);
  if (envalue.has_value()) {
    RocksdbSequence value =
        this->encryption_manager_.DecryptValue(shard, std::move(*envalue));

    // Delete.
    table.IndexDelete(shard, value);
    table.Delete(enkey);

    return std::make_pair(value.DecodeRecord(schema), 1);
  }

  return std::pair<ORecord, int>(ORecord(), 0);
}

}  // namespace sql
}  // namespace pelton
