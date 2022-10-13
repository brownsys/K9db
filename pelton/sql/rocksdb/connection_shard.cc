// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include <optional>

#include "glog/logging.h"

namespace pelton {
namespace sql {

using ORecord = std::optional<dataflow::Record>;

struct KeyData {
  RocksdbSequence key;
  bool del;
  std::unordered_set<util::ShardName> exclude;
  std::string shard;
};

// Shard-based operations for copying/moving/deleting records.
ResultSetAndStatus RocksdbConnection::AssignToShards(
    const std::string &table_name, size_t column_index,
    const std::vector<dataflow::Value> &values,
    const std::unordered_set<util::ShardName> &targets) {
  util::ShardName default_shard(DEFAULT_SHARD, DEFAULT_SHARD);
  // Look up table info.
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Find source then use the other API.
  std::vector<std::string> encoded_values = EncodeValues(values);
  std::optional<IndexSet> sources =
      table.IndexLookup(column_index, encoded_values);
  CHECK(sources.has_value()) << "Assign to shards with no index";
  if (sources->size() == 0) {
    return std::pair(sql::SqlResultSet(schema), 0);
  }

  // In principle, there may be 0 or more columns associated with each value.
  // These records may also be shared by multiple owners and thus have multiple
  // copies.
  // Sources include all the records and their copies. However, we only need
  // one copy of each record.
  std::unordered_map<std::string, std::list<KeyData>::iterator> dedup_pk;
  std::list<KeyData> keys;
  for (const RocksdbIndexRecord &source : sources.value()) {
    std::list<KeyData>::iterator key_it;
    // Have we seen this PK before?
    std::string pk = source.GetPK().ToString();
    auto it = dedup_pk.find(pk);
    bool first_time = (it == dedup_pk.end());
    if (first_time) {
      key_it = keys.emplace(keys.end());
      it = dedup_pk.emplace(pk, key_it).first;
    } else {
      // We have.
      key_it = it->second;
      if (key_it == keys.end()) {
        continue;  // Excluded row.
      }
    }

    // We want to use the default shard row (instead of other copies), and mark
    // it for deletion.
    bool del = (source.GetShard() == default_shard);
    if (first_time || del) {
      key_it->key.AppendEncoded(source.GetShard());
      key_it->key.AppendEncoded(source.GetPK());
      key_it->del = del;
    }

    // Exclude target from copying this record to since target already has the
    // record (or a copy of it).
    util::ShardName src(source.GetShard());
    if (targets.count(src) == 1) {
      key_it->exclude.insert(std::move(src));
      if (key_it->exclude.size() == targets.size()) {
        // We have excluded everything.
        keys.erase(key_it);
        it->second = keys.end();
      }
    }
  }

  // Encrypt remaining unexcluded keys.
  std::vector<EncryptedKey> enkeys;
  for (KeyData &data : keys) {
    data.shard = data.key.At(0).ToString();
    enkeys.push_back(this->encryption_manager_.EncryptKey(std::move(data.key)));
  }

  // Lookup keys.
  std::vector<std::optional<EncryptedValue>> rows = table.MultiGet(enkeys);

  // Copy/move the records as appropriate.
  int count = 0;
  std::vector<dataflow::Record> records;
  size_t i = 0;
  for (const KeyData &data : keys) {
    const EncryptedKey &enkey = enkeys.at(i);
    std::optional<EncryptedValue> &enrow = rows.at(i++);
    if (!enrow.has_value()) {
      continue;
    }

    // Need to decrypt row.
    RocksdbSequence row =
        this->encryption_manager_.DecryptValue(data.shard, std::move(*enrow));

    // Need to delete row.
    if (data.del) {
      table.IndexDelete(data.shard, row);
      table.Delete(enkey);
      count++;
    }

    // Need to insert into unexcluded targets.
    for (const util::ShardName &target : targets) {
      if (data.exclude.count(target) == 0) {
        // Create the new target key and encrypt it.
        RocksdbSequence target_key;
        target_key.Append(target);
        target_key.AppendEncoded(row.At(schema.keys().front()));
        EncryptedKey target_enkey =
            this->encryption_manager_.EncryptKey(std::move(target_key));

        // Encrypt the row.
        RocksdbSequence r(row);
        EncryptedValue target_envalue = this->encryption_manager_.EncryptValue(
            target.ByRef(), std::move(r));

        // Add to table.
        table.IndexAdd(target.AsSlice(), row);
        table.Put(target_enkey, target_envalue);
        count++;
      }
    }

    records.push_back(row.DecodeRecord(schema));
  }

  return std::pair(sql::SqlResultSet(schema, std::move(records)), count);
}

/*
std::pair<ORecord, int> RocksdbConnection::AssignToShards(
    const std::string &table_name, const util::ShardName &source,
    size_t column_index, const std::vector<dataflow::Value> &values,
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
    size_t column_index, const std::vector<dataflow::Value> &values) {
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
*/

}  // namespace sql
}  // namespace pelton
