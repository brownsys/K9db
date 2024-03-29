// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include <optional>

#include "glog/logging.h"
#include "k9db/sql/rocksdb/dedup.h"

namespace k9db {
namespace sql {
namespace rocks {

// Get records by shard + PK value.
std::vector<dataflow::Record> RocksdbSession::GetDirect(
    const std::string &table_name, const std::vector<KeyPair> &keys) const {
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();
  return this->GetDirect(table_name, schema.keys().front(), keys);
}

// Get records by shard + column value (PK or not).
std::vector<dataflow::Record> RocksdbSession::GetDirect(
    const std::string &table_name, size_t column_index,
    const std::vector<KeyPair> &keys) const {
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Encrypt keys for lookup.
  std::vector<std::string> decr;
  std::vector<EncryptedKey> en_keys;
  bool by_pk = (column_index == table.PKColumn());
  if (by_pk) {
    // Lookup by PK.
    for (const auto &[shard, value] : keys) {
      RocksdbSequence key;
      key.Append(shard);
      key.Append(value);
      en_keys.push_back(this->conn_->encryption_.EncryptKey(std::move(key)));
    }
  } else {
    RocksdbPlan plan = table.ChooseIndex(column_index);
    CHECK(plan.type() != RocksdbPlan::IndexChoiceType::PK) << "UNREACHABLE";
    CHECK(plan.type() != RocksdbPlan::IndexChoiceType::SCAN)
        << "Get Direct on non-indexed column";

    // Encode shards and column values for index lookup.
    std::vector<std::string> shard_names;
    std::vector<sqlast::Value> values;
    for (const auto &[shard_name, value] : keys) {
      shard_names.emplace_back(shard_name.AsView());
      values.push_back(value);
    }
    std::vector<std::string> encoded =
        EncodeValues(schema.TypeOf(column_index), values);

    // Find index and lookup.
    const RocksdbIndex &index = table.GetTableIndex(plan.idx());
    IndexSet s = index.GetWithShards(std::move(shard_names), std::move(encoded),
                                     this->txn_.get());
    for (auto it = s.begin(); it != s.end();) {
      auto node = s.extract(it++);
      RocksdbIndexRecord &record = node.value();
      decr.push_back(record.GetShard().ToString());
      en_keys.push_back(this->conn_->encryption_.EncryptKey(record.Release()));
    }
  }

  // Lookup using multiget.
  std::vector<std::optional<EncryptedValue>> en_rows =
      table.MultiGet(en_keys, this->txn_.get());

  // Decrypt result.
  std::vector<dataflow::Record> result;
  DedupSet<std::string> dedup;
  result.reserve(en_rows.size());
  for (size_t i = 0; i < en_rows.size(); i++) {
    std::optional<EncryptedValue> &en_row = en_rows.at(i);
    if (en_row.has_value()) {
      const std::string &shard = by_pk ? keys.at(i).first.ByRef() : decr.at(i);
      RocksdbSequence row =
          this->conn_->encryption_.DecryptValue(shard, std::move(*en_row));
      if (!dedup.Duplicate(row.At(table.PKColumn()).ToString())) {
        result.push_back(row.DecodeRecord(schema, true));
      }
    }
  }

  return result;
}

struct KeyData {
  RocksdbSequence key;
  bool del;
  std::unordered_set<util::ShardName> exclude;
  std::string shard;
};

// Shard-based operations for copying/moving/deleting records.
ResultSetAndStatus RocksdbSession::AssignToShards(
    const std::string &table_name, size_t column_index,
    const std::vector<sqlast::Value> &values,
    const std::unordered_set<util::ShardName> &targets) {
  CHECK(this->write_txn_) << "AssignToShards called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  util::ShardName default_shard(DEFAULT_SHARD, DEFAULT_SHARD);
  // Look up table info.
  RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Find source then use the other API.
  sqlast::ValueMapper value_mapper(schema);
  value_mapper.AddValues(column_index, std::vector<sqlast::Value>(values));
  std::optional<IndexSet> sources = table.IndexLookup(&value_mapper, txn);
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
    enkeys.push_back(this->conn_->encryption_.EncryptKey(std::move(data.key)));
  }

  // Lookup keys.
  std::vector<std::optional<EncryptedValue>> rows = table.MultiGet(enkeys, txn);

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
        this->conn_->encryption_.DecryptValue(data.shard, std::move(*enrow));

    // Need to delete row.
    if (data.del) {
      table.IndexDelete(data.shard, row, txn);
      table.Delete(enkey, txn);
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
            this->conn_->encryption_.EncryptKey(std::move(target_key));

        // Encrypt the row.
        RocksdbSequence r(row);
        EncryptedValue target_envalue =
            this->conn_->encryption_.EncryptValue(target.ByRef(), std::move(r));

        // Add to table.
        table.IndexAdd(target.AsSlice(), row, txn);
        table.Put(target_enkey, target_envalue, txn);
        count++;
      }
    }

    records.push_back(row.DecodeRecord(schema, true));
  }

  return std::pair(sql::SqlResultSet(schema, std::move(records)), count);
}

// Count the number of shards each record with a given PK value is in.
std::vector<size_t> RocksdbSession::CountShards(
    const std::string &table_name,
    const std::vector<sqlast::Value> &pk_values) const {
  // Look up table info.
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Lookup counts.
  return table.GetPKIndex().CountShards(
      EncodeValues(schema.TypeOf(table.PKColumn()), pk_values),
      this->txn_.get());
}

// Delete records from shard, moving indicated ones to default shard.
int RocksdbSession::DeleteFromShard(
    const std::string &table_name, const util::ShardName &shard_name,
    const std::vector<dataflow::Record> &records,
    const std::vector<bool> &move_to_default) {
  CHECK(this->write_txn_) << "DeleteFromShard called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  util::ShardName default_shard(DEFAULT_SHARD, DEFAULT_SHARD);

  // Look up table info.
  RocksdbTable &table = this->conn_->tables_.at(table_name);
  size_t pk_index = table.PKColumn();

  // Construct and encrypt key.
  std::vector<EncryptedKey> en_keys;
  std::vector<RocksdbSequence> rows;
  for (const dataflow::Record &record : records) {
    RocksdbSequence key;
    key.Append(shard_name);
    key.Append(record.GetValue(pk_index));
    en_keys.push_back(this->conn_->encryption_.EncryptKey(std::move(key)));
    rows.push_back(RocksdbSequence::FromRecord(record));
  }

  // Delete all keys.
  int count = 0;
  for (size_t i = 0; i < records.size(); i++) {
    table.IndexDelete(shard_name.AsSlice(), rows.at(i), txn);
    table.Delete(en_keys.at(i), txn);
    count++;

    // Move to default shard if appropriate.
    if (move_to_default.at(i)) {
      table.IndexAdd(default_shard.AsSlice(), rows.at(i), txn);

      // Encrypt record.
      RocksdbSequence key;
      key.Append(default_shard);
      key.Append(records.at(i).GetValue(pk_index));

      // Add to table with default shard.
      table.Put(this->conn_->encryption_.EncryptKey(std::move(key)),
                this->conn_->encryption_.EncryptValue(default_shard.ByRef(),
                                                      std::move(rows.at(i))),
                txn);
      count++;
    }
  }

  return count;
}

std::unordered_set<util::ShardName> RocksdbSession::FindShards(
    const std::string &table_name, size_t column_index,
    const sqlast::Value &value) const {
  RocksdbInterface *txn =
      reinterpret_cast<RocksdbInterface *>(this->txn_.get());

  // Encode value.
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  // CHECK(!value.IsNull()) << "Val is NULL";
  std::string encoded = EncodeValue(table.Schema().TypeOf(column_index), value);

  // Check via index.
  std::unordered_set<util::ShardName> shards;
  RocksdbPlan plan = table.ChooseIndex(column_index);
  switch (plan.type()) {
    case RocksdbPlan::IndexChoiceType::PK: {
      const RocksdbPKIndex &index = table.GetPKIndex();
      IndexSet set = index.Get({encoded}, txn);
      for (const RocksdbIndexRecord &record : set) {
        shards.emplace(record.GetShard().ToString());
      }
      break;
    }
    case RocksdbPlan::IndexChoiceType::UNIQUE:
    case RocksdbPlan::IndexChoiceType::REGULAR: {
      // Look up via index.
      const RocksdbIndex &index = table.GetTableIndex(plan.idx());
      IndexSet set = index.Get({encoded}, txn);
      for (const RocksdbIndexRecord &record : set) {
        shards.emplace(record.GetShard().ToString());
      }
      break;
    }
    default:
      CHECK(false) << "FindShards on non-indexed column";
  }

  return shards;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
