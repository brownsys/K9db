// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include "glog/logging.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace pelton {
namespace sql {
namespace rocks {

/*
 * INSERT STATEMENTS.
 */

bool RocksdbSession::Exists(const std::string &table_name,
                            const sqlast::Value &pk) const {
  CHECK(this->write_txn_) << "Exists called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  CHECK(!pk.IsNull()) << "PK is NULL";
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  size_t pk_index = table.Schema().keys().front();
  std::string pk_value = EncodeValue(table.Schema().TypeOf(pk_index), pk);
  return table.Exists(pk_value, txn);
}

bool RocksdbSession::Exists(const std::string &table_name, size_t column_index,
                            const sqlast::Value &val) const {
  CHECK(this->write_txn_) << "Exists(2) called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  // Special case: column index refers to PK.
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  if (table.Schema().keys().front() == column_index) {
    return this->Exists(table_name, val);
  }

  // Check via index.
  RocksdbPlan plan = table.ChooseIndex(column_index);
  CHECK(plan.type() != RocksdbPlan::IndexChoiceType::PK) << "UNREACHABLE";
  CHECK(plan.type() != RocksdbPlan::IndexChoiceType::SCAN)
      << "Exists(2) on non-indexed column";

  // Encode value.
  CHECK(!val.IsNull()) << "Val is NULL";
  std::string encoded = EncodeValue(table.Schema().TypeOf(column_index), val);

  // Look up via index.
  const RocksdbIndex &index = table.GetTableIndex(plan.idx());
  IndexSet set = index.Get({encoded}, txn);
  return set.size() > 0;
}

int RocksdbSession::ExecuteInsert(const sqlast::Insert &stmt,
                                  const util::ShardName &shard_name) {
  CHECK(this->write_txn_) << "Insert called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Encode row.
  RocksdbRecord record = RocksdbRecord::FromInsert(stmt, schema, shard_name);

  // Update indices.
  table.IndexAdd(shard_name.AsSlice(), record.Value(), txn);

  // Encrypt key and record value.
  EncryptedKey key =
      this->conn_->encryption_.EncryptKey(std::move(record.Key()));
  EncryptedValue value = this->conn_->encryption_.EncryptValue(
      shard_name.ByRef(), std::move(record.Value()));

  // Write to DB.
  table.Put(key, value, txn);

  // Done.
  return 1;
}

/*
 * REPLACE STATEMENTS.
 */

RecordAndStatus RocksdbSession::ExecuteReplace(
    const sqlast::Replace &stmt, const util::ShardName &shard_name) {
  CHECK(this->write_txn_) << "Replace called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->conn_->tables_.at(table_name);
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
  IndexSet set = table.GetPKIndex().Get(std::move(pkstr), txn);
  for (auto it = set.begin(); it != set.end();) {
    auto node = set.extract(it++);
    RocksdbIndexRecord &key = node.value();
    shards.push_back(key.GetShard().ToString());
    enkeys.push_back(this->conn_->encryption_.EncryptKey(key.Release()));
    if (shards.back() == shard_name) {
      same_shard_index = shards.size() - 1;
    }
  }

  // Lookup any existing records.
  std::vector<std::optional<EncryptedValue>> envalues =
      table.MultiGet(enkeys, txn);
  for (size_t i = 0; i < envalues.size(); i++) {
    std::optional<EncryptedValue> &opt = envalues.at(i);
    if (opt.has_value()) {
      // Decrypt record.
      std::string &shard = shards.at(i);
      RocksdbSequence value =
          this->conn_->encryption_.DecryptValue(shard, std::move(*opt));
      if (!result.has_value()) {
        result = value.DecodeRecord(schema, false);
      }

      // If this existing record has the same shard as what we are inserting,
      // we do not need to delete it (or update the PK index).
      // We can just overwrite on top of it, but we will need to update other
      // indices if they exist.
      if (static_cast<int>(i) != same_shard_index) {
        count++;
        table.IndexDelete(shard, value, txn);
        table.Delete(enkeys.at(i), txn);
      } else {
        table.IndexDelete(shard, value, txn, false);
      }
    }
  }

  // Encode row to be inserted.
  RocksdbRecord record = RocksdbRecord::FromInsert(stmt, schema, shard_name);

  // We do not need to update PK index if an existing record had the same shard.
  table.IndexAdd(shard_name.AsSlice(), record.Value(), txn,
                 same_shard_index == -1);

  // Encrypt record key and value.
  EncryptedKey key =
      same_shard_index > -1
          ? enkeys.at(same_shard_index)
          : this->conn_->encryption_.EncryptKey(std::move(record.Key()));
  EncryptedValue value = this->conn_->encryption_.EncryptValue(
      shard_name.ByRef(), std::move(record.Value()));

  // Write to DB.
  table.Put(key, value, txn);
  count++;

  // Done.
  return RecordAndStatus(std::move(result), count);
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
