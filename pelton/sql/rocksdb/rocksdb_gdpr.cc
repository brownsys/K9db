// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include <utility>

#include "pelton/dataflow/schema.h"

namespace pelton {
namespace sql {
namespace rocks {

/*
 * Shard-based operations for GDPR GET/FORGET.
 */
SqlResultSet RocksdbSession::GetShard(const std::string &table_name,
                                      util::ShardName &&shard_name) const {
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Holds the result.
  std::vector<dataflow::Record> records;

  // Encrypt the shard name.
  EncryptedPrefix seek =
      this->conn_->encryption_.EncryptSeek(std::move(shard_name));

  // Get all content of shard.
  RocksdbStream stream = table.GetShard(seek, this->txn_.get());
  for (auto [enkey, envalue] : stream) {
    RocksdbSequence key = this->conn_->encryption_.DecryptKey(std::move(enkey));
    std::string shard = key.At(0).ToString();
    RocksdbSequence value =
        this->conn_->encryption_.DecryptValue(shard, std::move(envalue));
    records.push_back(value.DecodeRecord(schema, true));
  }

  return SqlResultSet(schema, std::move(records));
}

SqlResultSet RocksdbSession::DeleteShard(const std::string &table_name,
                                         util::ShardName &&shard_name) {
  CHECK(this->write_txn_) << "DeleteShard called on read txn";
  RocksdbWriteTransaction *txn =
      reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get());

  RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Holds the result.
  std::vector<dataflow::Record> records;

  // Encrypt the shard name.
  EncryptedPrefix seek =
      this->conn_->encryption_.EncryptSeek(std::move(shard_name));

  // Get all content of shard.
  RocksdbStream stream = table.GetShard(seek, txn);
  for (auto [enkey, envalue] : stream) {
    // Delete in table.
    table.Delete(enkey, txn);

    // Decrypt and add to result set.
    RocksdbSequence key = this->conn_->encryption_.DecryptKey(std::move(enkey));
    std::string shard = key.At(0).ToString();
    RocksdbSequence value =
        this->conn_->encryption_.DecryptValue(shard, std::move(envalue));
    records.push_back(value.DecodeRecord(schema, false));

    // Delete in indices.
    table.IndexDelete(shard, value, txn);
  }

  return SqlResultSet(schema, std::move(records));
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
