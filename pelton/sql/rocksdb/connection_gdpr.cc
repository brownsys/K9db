// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include <utility>

#include "pelton/dataflow/schema.h"

namespace pelton {
namespace sql {

/*
 * Shard-based operations for GDPR GET/FORGET.
 */
SqlResultSet RocksdbConnection::GetShard(const std::string &table_name,
                                         util::ShardName &&shard_name) const {
  const RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Holds the result.
  std::vector<dataflow::Record> records;

  // Encrypt the shard name.
  EncryptedPrefix seek =
      this->encryption_manager_.EncryptSeek(std::move(shard_name));

  // Get all content of shard.
  RocksdbStream stream = table.GetShard(seek);
  for (auto [enkey, envalue] : stream) {
    RocksdbSequence key =
        this->encryption_manager_.DecryptKey(std::move(enkey));
    std::string shard = key.At(0).ToString();
    RocksdbSequence value =
        this->encryption_manager_.DecryptValue(shard, std::move(envalue));
    records.push_back(value.DecodeRecord(schema, true));
  }

  return SqlResultSet(schema, std::move(records));
}

SqlResultSet RocksdbConnection::DeleteShard(const std::string &table_name,
                                            util::ShardName &&shard_name) {
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Holds the result.
  std::vector<dataflow::Record> records;

  // Encrypt the shard name.
  EncryptedPrefix seek =
      this->encryption_manager_.EncryptSeek(std::move(shard_name));

  // Get all content of shard.
  RocksdbStream stream = table.GetShard(seek);
  for (auto [enkey, envalue] : stream) {
    // Delete in table.
    table.Delete(enkey);

    // Decrypt and add to result set.
    RocksdbSequence key =
        this->encryption_manager_.DecryptKey(std::move(enkey));
    std::string shard = key.At(0).ToString();
    RocksdbSequence value =
        this->encryption_manager_.DecryptValue(shard, std::move(envalue));
    records.push_back(value.DecodeRecord(schema, false));

    // Delete in indices.
    table.IndexDelete(shard, value);
  }

  return SqlResultSet(schema, std::move(records));
}

}  // namespace sql
}  // namespace pelton
