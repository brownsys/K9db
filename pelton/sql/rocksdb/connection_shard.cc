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
                                         const std::string &shard_kind,
                                         const std::string &user_id) const {
  const RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Holds the result.
  std::vector<dataflow::Record> records;

  // Encrypt the shard name.
  std::string shard_name = shard_kind + "__" + user_id;
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
    records.push_back(value.DecodeRecord(schema));
  }

  return SqlResultSet(schema, std::move(records));
}

SqlResultSet RocksdbConnection::DeleteShard(const std::string &table_name,
                                            const std::string &shard_kind,
                                            const std::string &user_id) {
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Holds the result.
  std::vector<dataflow::Record> records;

  // Encrypt the shard name.
  std::string shard_name = shard_kind + "__" + user_id;
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
    records.push_back(value.DecodeRecord(schema));

    // Delete in indices.
    table.IndexDelete(shard, value);
  }

  return SqlResultSet(schema, std::move(records));
}

}  // namespace sql
}  // namespace pelton
