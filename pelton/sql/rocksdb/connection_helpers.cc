// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include "glog/logging.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/filter.h"

namespace pelton {
namespace sql {

// Get records matching where condition.
std::vector<dataflow::Record> RocksdbConnection::GetRecords(
    const std::string &table_name,
    const sqlast::BinaryExpression &where) const {
  const RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Turn where condition into a value mapper.
  sqlast::ValueMapper value_mapper(schema);
  value_mapper.VisitBinaryExpression(where);

  // Look up existing indices.
  std::vector<dataflow::Record> records;
  std::optional<KeySet> lookup = table.IndexLookup(&value_mapper);
  if (lookup.has_value()) {
    // Can lookup by index.
    KeySet &set = *lookup;

    // Encrypt all keys; keep track of corresponding shards for decryption.
    std::vector<std::string> shards;
    std::vector<EncryptedKey> keys;
    for (const RocksdbSequence &key : set) {
      shards.push_back(key.At(0).ToString());
      keys.push_back(this->encryption_manager_.EncryptKey(
          std::move(const_cast<RocksdbSequence &>(key))));
    }

    // Multi Lookup by encrypted keys.
    std::vector<std::optional<EncryptedValue>> envalues = table.MultiGet(keys);

    // Decrypt all values.
    for (size_t i = 0; i < envalues.size(); i++) {
      std::optional<EncryptedValue> &opt = envalues.at(i);
      if (opt.has_value()) {
        const std::string &shard = shards.at(i);
        RocksdbSequence value =
            this->encryption_manager_.DecryptValue(shard, std::move(*opt));
        records.push_back(value.DecodeRecord(schema));
      }
    }
  } else {
    // No relevant index; iterate over everything.
    LOG(WARNING) << "Getting all records from table " << table_name;

    // Iterate over everything.
    RocksdbStream all = table.GetAll();
    for (auto [enkey, enval] : all) {
      // Decrypt key.
      RocksdbSequence key =
          this->encryption_manager_.DecryptKey(std::move(enkey));

      // Use shard from decrypted key to decrypt value.
      std::string shard = key.At(0).ToString();
      RocksdbSequence value =
          this->encryption_manager_.DecryptValue(shard, std::move(enval));

      records.push_back(value.DecodeRecord(schema));
    }
  }

  // Apply remaining filters (if any).
  if (!value_mapper.EmptyBefore()) {
    std::vector<dataflow::Record> filtered;
    for (dataflow::Record &record : records) {
      if (InMemoryFilter(value_mapper, record)) {
        filtered.push_back(std::move(record));
      }
    }
    records = std::move(filtered);
  }

  return records;
}

}  // namespace sql
}  // namespace pelton
