// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include <utility>

#include "glog/logging.h"
#include "pelton/sql/rocksdb/filter.h"

namespace pelton {
namespace sql {

namespace {

struct DelElement {
  std::string shard;
  EncryptedKey key;
  RocksdbSequence value;
  DelElement(std::string &&s, EncryptedKey &&k, RocksdbSequence &&v)
      : shard(std::move(s)), key(std::move(k)), value(std::move(v)) {}
};

}  // namespace

/*
 * DELETE STATEMENTS.
 */

SqlResultSet RocksdbConnection::ExecuteDelete(const sqlast::Delete &sql) {
  const std::string &table_name = sql.table_name();
  RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  const sqlast::BinaryExpression *const where = sql.GetWhereClause();
  if (where == nullptr) {
    LOG(FATAL) << "Delete with no where clause";
  }

  // Turn where condition into a value mapper.
  sqlast::ValueMapper value_mapper(schema);
  value_mapper.VisitBinaryExpression(*where);

  // Look up existing indices.
  std::vector<DelElement> records;
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
      std::optional<EncryptedValue> &envalue = envalues.at(i);
      if (envalue.has_value()) {
        std::string &shard = shards.at(i);
        RocksdbSequence val =
            this->encryption_manager_.DecryptValue(shard, std::move(*envalue));

        records.emplace_back(std::move(shard), std::move(keys.at(i)),
                             std::move(val));
      }
    }
  } else {
    // No relevant index; iterate over everything.
    LOG(WARNING) << "Deleting by scan in table " << table_name;

    // Iterate over everything.
    RocksdbStream all = table.GetAll();
    for (auto [enkey, enval] : all) {
      // Decrypt key.
      EncryptedKey copy = enkey;
      RocksdbSequence key =
          this->encryption_manager_.DecryptKey(std::move(enkey));

      // Use shard from decrypted key to decrypt value.
      std::string shard = key.At(0).ToString();
      RocksdbSequence value =
          this->encryption_manager_.DecryptValue(shard, std::move(enval));

      records.emplace_back(std::move(shard), std::move(copy), std::move(value));
    }
  }

  // Iterate over records to be deleted and delete them.
  std::vector<dataflow::Record> result;
  const bool filter = !value_mapper.EmptyBefore();
  for (DelElement &element : records) {
    // If any filters remain check them.
    dataflow::Record record = element.value.DecodeRecord(schema);
    if (filter && !InMemoryFilter(value_mapper, record)) {
      continue;
    }

    // Update indices.
    table.IndexDelete(element.shard, element.value);

    // Delete record.
    table.Delete(element.key);

    // Append record to result.
    result.push_back(std::move(record));
  }

  return SqlResultSet(schema, std::move(result));
}

}  // namespace sql
}  // namespace pelton
