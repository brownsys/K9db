// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include "glog/logging.h"
#include "pelton/sql/rocksdb/project.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/filter.h"

namespace pelton {
namespace sql {

/*
 * SELECT STATEMENTS.
 */

SqlResultSet RocksdbConnection::ExecuteSelect(const sqlast::Select &sql) const {
  const std::string &table_name = sql.table_name();
  const RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();
  const sqlast::BinaryExpression *const where = sql.GetWhereClause();

  // Filter by where clause.
  std::vector<dataflow::Record> records;
  if (where != nullptr) {
    records = this->GetRecords(sql.table_name(), *where);
  } else {
    RocksdbStream stream = table.GetAll();
    for (auto [enkey, envalue] : stream) {
      RocksdbSequence key =
          this->encryption_manager_.DecryptKey(std::move(enkey));
      std::string shard = key.At(0).ToString();
      RocksdbSequence value =
          this->encryption_manager_.DecryptValue(shard, std::move(envalue));
      records.push_back(value.DecodeRecord(schema));
    }
  }

  // Apply projection, if any.
  Projection projection = ProjectionSchema(schema, sql.GetColumns());
  if (projection.schema != schema) {
    std::vector<dataflow::Record> projected;
    for (const dataflow::Record &record : records) {
      projected.push_back(Project(projection, record));
    }
    records = std::move(projected);
  }

  return SqlResultSet(projection.schema, std::move(records));
}

SqlResultSet RocksdbConnection::GetShard(const std::string &table_name,
                                         const std::string &shard_name) const {
  // Get table.
  const RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Holds the result.
  std::vector<dataflow::Record> records;

  // Encrypt the shard name.
  EncryptedPrefix seek =
      this->encryption_manager_.EncryptSeek(std::string(shard_name));
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
    LOG(WARNING) << "Selecting by scan from table " << table_name;

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
