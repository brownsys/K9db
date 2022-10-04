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
  const dataflow::SchemaRef &schema =
      this->tables_.at(table_name).front().Schema();
  const sqlast::BinaryExpression *const where = sql.GetWhereClause();

  // Filter by where clause.
  std::vector<dataflow::Record> records;
  if (where != nullptr) {
    records = this->GetRecords(sql.table_name(), *where);
  } else {
    records = this->GetAll(table_name).Vec();
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
  const dataflow::SchemaRef &schema =
      this->tables_.at(table_name).front().Schema();

  // Get from every copy of table.
  SqlResultSet result(schema);
  for (const RocksdbTable &table : this->tables_.at(table_name)) {
    // Holds the result.
    std::vector<dataflow::Record> records;
    std::vector<std::string> keys;

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
      keys.push_back(key.At(1).ToString());
    }
    result.Append(SqlResultSet(schema, std::move(records), std::move(keys)),
                  true);
  }
  return result;
}

// Get records matching where condition.
std::vector<dataflow::Record> RocksdbConnection::GetRecords(
    const std::string &table_name,
    const sqlast::BinaryExpression &where) const {
  const dataflow::SchemaRef &schema =
      this->tables_.at(table_name).front().Schema();

  SqlResultSet result(schema);
  bool first = true;
  for (const RocksdbTable &table : this->tables_.at(table_name)) {
    // Turn where condition into a value mapper.
    sqlast::ValueMapper value_mapper(schema);
    value_mapper.VisitBinaryExpression(where);

    // Look up existing indices.
    std::vector<dataflow::Record> records;
    std::vector<std::string> dup_keys;
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
      std::vector<std::optional<EncryptedValue>> envalues =
          table.MultiGet(keys);

      // Decrypt all values.
      for (size_t i = 0; i < envalues.size(); i++) {
        std::optional<EncryptedValue> &opt = envalues.at(i);
        if (opt.has_value()) {
          const std::string &shard = shards.at(i);
          RocksdbSequence value =
              this->encryption_manager_.DecryptValue(shard, std::move(*opt));
          dup_keys.push_back(value.At(schema.keys().at(0)).ToString());
          records.push_back(value.DecodeRecord(schema));
        }
      }
    } else {
      if (first) {
        // No relevant index; iterate over everything.
        LOG(WARNING) << "Selecting by scan from table " << table_name;
      }

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

        dup_keys.push_back(key.At(1).ToString());
        records.push_back(value.DecodeRecord(schema));
      }
    }

    // Apply remaining filters (if any).
    if (!value_mapper.EmptyBefore()) {
      std::vector<dataflow::Record> filtered;
      std::vector<std::string> filtered_keys;
      for (size_t i = 0; i < records.size(); i++) {
        dataflow::Record &record = records.at(i);
        if (InMemoryFilter(value_mapper, record)) {
          filtered.push_back(std::move(record));
          filtered_keys.push_back(std::move(dup_keys.at(i)));
        }
      }
      records = std::move(filtered);
      dup_keys = std::move(filtered_keys);
    }

    result.Append(SqlResultSet(schema, std::move(records), std::move(dup_keys)),
                  true);
    first = false;
  }

  return result.Vec();
}

// Everything in a table.
SqlResultSet RocksdbConnection::GetAll(const std::string &table_name) const {
  const dataflow::SchemaRef &schema =
      this->tables_.at(table_name).front().Schema();

  // Get from every copy of table.
  SqlResultSet result(schema);
  size_t copies = this->tables_.at(table_name).size();
  for (size_t copy = 0; copy < copies; copy++) {
    result.Append(this->GetAll(table_name, copy), true);
  }
  return result;
}
SqlResultSet RocksdbConnection::GetAll(const std::string &table_name,
                                       size_t copy) const {
  std::vector<dataflow::Record> records;
  std::vector<std::string> keys;

  const RocksdbTable &table = this->tables_.at(table_name).at(copy);
  RocksdbStream stream = table.GetAll();
  for (auto [enkey, envalue] : stream) {
    RocksdbSequence key =
        this->encryption_manager_.DecryptKey(std::move(enkey));
    std::string shard = key.At(0).ToString();
    RocksdbSequence value =
        this->encryption_manager_.DecryptValue(shard, std::move(envalue));
    records.push_back(value.DecodeRecord(table.Schema()));
    keys.push_back(key.At(1).ToString());
  }

  return SqlResultSet(table.Schema(), std::move(records), std::move(keys));
}

}  // namespace sql
}  // namespace pelton
