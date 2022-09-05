// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include "pelton/sql/rocksdb/project.h"

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

}  // namespace sql
}  // namespace pelton
