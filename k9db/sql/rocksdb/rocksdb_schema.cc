// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include "glog/logging.h"
#include "k9db/sql/rocksdb/dedup.h"
#include "k9db/util/status.h"

namespace k9db {
namespace sql {
namespace rocks {

/*
 * CREATE STATEMENTS.
 */

bool RocksdbConnection::ExecuteCreateTable(const sqlast::CreateTable &stmt) {
  // Check table does not exist.
  const std::string &table_name = stmt.table_name();
  if (this->tables_.count(table_name) > 0) {
    LOG(FATAL) << "Table already exists!";
  }

  // Create schema.
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(stmt);

  // Create table.
  auto [it, flag] = this->tables_.emplace(
      std::piecewise_construct, std::make_tuple(table_name),
      std::make_tuple(this->db_.get(), table_name, schema, &this->metadata_));
  CHECK(flag);

  // Create indices for table.
  for (size_t i = 0; i < stmt.GetColumns().size(); i++) {
    // Primary key already has index created for it.
    if (i == schema.keys().front()) {
      continue;
    }
    // For non-PK columns, we create an index automatically if unique or FK.
    const auto &column = stmt.GetColumns().at(i);
    for (const auto &cnstr : column.GetConstraints()) {
      switch (cnstr.type()) {
        case sqlast::ColumnConstraint::Type::UNIQUE:
          it->second.AddUniqueColumn(i);
        case sqlast::ColumnConstraint::Type::FOREIGN_KEY:
          it->second.CreateIndex(std::vector<size_t>{i});
          break;
        default:
          continue;
      }
    }
  }

  // After table creation succeeds. Persist the statement so that it can be
  // reloaded on restart.
  this->metadata_.Persist(stmt);

  return true;
}

// Execute Create Index Statement.
bool RocksdbConnection::ExecuteCreateIndex(const sqlast::CreateIndex &stmt) {
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->tables_.at(table_name);
  std::string cf_name = table.CreateIndex(stmt.column_names());

  // After index creation succeeds. Persist the statement so that it can be
  // reloaded on restart.
  this->metadata_.Persist(cf_name, stmt);

  return true;
}

// Persist a Create View Statement to disk.
bool RocksdbConnection::PersistCreateView(const sqlast::CreateView &sql) {
  this->metadata_.Persist(sql);
  return true;
}

// Get the max value for an INT column (for reloading auto increment counter).
int64_t RocksdbConnection::GetMaximumValue(
    const std::string &table_name, const std::string &column_name) const {
  if (this->tables_.count(table_name) == 0) {
    LOG(FATAL) << "Table does not exist";
  }

  // Find column in schema.
  const RocksdbTable &table = this->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();
  size_t column_index = schema.IndexOf(column_name);
  CHECK_EQ(schema.TypeOf(column_index), sqlast::ColumnDefinition::Type::INT)
      << "GetMaximumValue() called for a non-INT column";

  // Read all the rows.
  int64_t max = 0;
  RocksdbReadSnapshot txn(this->db_.get());
  RocksdbStream all = table.GetAll(&txn);
  DedupSet<std::string> dup_keys;
  for (auto [enkey, enval] : all) {
    // Decrypt key.
    RocksdbSequence key = this->encryption_.DecryptKey(std::move(enkey));

    // Use shard from decrypted key to decrypt value.
    if (!dup_keys.Duplicate(key.At(1).ToString())) {
      // Decrypt record.
      std::string shard = key.At(0).ToString();
      RocksdbSequence value =
          this->encryption_.DecryptValue(shard, std::move(enval));

      // Track max.
      dataflow::Record record = value.DecodeRecord(schema, true);
      int64_t v = record.GetInt(column_index);
      if (v > max) {
        max = v;
      }
    }
  }

  return max;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
