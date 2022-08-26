#include "glog/logging.h"
#include "pelton/sql/rocksdb/connection.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

/*
 * CREATE STATEMENTS.
 */
bool RocksdbConnection::ExecuteCreateTable(const sqlast::CreateTable &stmt) {
  const std::string &table_name = stmt.table_name();
  if (this->schemas_.count(table_name) > 0) {
    LOG(FATAL) << "Table already exists!";
  }

  // Schema and PK.
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(stmt);
  const std::vector<dataflow::ColumnID> &keys = schema.keys();
  CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";

  // Create rocksdb table.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.OptimizeLevelStyleCompaction();
  options.prefix_extractor.reset(new PeltonPrefixTransform());
  options.comparator = PeltonComparator();
  PANIC(this->db_->CreateColumnFamily(options, table_name, &handle));

  // Fill in table metadata.
  this->schemas_.emplace(table_name, schema);
  this->handlers_.emplace(table_name, handle);
  std::vector<std::optional<RocksdbIndex>> &indices =
      this->indices_[table_name];
  for (size_t i = 0; i < schema.size(); i++) {
    indices.push_back(std::optional<RocksdbIndex>());
  }

  // Create indices for table.
  for (size_t i = 0; i < stmt.GetColumns().size(); i++) {
    const auto &column = stmt.GetColumns().at(i);
    bool has_constraint = false;
    for (const auto &cnstr : column.GetConstraints()) {
      if (cnstr.type() == sqlast::ColumnConstraint::Type::PRIMARY_KEY ||
          cnstr.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY ||
          cnstr.type() == sqlast::ColumnConstraint::Type::UNIQUE) {
        has_constraint = true;
        break;
      }
    }
    if (has_constraint) {
      indices[i] = RocksdbIndex(this->db_.get(), table_name, i);
    }
  }

  // Done.
  return true;
}

// Execute Create Index Statement
bool RocksdbConnection::ExecuteCreateIndex(const sqlast::CreateIndex &stmt) {
  const std::string &table_name = stmt.table_name();
  const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
  size_t column = schema.IndexOf(stmt.column_name());
  std::vector<std::optional<RocksdbIndex>> &indices =
      this->indices_.at(table_name);
  std::optional<RocksdbIndex> &index = indices.at(column);
  if (!index.has_value()) {
    index = RocksdbIndex(this->db_.get(), table_name, column);
  }
  return true;
}

}  // namespace sql
}  // namespace pelton
