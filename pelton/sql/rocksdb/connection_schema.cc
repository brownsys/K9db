// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

#include "glog/logging.h"
#include "pelton/util/status.h"

namespace pelton {
namespace sql {

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
      std::make_tuple(this->db_.get(), table_name, schema));
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
      }
    }
  }

  return true;
}

// Execute Create Index Statement
bool RocksdbConnection::ExecuteCreateIndex(const sqlast::CreateIndex &stmt) {
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->tables_.at(table_name);
  table.CreateIndex(stmt.column_names());
  return true;
}

}  // namespace sql
}  // namespace pelton
