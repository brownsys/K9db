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
    const auto &column = stmt.GetColumns().at(i);
    for (const auto &cnstr : column.GetConstraints()) {
      if (cnstr.type() == sqlast::ColumnConstraint::Type::PRIMARY_KEY ||
          cnstr.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY ||
          cnstr.type() == sqlast::ColumnConstraint::Type::UNIQUE) {
        it->second.CreateIndex(i);
        break;
      }
    }
  }

  // Done.
  return true;
}

// Execute Create Index Statement
bool RocksdbConnection::ExecuteCreateIndex(const sqlast::CreateIndex &stmt) {
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->tables_.at(table_name);
  table.CreateIndex(stmt.column_name());
  return true;
}

}  // namespace sql
}  // namespace pelton
