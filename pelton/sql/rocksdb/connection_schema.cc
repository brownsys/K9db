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

bool RocksdbConnection::ExecuteCreateTable(const sqlast::CreateTable &stmt,
                                           size_t copies) {
  // Check table does not exist.
  const std::string &name = stmt.table_name();

  // Create schema.
  dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(stmt);

  // Create table.
  std::vector<RocksdbTable> &vec = this->tables_[name];
  size_t offset = vec.size();
  for (size_t i = offset; i < offset + copies; i++) {
    vec.emplace_back(this->db_.get(), name + "__" + std::to_string(i), schema);

    // Create indices for table.
    for (size_t i = 0; i < stmt.GetColumns().size(); i++) {
      const auto &column = stmt.GetColumns().at(i);
      for (const auto &cnstr : column.GetConstraints()) {
        if (cnstr.type() == sqlast::ColumnConstraint::Type::PRIMARY_KEY ||
            cnstr.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY ||
            cnstr.type() == sqlast::ColumnConstraint::Type::UNIQUE) {
          vec.back().CreateIndex(i);
          break;
        }
      }
    }
  }

  // Done.
  return true;
}

// Execute Create Index Statement
bool RocksdbConnection::ExecuteCreateIndex(const sqlast::CreateIndex &stmt) {
  CHECK(stmt.ondisk()) << "Creating a rocksdb index for a non-disk statement";
  const std::string &table_name = stmt.table_name();
  for (RocksdbTable &table : this->tables_.at(table_name)) {
    table.CreateIndex(stmt.column_name());
  }
  return true;
}

}  // namespace sql
}  // namespace pelton
