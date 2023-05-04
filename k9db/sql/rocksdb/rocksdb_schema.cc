// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include "glog/logging.h"
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

#ifndef K9DB_PHYSICAL_SEPARATION
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
#endif  // K9DB_PHYSICAL_SEPARATION

  // After table creation succeeds. Persist the statement so that it can be
  // reloaded on restart.
  this->metadata_.Persist(stmt);

  return true;
}

// Execute Create Index Statement.
bool RocksdbConnection::ExecuteCreateIndex(const sqlast::CreateIndex &stmt) {
#ifdef K9DB_PHYSICAL_SEPARATION
  LOG(FATAL) << "UNSUPPORTED";
#else
  const std::string &table_name = stmt.table_name();
  RocksdbTable &table = this->tables_.at(table_name);
  std::string cf_name = table.CreateIndex(stmt.column_names());

  // After index creation succeeds. Persist the statement so that it can be
  // reloaded on restart.
  this->metadata_.Persist(cf_name, stmt);

  return true;
#endif
}

// Persist a Create View Statement to disk.
bool RocksdbConnection::PersistCreateView(const sqlast::CreateView &sql) {
  this->metadata_.Persist(sql);
  return true;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
