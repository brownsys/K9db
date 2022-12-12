// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/rocksdb_connection.h"
// clang-format on

#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/project.h"

namespace pelton {
namespace sql {
namespace rocks {

/*
 * SELECT STATEMENTS.
 */

SqlResultSet RocksdbSession::ExecuteSelect(const sqlast::Select &sql) const {
  const std::string &table_name = sql.table_name();
  const sqlast::BinaryExpression *const where = sql.GetWhereClause();

  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Filter by where clause.
  std::vector<dataflow::Record> records =
      this->GetRecords<SelectRecord, true>(table_name, where, sql.limit());

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

// Everything in a table.
SqlResultSet RocksdbSession::GetAll(const std::string &table_name) const {
  const RocksdbTable &table = this->conn_->tables_.at(table_name);
  const dataflow::SchemaRef &schema = table.Schema();

  // Filter by where clause.
  std::vector<dataflow::Record> records =
      this->GetRecords<SelectRecord, true>(table_name, nullptr);

  return SqlResultSet(schema, std::move(records));
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
