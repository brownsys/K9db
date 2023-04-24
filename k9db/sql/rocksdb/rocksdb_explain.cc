// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/rocksdb_connection.h"
// clang-format on

namespace k9db {
namespace sql {
namespace rocks {

std::vector<std::string> RocksdbConnection::GetIndices(
    const std::string &tbl) const {
  return this->tables_.at(tbl).GetIndices();
}

std::string RocksdbConnection::GetIndex(
    const std::string &tbl, const sqlast::BinaryExpression *const where) const {
  const RocksdbTable &table = this->tables_.at(tbl);
  const dataflow::SchemaRef &schema = table.Schema();

  // Parse where condition.
  sqlast::ValueMapper value_mapper(schema);
  if (where != nullptr) {
    value_mapper.VisitBinaryExpression(*where);
  }

  RocksdbPlan plan = table.ChooseIndex(&value_mapper);
  return plan.ToString(value_mapper);
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
