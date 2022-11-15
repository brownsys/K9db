// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/connection.h"
// clang-format on

namespace pelton {
namespace sql {

std::vector<std::string> RocksdbConnection::GetIndices(
    const std::string &tbl) const {
  return this->tables_.at(tbl).GetIndices();
}

std::string RocksdbConnection::GetIndex(
    const std::string &tbl, const sqlast::BinaryExpression *const where) const {
  if (where != nullptr) {
    const RocksdbTable &table = this->tables_.at(tbl);
    sqlast::ValueMapper value_mapper(table.Schema());
    value_mapper.VisitBinaryExpression(*where);
    std::optional<std::string> index = table.ChooseIndex(&value_mapper);
    if (index.has_value()) {
      return index.value();
    }
  }
  return "SCAN";
}

}  // namespace sql
}  // namespace pelton
