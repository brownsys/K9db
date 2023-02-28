// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/rocksdb_connection.h"
// clang-format on

namespace pelton {
namespace sql {
namespace rocks {

namespace {
std::string Join(const std::vector<size_t> &cols,
                 const dataflow::SchemaRef &schema) {
  std::string res = "(";
  for (size_t col : cols) {
    res += schema.NameOf(col) + ", ";
  }
  res.pop_back();
  res.pop_back();
  res.push_back(')');
  return res;
}

}  // namespace

std::vector<std::string> RocksdbConnection::GetIndices(
    const std::string &tbl) const {
  return this->tables_.at(tbl).GetIndices();
}

std::string RocksdbConnection::GetIndex(
    const std::string &tbl, const sqlast::BinaryExpression *const where) const {
  const RocksdbTable &table = this->tables_.at(tbl);
  const dataflow::SchemaRef &schema = table.Schema();

  // No where condition.
  if (where == nullptr) {
    return "[SCAN]";
  }

  // Parse where condition.
  sqlast::ValueMapper value_mapper(schema);
  value_mapper.VisitBinaryExpression(*where);

  // Find plan and describe it.
  std::vector<size_t> index_cols;
  std::string result = "";
  RocksdbTable::IndexChoice plan = table.ChooseIndex(&value_mapper);
  switch (plan.type) {
    case RocksdbTable::IndexChoiceType::PK: {
      index_cols.push_back(table.PKColumn());
      result = "(" + schema.NameOf(table.PKColumn()) + ")";
      break;
    }
    case RocksdbTable::IndexChoiceType::UNIQUE: {
      const RocksdbIndex &index = table.GetTableIndex(plan.idx);
      index_cols = index.GetColumns();
      result = Join(index_cols, schema) + " [UNIQUE]";
      break;
    }
    case RocksdbTable::IndexChoiceType::REGULAR: {
      const RocksdbIndex &index = table.GetTableIndex(plan.idx);
      index_cols = index.GetColumns();
      result = Join(index_cols, schema) + " [TOTAL]";
      break;
    }
    case RocksdbTable::IndexChoiceType::SCAN: {
      result = "[SCAN]";
      break;
    }
    default: {
      LOG(FATAL) << "UNSUPPORTED PLAN";
    }
  }

  // Describe any remaining filters.
  auto remaining_clauses = value_mapper.Values();
  for (size_t index_col : index_cols) {
    remaining_clauses.erase(index_col);
  }
  if (!remaining_clauses.empty()) {
    result += " WITH FILTERS (";
    for (const auto &[column, _] : remaining_clauses) {
      result += schema.NameOf(column) + ", ";
    }
    result.pop_back();
    result.pop_back();
    result += ") [INMEMORY]";
  }
  return result;
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
