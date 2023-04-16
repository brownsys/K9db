#include "pelton/sql/rocksdb/plan.h"

namespace pelton {
namespace sql {
namespace rocks {

// Helpers that return information about metadata and schema for use in
// EXPLAIN COMPLIANCE
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

// Turn this into a string description for EXPLAIN.
std::string RocksdbPlan::ToString(
    const sqlast::ValueMapper &value_mapper) const {
  std::string result = Join(this->cols_, this->table_schema_);

  // Figure out index type.
  switch (this->type_) {
    case IndexChoiceType::SCAN: {
      return "[SCAN]";
    }
    case IndexChoiceType::PK: {
      result += " [PK]";
      break;
    }
    case IndexChoiceType::UNIQUE: {
      result += " [UNIQUE]";
      break;
    }
    case IndexChoiceType::REGULAR: {
      result += " [TOTAL]";
      break;
    }
    default: {
      LOG(FATAL) << "Unsupported";
    }
  }

  // Describe any remaining filters.
  auto remaining_clauses = value_mapper.Values();
  for (size_t index_col : this->cols_) {
    remaining_clauses.erase(index_col);
  }
  if (!remaining_clauses.empty()) {
    result += " WITH FILTERS (";
    for (const auto &[column, _] : remaining_clauses) {
      result += this->table_schema_.NameOf(column) + ", ";
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
