#ifndef K9DB_SQL_ROCKSDB_PLAN_H__
#define K9DB_SQL_ROCKSDB_PLAN_H__

#include <string>
#include <vector>

#include "k9db/dataflow/schema.h"
#include "k9db/sqlast/ast.h"
#include "k9db/sqlast/value_mapper.h"

namespace k9db {
namespace sql {
namespace rocks {

// Plan describing how to handle looking up a where condition.
class RocksdbPlan {
 public:
  enum IndexChoiceType { PK, UNIQUE, REGULAR, SCAN };

  // Constructor.
  RocksdbPlan(const std::string &table_name,
              const dataflow::SchemaRef &table_schema)
      : table_name_(table_name),
        table_schema_(table_schema),
        type_(IndexChoiceType::SCAN),
        cols_(),
        idx_(0) {}

  // Make a certain type.
  void MakeScan() { this->type_ = IndexChoiceType::SCAN; }
  void MakePK(const std::vector<size_t> &cols) {
    this->type_ = IndexChoiceType::PK;
    this->cols_ = cols;
  }
  void MakeIndex(bool unique, const std::vector<size_t> &cols, size_t idx) {
    this->cols_ = cols;
    this->idx_ = idx;
    if (unique) {
      this->type_ = IndexChoiceType::UNIQUE;
    } else {
      this->type_ = IndexChoiceType::REGULAR;
    }
  }

  // Accessors.
  IndexChoiceType type() const { return this->type_; }
  const std::vector<size_t> &cols() const { return this->cols_; }
  size_t idx() const { return this->idx_; }

  // Turn this into a string description for EXPLAIN.
  std::string ToString(const sqlast::ValueMapper &value_mapper) const;

 private:
  // Table information.
  std::string table_name_;
  dataflow::SchemaRef table_schema_;

  // Index information.
  IndexChoiceType type_;
  std::vector<size_t> cols_;
  size_t idx_;  // Index number for quickly fetching index on lookup.
};

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_PLAN_H__
