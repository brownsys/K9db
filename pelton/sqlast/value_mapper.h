#ifndef PELTON_SQLAST_VALUE_MAPPER_H_
#define PELTON_SQLAST_VALUE_MAPPER_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sqlast {

// Transforms Where clauses into a map from variable name to condition values.
// std::string -> std::vector<std::string>
// values inside std::vector are basically an OR.
// TODO(babman): values of strings can be slices.
class ValueMapper : public AbstractVisitor<void> {
 public:
  explicit ValueMapper(const dataflow::SchemaRef &schema)
      : AbstractVisitor(), schema_(schema) {}

  // Get the value(s) of a column.
  bool HasBefore(size_t col_idx) const {
    return this->before_.count(col_idx) == 1;
  }
  bool HasAfter(size_t col_idx) const {
    return this->after_.count(col_idx) == 1;
  }
  const std::vector<std::string> &Before(size_t col_idx) const {
    return this->before_.at(col_idx);
  }
  const std::string &After(size_t col_idx) const {
    return this->after_.at(col_idx);
  }

  // Remove conditions (if they have been consumed).
  void RemoveBefore(size_t col_idx) { this->before_.erase(col_idx); }
  void RemoveAfter(size_t col_idx) { this->after_.erase(col_idx); }

  // For testing.
  void AddBefore(size_t col_idx, const std::string &val) {
    this->before_[col_idx].push_back(val);
  }
  void SetAfter(size_t col_idx, const std::string &val) {
    this->after_[col_idx] = val;
  }

  // Visitors.
  // These are unsupported.
  void VisitCreateTable(const sqlast::CreateTable &ast) override;
  void VisitColumnDefinition(const sqlast::ColumnDefinition &ast) override;
  void VisitColumnConstraint(const sqlast::ColumnConstraint &ast) override;
  void VisitCreateIndex(const sqlast::CreateIndex &ast) override;

  // These are important.
  void VisitInsert(const sqlast::Insert &ast) override;
  void VisitUpdate(const sqlast::Update &ast) override;
  void VisitSelect(const sqlast::Select &ast) override;
  void VisitDelete(const sqlast::Delete &ast) override;
  void VisitBinaryExpression(const sqlast::BinaryExpression &ast) override;

  // These will never be invoked.
  void VisitColumnExpression(const sqlast::ColumnExpression &ast) override;
  void VisitLiteralExpression(const sqlast::LiteralExpression &ast) override;
  void VisitLiteralListExpression(
      const sqlast::LiteralListExpression &ast) override;

 private:
  dataflow::SchemaRef schema_;
  std::unordered_map<size_t, std::vector<std::string>> before_;
  std::unordered_map<size_t, std::string> after_;
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_VALUE_MAPPER_H_
