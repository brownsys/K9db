#ifndef PELTON_SQLAST_VALUE_MAPPER_H_
#define PELTON_SQLAST_VALUE_MAPPER_H_

#include <string>
#include <unordered_map>
#include <utility>
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
  bool HasValues(size_t col_idx) const {
    return this->values_.count(col_idx) == 1;
  }
  std::vector<Value> ReleaseValues(size_t col_idx) {
    auto node = this->values_.extract(col_idx);
    std::vector<Value> res = std::move(node.mapped());
    return res;
  }
  const std::unordered_map<size_t, std::vector<Value>> &Values() const {
    return this->values_;
  }
  bool Empty() const { return this->values_.size() == 0; }

  // Visitors.
  // These are unsupported.
  void VisitCreateTable(const CreateTable &ast) override;
  void VisitColumnDefinition(const ColumnDefinition &ast) override;
  void VisitColumnConstraint(const ColumnConstraint &ast) override;
  void VisitAnonymizationRule(const AnonymizationRule &ast) override;
  void VisitCreateIndex(const CreateIndex &ast) override;
  void VisitCreateView(const CreateView &ast) override;

  void VisitInsert(const Insert &ast) override;
  void VisitReplace(const Replace &ast) override;
  void VisitGDPRStatement(const GDPRStatement &ast) override;

  // These are important.
  void VisitSelect(const Select &ast) override;
  void VisitUpdate(const Update &ast) override;
  void VisitDelete(const Delete &ast) override;
  void VisitBinaryExpression(const BinaryExpression &ast) override;

  // These will never be invoked.
  void VisitColumnExpression(const ColumnExpression &ast) override;
  void VisitLiteralExpression(const LiteralExpression &ast) override;
  void VisitLiteralListExpression(const LiteralListExpression &ast) override;

 private:
  dataflow::SchemaRef schema_;
  std::unordered_map<size_t, std::vector<Value>> values_;

 public:
#ifdef PELTON_FILTER_UNITTEST
  // To allow unittests to add values for testing.
  void AddValue(size_t i, const Value &value) {
    this->values_[i].push_back(value);
  }
#endif  // PELTON_FILTER_UNITTEST
};

}  // namespace sqlast
}  // namespace pelton

#endif  // PELTON_SQLAST_VALUE_MAPPER_H_
