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
  bool HasBefore(size_t col_idx) const {
    return this->before_.count(col_idx) == 1;
  }
  bool HasAfter(size_t col_idx) const {
    return this->after_.count(col_idx) == 1;
  }
  std::vector<std::string> ReleaseBefore(size_t col_idx) {
    auto node = this->before_.extract(col_idx);
    std::vector<std::string> res = std::move(node.mapped());
    return res;
  }
  std::string ReleaseAfter(size_t col_idx) {
    auto node = this->after_.extract(col_idx);
    std::string res = std::move(node.mapped());
    return res;
  }

  // Access before and after.
  std::unordered_map<size_t, std::vector<std::string>> &Before() {
    return this->before_;
  }
  std::unordered_map<size_t, std::string> &After() { return this->after_; }
  const std::unordered_map<size_t, std::vector<std::string>> &Before() const {
    return this->before_;
  }
  const std::unordered_map<size_t, std::string> &After() const {
    return this->after_;
  }

  // Empty?
  bool EmptyBefore() const { return this->before_.size() == 0; }
  bool EmptyAfter() const { return this->after_.size() == 0; }

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
