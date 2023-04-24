#ifndef K9DB_DATAFLOW_OPS_FILTER_H_
#define K9DB_DATAFLOW_OPS_FILTER_H_

#include <cstdint>
#include <memory>
#include <string>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "gtest/gtest_prod.h"
#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/ops/filter_enum.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/types.h"
#include "k9db/sqlast/ast.h"

namespace k9db {
namespace dataflow {

class FilterOperator : public Operator {
 public:
  // The operations we can use to filter (on a value in a record).
  using Operation = FilterOperationEnum;
  // Cannot copy an operator.
  FilterOperator(const FilterOperator &other) = delete;
  FilterOperator &operator=(const FilterOperator &other) = delete;

  // Construct the filter operator by first providing the input schema.
  FilterOperator() : Operator(Operator::Type::FILTER) {}

  // Add filter conditions/operations.
  void AddNullOperation(ColumnID column, Operation op) {
    this->ops_.emplace_back(column, op);
  }
  void AddLiteralOperation(ColumnID column, uint64_t value, Operation op) {
    this->ops_.emplace_back(column, sqlast::Value(value), op);
  }
  void AddLiteralOperation(ColumnID column, int64_t value, Operation op) {
    // Everything gets parsed in the ast and calcite as a *signed* int.
    // However, we may be comparing against an unsigned column.
    // We change the value to unsigned in that case.
    if (this->input_schemas_.at(0).TypeOf(column) ==
        sqlast::ColumnDefinition::Type::UINT) {
      CHECK_GE(value, 0) << "Cannot implicitly convert int to uint in filter";
      uint64_t cast = static_cast<uint64_t>(value);
      this->ops_.emplace_back(column, sqlast::Value(cast), op);
    } else {
      this->ops_.emplace_back(column, sqlast::Value(value), op);
    }
  }
  void AddLiteralOperation(ColumnID column, const std::string &value,
                           Operation op) {
    this->ops_.emplace_back(column, sqlast::Value(value), op);
  }
  void AddColumnOperation(ColumnID left, ColumnID right, Operation op) {
    this->ops_.emplace_back(left, right, op);
  }

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;

 private:
  bool Accept(const Record &record) const;

  class FilterOperation {
   public:
    // Use this for filter column with null.
    FilterOperation(ColumnID l, Operation op)
        : left_(l), right_(), op_(op), col_(false) {
      CHECK(op == Operation::IS_NULL || op == Operation::IS_NOT_NULL);
    }

    // Use this for filter column with literal (excluding null).
    FilterOperation(ColumnID l, sqlast::Value &&r, Operation op)
        : left_(l), right_(r), op_(op), col_(false) {}

    // Use this for filter column with another column.
    FilterOperation(ColumnID l, ColumnID r, Operation op)
        : left_(l), right_(static_cast<uint64_t>(r)), op_(op), col_(true) {}

    // Accessors.
    ColumnID left() const { return this->left_; }
    ColumnID right_column() const {
      return static_cast<ColumnID>(this->right_->GetUInt());
    }
    const sqlast::Value &right_value() const { return *this->right_; }
    Operation op() const { return this->op_; }
    bool is_column() const { return this->col_; }

   private:
    // Without loss of generality, column always on the left (planner inverts
    // the operator when column appears on the right).
    ColumnID left_;
    // The right operand, might be a literal (including null), or another
    // column.
    std::optional<sqlast::Value> right_;
    // Filter operation (e.g. <, ==, etc).
    Operation op_;
    // Whether the right operand is a column or not.
    bool col_;
  };

  std::vector<FilterOperation> ops_;

  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(FilterOperatorTest, SingleAccept);
  FRIEND_TEST(FilterOperatorTest, AndAccept);
  FRIEND_TEST(FilterOperatorTest, SeveralOpsPerColumn);
  FRIEND_TEST(FilterOperatorTest, BatchTest);
  FRIEND_TEST(FilterOperatorTest, ColOps);
  FRIEND_TEST(FilterOperatorTest, TypeMistmatch);
  FRIEND_TEST(FilterOperatorTest, ImplicitTypeConversion);
  FRIEND_TEST(FilterOperatorTest, IsNullAccept);
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_FILTER_H_
