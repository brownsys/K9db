#ifndef PELTON_DATAFLOW_OPS_FILTER_H_
#define PELTON_DATAFLOW_OPS_FILTER_H_

#include <cstdint>
#include <string>
#include <tuple>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/filter_enum.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/dataflow/value.h"

namespace pelton {
namespace dataflow {

class FilterOperator : public Operator {
 public:
  // The operations we can use to filter (on a value in a record).
  using Operation = FilterOperationEnum;

  // Construct the filter operator by first providing the input schema.
  FilterOperator() : Operator(Operator::Type::FILTER) {}

  // Add filter conditions/operations.
  void AddOperation(const std::string &value, ColumnID column, Operation op) {
    this->ops_.emplace_back(column, value, op);
  }
  void AddOperation(uint64_t value, ColumnID column, Operation op) {
    this->ops_.emplace_back(column, value, op);
  }
  void AddOperation(int64_t value, ColumnID column, Operation op) {
    if (this->input_schemas_.size() > 0 &&
        this->input_schemas_.at(0).TypeOf(column) ==
            sqlast::ColumnDefinition::Type::UINT) {
      CHECK_GE(value, 0);
      this->AddOperation(static_cast<uint64_t>(value), column, op);
    } else {
      this->ops_.emplace_back(column, value, op);
    }
  }
  void AddOperation(ColumnID column, Operation op) {
    this->ops_.emplace_back(column, op);
  }
  void AddOperation(ColumnID left_column, Operation op, ColumnID right_column) {
    this->ops_.emplace_back(left_column, op, right_column);
  }

  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;

 protected:
  bool Accept(const Record &record) const;

  void ComputeOutputSchema() override;

 private:
  class FilterOperation {
   public:
    // Use this for unary operation with null.
    FilterOperation(ColumnID col, Operation op)
        : data_(std::make_tuple(col, NullValue(), op, false)) {
      CHECK(op == Operation::IS_NULL || op == Operation::IS_NOT_NULL);
    }

    // Use this for filter column with literal (excluding null).
    FilterOperation(ColumnID l, Record::DataVariant r, Operation op)
        : data_(std::make_tuple(l, r, op, false)) {}

    // Use this for filter column with another column.
    FilterOperation(ColumnID l, Operation op, ColumnID r)
        : data_(std::make_tuple(l, static_cast<uint64_t>(r), op, true)) {}

    // Accessors.
    ColumnID left() const { return std::get<0>(this->data_); }
    ColumnID right_column() const {
      uint64_t col_id = std::get<uint64_t>(std::get<1>(this->data_));
      return static_cast<ColumnID>(col_id);
    }
    const Record::DataVariant &right() const {
      return std::get<1>(this->data_);
    }
    Operation op() const { return std::get<2>(this->data_); }
    bool is_column() const { return std::get<3>(this->data_); }

   private:
    // [
    //   LeftExpression (always a column),
    //   RightExpression (May be column id, or literal including null),
    //   Operation (enum),
    //   whether right expression is a column or not (bool)
    // ]
    std::tuple<ColumnID, Record::DataVariant, Operation, bool> data_;
  };

  std::vector<FilterOperation> ops_;

  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(FilterOperatorTest, SingleAccept);
  FRIEND_TEST(FilterOperatorTest, AndAccept);
  FRIEND_TEST(FilterOperatorTest, SeveralOpsPerColumn);
  FRIEND_TEST(FilterOperatorTest, BatchTest);
  FRIEND_TEST(FilterOperatorTest, TypeMistmatch);
  FRIEND_TEST(FilterOperatorTest, IsNullAccept);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_FILTER_H_
