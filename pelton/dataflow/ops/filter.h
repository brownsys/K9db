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
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class FilterOperator : public Operator {
 public:
  // The operations we can use to filter (on a value in a record).
  enum class Operation : unsigned char {
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    EQUAL,
    NOT_EQUAL
  };

  // Construct the filter operator by first providing the input schema.
  FilterOperator() : Operator(Operator::Type::FILTER) {}

  // Add filter conditions/operations.
  void AddOperation(const std::string &value, ColumnID column, Operation op) {
    this->ops_.push_back(std::make_tuple(value, column, op));
  }
  void AddOperation(uint64_t value, ColumnID column, Operation op) {
    this->ops_.push_back(std::make_tuple(value, column, op));
  }
  void AddOperation(int64_t value, ColumnID column, Operation op) {
    this->ops_.push_back(std::make_tuple(value, column, op));
  }

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;

  bool Accept(const Record &record) const;

  void ComputeOutputSchema() override;

 private:
  std::vector<std::tuple<Record::DataVariant, ColumnID, Operation>> ops_;
  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(FilterOperatorTest, SingleAccept);
  FRIEND_TEST(FilterOperatorTest, AndAccept);
  FRIEND_TEST(FilterOperatorTest, SeveralOpsPerColumn);
  FRIEND_TEST(FilterOperatorTest, BatchTest);
  FRIEND_TEST(FilterOperatorTest, TypeMistmatch);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_FILTER_H_
