#ifndef PELTON_DATAFLOW_OPS_AGGREGATE_H_
#define PELTON_DATAFLOW_OPS_AGGREGATE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "gtest/gtest_prod.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/aggregate_enum.h"
#include "pelton/dataflow/ops/grouped_data.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class AggregateOperator : public Operator {
 public:
  using Function = AggregateFunctionEnum;
  AggregateOperator() = delete;
  // Cannot copy an operator.
  AggregateOperator(const AggregateOperator &other) = delete;
  AggregateOperator &operator=(const AggregateOperator &other) = delete;

  AggregateOperator(std::vector<ColumnID> group_columns,
                    Function aggregate_function, ColumnID aggregate_column)
      : Operator(Operator::Type::AGGREGATE),
        group_columns_(group_columns),
        aggregate_function_(aggregate_function),
        aggregate_column_(aggregate_column),
        aggregate_schema_() {}

  std::shared_ptr<Operator> Clone() const override;
  std::vector<ColumnID> OutPartitionCols() const;
  // Accessors
  const std::vector<ColumnID> &group_columns() const {
    return this->group_columns_;
  }

  ~AggregateOperator() {
    // Ensure that schemas are not destructed first
    state_.~GroupedDataT();
  }

 protected:
  std::optional<std::vector<Record>> Process(
      NodeIndex /*source*/, const std::vector<Record> &records) override;
  void ComputeOutputSchema() override;

  uint64_t SizeInMemory() const override { return this->state_.SizeInMemory(); }

 private:
  UnorderedGroupedData state_;
  std::vector<ColumnID> group_columns_;
  Function aggregate_function_;
  ColumnID aggregate_column_;
  // This contains a single column (the one resulting from the aggregate).
  SchemaRef aggregate_schema_;

  // Equivalent of doing a logical project on record
  // std::vector<Key> GenerateKey(const Record &record) const;
  // Combine key from state with computed aggregate and emit record. Used for
  // emitting both positive and negative records
  void EmitRecord(const Key &key, const Record &aggregate_record, bool positive,
                  std::vector<Record> *output) const;

  // A helper function to improve code readability inside the process function
  inline void InitAggregateValue(Record *aggregate_record,
                                 const Record &from_record,
                                 ColumnID from_column) const;

  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(AggregateOperatorTest, MultipleGroupColumnsCountPositive);
  FRIEND_TEST(AggregateOperatorTest, MultipleGroupColumnsSumPositive);
  FRIEND_TEST(AggregateOperatorTest, CountPositiveNegative);
  FRIEND_TEST(AggregateOperatorTest, SumPositiveNegative);
  FRIEND_TEST(AggregateOperatorTest, CountPositiveNegativeSingleBatch);
  FRIEND_TEST(AggregateOperatorTest, SumPositiveNegativeSingleBatch);
  FRIEND_TEST(AggregateOperatorTest, OutputSchemaPrimaryKeyTest);
  FRIEND_TEST(AggregateOperatorTest, OutputSchemaCompositeKeyTest);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_AGGREGATE_H_
