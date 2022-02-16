#ifndef PELTON_DATAFLOW_OPS_AGGREGATE_H_
#define PELTON_DATAFLOW_OPS_AGGREGATE_H_

#include <cstdint>
#include <list>
#include <memory>
#include <string>
#include <tuple>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
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
  using State = GroupedDataT<absl::flat_hash_map<Key, std::list<Value>>,
                             std::list<Value>, std::nullptr_t, Value>;

  AggregateOperator() = delete;
  // Cannot copy an operator.
  AggregateOperator(const AggregateOperator &other) = delete;
  AggregateOperator &operator=(const AggregateOperator &other) = delete;

  AggregateOperator(std::vector<ColumnID> group_columns,
                    Function aggregate_function, ColumnID aggregate_column)
      : Operator(Operator::Type::AGGREGATE),
        state_(),
        group_columns_(group_columns),
        aggregate_function_(aggregate_function),
        aggregate_column_index_(aggregate_column),
        aggregate_column_name_("") {}

  AggregateOperator(std::vector<ColumnID> group_columns,
                    Function aggregate_function, ColumnID aggregate_column,
                    const std::string &agg_column_name)
      : Operator(Operator::Type::AGGREGATE),
        state_(),
        group_columns_(group_columns),
        aggregate_function_(aggregate_function),
        aggregate_column_index_(aggregate_column),
        aggregate_column_name_(agg_column_name) {}

  uint64_t SizeInMemory() const override { return this->state_.SizeInMemory(); }

  const std::vector<ColumnID> &group_columns() const {
    return this->group_columns_;
  }

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;

 private:
  State state_;
  // Group by columns information.
  std::vector<ColumnID> group_columns_;
  // Aggregate column information.
  Function aggregate_function_;
  ColumnID aggregate_column_index_;
  std::string aggregate_column_name_;
  sqlast::ColumnDefinition::Type aggregate_column_type_;

  // Construct an output record with this->output_schema_.
  // The first n-1 columns are assigned values from the given key corresponding
  // to group by columns.
  // The last column is assigned value from the aggregate value.
  Record EmitRecord(const Key &key, const Value &aggregate,
                    bool positive) const;

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
