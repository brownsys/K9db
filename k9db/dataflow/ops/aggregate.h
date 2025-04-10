#ifndef K9DB_DATAFLOW_OPS_AGGREGATE_H_
#define K9DB_DATAFLOW_OPS_AGGREGATE_H_

#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest_prod.h"
#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/ops/aggregate_enum.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"
#include "k9db/policy/abstract_policy.h"
#include "k9db/sqlast/ast.h"

namespace k9db {
namespace dataflow {

// Data we need to store to compute the aggregate.
// For simple COUNT and SUM this is a single integer value.
// For DISTINCT count, this is a histogram of count per value.
// For AVG, this is three values, the average, sum, and count.
class AbstractAggregateState {
 protected:
  using PolicyPtr = std::unique_ptr<policy::AbstractPolicy>;

  // NOLINTNEXTLINE
  explicit AbstractAggregateState(PolicyPtr &policy) : policy_(policy) {}

 public:
  virtual ~AbstractAggregateState() {}

  virtual bool Depleted() const = 0;
  virtual void Add(const sqlast::Value &nvalue) = 0;
  virtual void Remove(const sqlast::Value &ovalue) = 0;
  virtual sqlast::Value Value() const = 0;
  virtual uint64_t SizeInMemory() const = 0;

  std::unique_ptr<policy::AbstractPolicy> CopyPolicy() const;
  void CombinePolicy(const PolicyPtr &o);
  void SubtractPolicy(const PolicyPtr &o);

 private:
  PolicyPtr &policy_;
};

class AggregateOperator : public Operator {
 public:
  using Function = AggregateFunctionEnum;
  using State =
      absl::flat_hash_map<Key, std::unique_ptr<AbstractAggregateState>>;

  // No default constructor.
  AggregateOperator() = delete;

  // Cannot copy an operator.
  AggregateOperator(const AggregateOperator &other) = delete;
  AggregateOperator &operator=(const AggregateOperator &other) = delete;

  // Constructors declare group columns, aggregate column, and the aggregate
  // function.
  AggregateOperator(std::vector<ColumnID> group_columns,
                    Function aggregate_function, ColumnID aggregate_column)
      : Operator(Operator::Type::AGGREGATE),
        state_(),
        policies_(),
        group_columns_(group_columns),
        aggregate_function_(aggregate_function),
        aggregate_column_index_(aggregate_column),
        aggregate_column_name_("") {}

  AggregateOperator(std::vector<ColumnID> group_columns,
                    Function aggregate_function, ColumnID aggregate_column,
                    const std::string &agg_column_name)
      : Operator(Operator::Type::AGGREGATE),
        state_(),
        policies_(),
        group_columns_(group_columns),
        aggregate_function_(aggregate_function),
        aggregate_column_index_(aggregate_column),
        aggregate_column_name_(agg_column_name) {}

  uint64_t SizeInMemory() const override;

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
  std::list<std::unique_ptr<policy::AbstractPolicy>> policies_;
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
  Record EmitRecord(const Key &key, const sqlast::Value &aggregate,
                    std::unique_ptr<policy::AbstractPolicy> &&policy,
                    bool positive) const;

  // Initial empty data for a new group.
  std::unique_ptr<AbstractAggregateState> InitialData();

  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(AggregateOperatorTest, MultipleGroupColumnsCountPositive);
  FRIEND_TEST(AggregateOperatorTest, MultipleGroupColumnsSumPositive);
  FRIEND_TEST(AggregateOperatorTest, CountPositiveNegative);
  FRIEND_TEST(AggregateOperatorTest, SumPositiveNegative);
  FRIEND_TEST(AggregateOperatorTest, CountPositiveNegativeSingleBatch);
  FRIEND_TEST(AggregateOperatorTest, SumPositiveNegativeSingleBatch);
  FRIEND_TEST(AggregateOperatorTest, OutputSchemaPrimaryKeyTest);
  FRIEND_TEST(AggregateOperatorTest, OutputSchemaCompositeKeyTest);
  FRIEND_TEST(AggregateOperatorTest, SumGoesAwayWithFilter);
  FRIEND_TEST(AggregateOperatorTest, CountGoesAwayOnDelete);
  FRIEND_TEST(AggregateOperatorTest, SimpleAverage);
  FRIEND_TEST(AggregateOperatorTest, DistinctCount);
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_AGGREGATE_H_
