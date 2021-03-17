#ifndef PELTON_DATAFLOW_OPS_AGGREGATE_H_
#define PELTON_DATAFLOW_OPS_AGGREGATE_H_

#include <cstdint>
#include <string>
#include <tuple>
// NOLINTNEXTLINE
#include <variant>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "gtest/gtest_prod.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class AggregateOperator : public Operator {
 public:
  enum class Function : unsigned char { COUNT, SUM };
  explicit AggregateOperator(std::vector<ColumnID> group_columns,
                             Function aggregate_function,
                             ColumnID aggregate_column)
      : Operator(Operator::Type::AGGREGATE),
        group_columns_(group_columns),
        aggregate_function_(aggregate_function),
        aggregate_column_(aggregate_column),
        owned_output_schema_(),
        aggregate_schema_() {}

  ~AggregateOperator() {
    // ensure that schemas are not destructed first
    state_.~flat_hash_map();
  }

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;
  void ComputeOutputSchema() override;

 private:
  absl::flat_hash_map<std::vector<Key>, Record> state_;
  std::vector<ColumnID> group_columns_;
  Function aggregate_function_;
  ColumnID aggregate_column_;
  // Aggregate operator owns the following schemas
  SchemaOwner owned_output_schema_;
  // agg_schema_ will contain a single column
  SchemaOwner aggregate_schema_;
  SchemaRef aggregate_schema_ref_;

  // equivalent of doing a logical project on record
  std::vector<Key> GenerateKey(const Record &record) const;
  // Combine key from state with computed aggregate and emit record. Used for
  // emitting both positive and negative records
  void EmitRecord(const std::vector<Key> &key, const Record &aggregate_record,
                  bool positive, std::vector<Record> *output) const;

  // A helper function to improve code readability inside the process function
  inline void InitAggregateValue(Record &aggregate_record,
                                 const Record &from_record,
                                 ColumnID from_column) const;
  // Allow tests to use .Process(...) directly.
  FRIEND_TEST(AggregateOperatorTest, MultipleGroupColumnsCountPositive);
  FRIEND_TEST(AggregateOperatorTest, MultipleGroupColumnsSumPositive);
  FRIEND_TEST(AggregateOperatorTest, CountPositiveNegative);
  FRIEND_TEST(AggregateOperatorTest, SumPositiveNegative);
  FRIEND_TEST(AggregateOperatorTest, CountPositiveNegativeSingleBatch);
  FRIEND_TEST(AggregateOperatorTest, SumPositiveNegativeSingleBatch);
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_AGGREGATE_H_
