#ifndef PELTON_DATAFLOW_OPS_AGGREGATE_H_
#define PELTON_DATAFLOW_OPS_AGGREGATE_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class AggregateOperator : public Operator {
 public:
  enum Func : unsigned char { FuncMax, FuncMin, FuncSum, FuncCount };

  OperatorType type() override { return OperatorType::AGGREGATE; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

  explicit AggregateOperator(std::vector<ColumnID> group_cols, Func agg_func,
                             ColumnID agg_col)
      : group_cols_(group_cols), agg_func_(agg_func), agg_col_(agg_col) {}

 private:
  absl::flat_hash_map<std::vector<RecordData>, uint64_t> state_;
  std::vector<ColumnID> group_cols_;
  Func agg_func_;
  ColumnID agg_col_;

  std::vector<RecordData> get_key(const Record& r) {
    std::vector<RecordData> key;
    for (auto i : group_cols_) {
      key.push_back(r.raw_at(i));
    }
    return key;
  }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_AGGREGATE_H_
