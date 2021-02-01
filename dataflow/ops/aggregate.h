#ifndef PELTON_DATAFLOW_OPS_AGGREGATE_H_
#define PELTON_DATAFLOW_OPS_AGGREGATE_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class AggregateOperator : public Operator {
 public:
  enum Func : unsigned char { FuncCount, FuncSum };

  OperatorType type() override { return OperatorType::AGGREGATE; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

  explicit AggregateOperator(std::vector<ColumnID> group_cols, Func agg_func,
                             ColumnID agg_col, const Schema& schema,
                             std::vector<ColumnID> out_cols)
      : group_cols_(group_cols),
        agg_func_(agg_func),
        agg_col_(agg_col),
        out_schema_(Schema(schema.ColumnSubset(out_cols))),
        agg_schema_(
            Schema(schema.ColumnSubset(std::vector<ColumnID>({agg_col})))) {}

  const Schema& schema() { return out_schema_; }

  ~AggregateOperator() {
    // ensure that schemas are not distructed first
    state_.~flat_hash_map();
  }

 private:
  absl::flat_hash_map<std::vector<Key>, Record> state_;
  std::vector<ColumnID> group_cols_;
  Func agg_func_;
  ColumnID agg_col_;
  // schema must outlive records
  // Aggregate operator owns the following schemas
  Schema out_schema_;
  // agg_schema_ will contain a single column
  Schema agg_schema_;

  std::vector<Key> get_key(const Record& r) {
    std::vector<Key> key;
    for (auto i : group_cols_) {
      DataType type = r.schema().TypeOf(i);
      switch (type) {
        case DataType::kUInt:
          key.push_back(Key(r.as_uint(i), DataType::kUInt));
          break;
        case DataType::kInt:
          key.push_back(Key(r.as_int(i), DataType::kInt));
          break;
        case DataType::kText:
          key.push_back(Key(*(r.as_string(i))));
          break;
        default:
          // TODO(Ishan): Handle datetime
          LOG(FATAL) << "Unimplemented type";
      }
    }
    return key;
  }

  Record gen_out_record(const std::vector<Key>& key, Record agg_r,
                        bool positive) {
    // generate record
    Record r(out_schema_, positive);
    for (size_t i = 0; i < key.size(); i++) {
      switch (out_schema_.TypeOf(i)) {
        case DataType::kUInt:
          r.set_uint(i, key.at(i).as_uint());
          break;
        case DataType::kInt:
          r.set_int(i, key.at(i).as_int());
          break;
        case DataType::kText:
          // records have to own their pointed data
          r.set_string(i, new std::string(key.at(i).as_string()));
          break;
        default:
          // TODO(Ishan): handle datetime
          LOG(FATAL) << "Unimplemented type";
      }
    }

    // Add aggregate value to the last column
    size_t agg_index = out_schema_.num_columns() - 1;
    switch (out_schema_.TypeOf(agg_index)) {
      case DataType::kUInt:
        r.set_uint(agg_index, agg_r.as_uint(0));
        break;
      case DataType::kInt:
        r.set_int(agg_index, agg_r.as_int(0));
        break;
      case DataType::kText:
        // records have to own their pointed data
        r.set_string(agg_index, new std::string(*(agg_r.as_string(0))));
        break;
      default:
        // TODO(Ishan): handle datetime
        LOG(FATAL) << "Unimplemented type";
    }
    return r;
  }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_AGGREGATE_H_
