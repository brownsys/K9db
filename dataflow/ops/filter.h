#ifndef PELTON_DATAFLOW_OPS_FILTER_H_
#define PELTON_DATAFLOW_OPS_FILTER_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class FilterOperator : public Operator {
public:
  explicit FilterOperator(ColumnID cid, std::string comp_op, RecordData comp_val){
    cid = cid_; comp_op = op_; val_ = comp_val;
  };
  OperatorType type() override { return OperatorType::FILTER; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

private:
  ColumnID cid_;
  std::string op_;
  RecordData val_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
