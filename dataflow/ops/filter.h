#ifndef PELTON_DATAFLOW_OPS_FILTER_H_
#define PELTON_DATAFLOW_OPS_FILTER_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class FilterOperator : public Operator {
 public:
  enum Ops : unsigned char { OpsLT, OpsLT_Eq, OpsGT, OpsGT_Eq, OpsEq, OpsN_Eq };

  explicit FilterOperator(std::vector<ColumnID> cids, std::vector<Ops> comp_ops,
                          std::vector<RecordData> comp_vals) {
    cids = cid_;
    comp_ops = op_;
    val_ = comp_vals;
  };
  OperatorType type() override { return OperatorType::FILTER; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

 private:
  std::vector<ColumnID> cid_;
  std::vector<Ops> op_;
  std::vector<RecordData> val_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
