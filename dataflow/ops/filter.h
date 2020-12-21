#ifndef PELTON_DATAFLOW_OPS_FILTER_H_
#define PELTON_DATAFLOW_OPS_FILTER_H_

#include <vector>

#include "glog/logging.h"

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class FilterOperator : public Operator {
 public:
  enum Ops : unsigned char {
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    Equal,
    NotEqual
  };

  explicit FilterOperator(std::vector<ColumnID>& cids,
                          std::vector<Ops>& comp_ops, Record comp_vals)
      : cids_(cids), ops_(comp_ops), vals_(comp_vals) {
    CHECK_EQ(cids.size(), comp_ops.size());
    CHECK_EQ(comp_ops.size(), comp_vals.schema().num_columns());
  };

  OperatorType type() override { return OperatorType::FILTER; }

  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

 private:
  std::vector<ColumnID> cids_;
  std::vector<Ops> ops_;
  Record vals_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
