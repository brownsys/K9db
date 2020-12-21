#ifndef PELTON_DATAFLOW_OPS_UNION_H_
#define PELTON_DATAFLOW_OPS_UNION_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class UnionOperator : public Operator {
 public:
  OperatorType type() override { return OperatorType::UNION; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_UNION_H_