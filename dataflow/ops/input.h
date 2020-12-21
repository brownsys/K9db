#ifndef PELTON_DATAFLOW_OPS_INPUT_H_
#define PELTON_DATAFLOW_OPS_INPUT_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class InputOperator : public Operator {
 public:
  OperatorType type() const override { return OperatorType::INPUT; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_INPUT_H_
