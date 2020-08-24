#ifndef PELTON_DATAFLOW_OPS_INPUT_H_
#define PELTON_DATAFLOW_OPS_INPUT_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class InputOperator : public Operator {
  bool process(std::vector<Record>& rs) override;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_INPUT_H_
