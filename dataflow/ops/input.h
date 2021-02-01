#ifndef PELTON_DATAFLOW_OPS_INPUT_H_
#define PELTON_DATAFLOW_OPS_INPUT_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class InputOperator : public Operator {
 public:
  explicit InputOperator(const Schema& schema) : schema_(&schema){}
  OperatorType type() override { return OperatorType::INPUT; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;
  const Schema& schema(){return *schema_;}

private:
  const Schema* schema_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_INPUT_H_
