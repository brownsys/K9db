#ifndef PELTON_DATAFLOW_OPS_IDENTITY_H_
#define PELTON_DATAFLOW_OPS_IDENTITY_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class IdentityOperator : public Operator {
 public:
  OperatorType type() override { return OperatorType::IDENTITY; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
