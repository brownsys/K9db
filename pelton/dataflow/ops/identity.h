#ifndef PELTON_DATAFLOW_OPS_IDENTITY_H_
#define PELTON_DATAFLOW_OPS_IDENTITY_H_

#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

class IdentityOperator : public Operator {
  OperatorType type() const override { return OperatorType::IDENTITY; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
