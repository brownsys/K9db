#ifndef PELTON_DATAFLOW_OPS_UNION_H_
#define PELTON_DATAFLOW_OPS_UNION_H_

#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

class UnionOperator : public Operator {
 public:
  OperatorType type() const override { return OperatorType::UNION; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_UNION_H_
