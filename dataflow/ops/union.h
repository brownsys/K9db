#ifndef PELTON_DATAFLOW_OPS_UNION_H_
#define PELTON_DATAFLOW_OPS_UNION_H_

#include <vector>

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class UnionOperator : public Operator {
 public:
  explicit UnionOperator(const Schema& schema) : schema_(&schema) {}
  OperatorType type() override { return OperatorType::UNION; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;
  const Schema& schema() { return *schema_; }

 private:
  const Schema* schema_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_UNION_H_
