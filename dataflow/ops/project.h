#ifndef PELTON_DATAFLOW_OPS_PROJECT_H_
#define PELTON_DATAFLOW_OPS_PROJECT_H_

#include <vector>

#include "glog/logging.h"

#include "dataflow/operator.h"
#include "dataflow/record.h"

namespace dataflow {

class ProjectOperator : public Operator {
 public:
  explicit ProjectOperator(std::vector<ColumnID>& cids, const Schema& schema)
      : cids_(cids), in_schema_(&schema), out_schema_(Schema(schema.ColumnSubset(cids))) {};

  OperatorType type() override { return OperatorType::PROJECT; }

  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

  const Schema& schema(){return out_schema_;}

 private:
  std::vector<ColumnID> cids_;
  const Schema* in_schema_;
  // this operator owns this schema
  Schema out_schema_;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPS_PROJECT_H_
