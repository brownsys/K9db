#ifndef PELTON_DATAFLOW_OPS_FORWARD_VIEW_H_
#define PELTON_DATAFLOW_OPS_FORWARD_VIEW_H_

#include <memory>
#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class ForwardViewOperator : public Operator {
 public:
  // Cannot copy an operator.
  ForwardViewOperator(const ForwardViewOperator &other) = delete;
  ForwardViewOperator &operator=(const ForwardViewOperator &other) = delete;

  ForwardViewOperator() : Operator(Operator::Type::FORWARD_VIEW) {}

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_FORWARD_VIEW_H_
