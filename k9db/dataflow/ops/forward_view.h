#ifndef K9DB_DATAFLOW_OPS_FORWARD_VIEW_H_
#define K9DB_DATAFLOW_OPS_FORWARD_VIEW_H_

#include <memory>
#include <string>
#include <vector>

#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/types.h"

namespace k9db {
namespace dataflow {

class ForwardViewOperator : public Operator {
 public:
  // Cannot copy an operator.
  ForwardViewOperator(const ForwardViewOperator &other) = delete;
  ForwardViewOperator &operator=(const ForwardViewOperator &other) = delete;

  ForwardViewOperator(SchemaRef schema, const std::string &parent_flow,
                      NodeIndex parent_id)
      : Operator(Operator::Type::FORWARD_VIEW),
        parent_flow_(parent_flow),
        parent_id_(parent_id) {
    // this->input_schemas_.push_back(schema);
    this->output_schema_ = schema;
  }

  NodeIndex parent_id() const { return this->parent_id_; }
  const std::string &parent_flow() const { return this->parent_flow_; }

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override;

  std::unique_ptr<Operator> Clone() const override;

 private:
  std::string parent_flow_;
  NodeIndex parent_id_;
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_FORWARD_VIEW_H_
