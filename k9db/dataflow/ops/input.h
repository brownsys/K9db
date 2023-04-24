#ifndef K9DB_DATAFLOW_OPS_INPUT_H_
#define K9DB_DATAFLOW_OPS_INPUT_H_

#include <memory>
#include <string>
#include <vector>

#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/types.h"

namespace k9db {
namespace dataflow {

class InputOperator : public Operator {
 public:
  // Cannot copy an operator.
  InputOperator(const InputOperator &other) = delete;
  InputOperator &operator=(const InputOperator &other) = delete;

  InputOperator(const std::string &input_name, const SchemaRef &schema)
      : Operator(Operator::Type::INPUT), input_name_(input_name) {
    this->input_schemas_.push_back(schema);
    this->output_schema_ = schema;
  }

  const std::string &input_name() const { return this->input_name_; }

 protected:
  std::vector<Record> Process(NodeIndex source, std::vector<Record> &&records,
                              const Promise &promise) override;

  void ComputeOutputSchema() override {}

  std::unique_ptr<Operator> Clone() const override;

 private:
  std::string input_name_;
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_OPS_INPUT_H_
