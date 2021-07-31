#ifndef PELTON_DATAFLOW_OPS_INPUT_H_
#define PELTON_DATAFLOW_OPS_INPUT_H_

#include <string>
#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class InputOperator : public Operator {
 public:
  InputOperator(const std::string &input_name, const SchemaRef &schema)
      : Operator(Operator::Type::INPUT), input_name_(input_name) {
    this->input_schemas_.push_back(schema);
    this->output_schema_ = schema;
  }

  const std::string &input_name() const { return this->input_name_; }

  std::optional<std::vector<Record>> Process(
      NodeIndex source, const std::vector<Record> &records) override;

 protected:
  void ComputeOutputSchema() override {}

 private:
  std::string input_name_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_INPUT_H_
