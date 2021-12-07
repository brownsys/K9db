#ifndef PELTON_DATAFLOW_OPS_INPUT_H_
#define PELTON_DATAFLOW_OPS_INPUT_H_

#include <memory>
#include <string>
#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

using ViewName = std::string;

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

  // amrit
  void set_name(const ViewName &name){
    std::size_t pos = name.find("_") + 1;
    this->view_name_ = name.substr(pos);
    // this->view_name_ = name;
    // split
  }

  const std::string &get_flow_name() const { return this->view_name_; }


 protected:
  std::vector<Record> Process(NodeIndex source,
                              std::vector<Record> &&records) override;

  void ComputeOutputSchema() override {}

  std::unique_ptr<Operator> Clone() const override;

 private:
  std::string input_name_;
  ViewName view_name_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_INPUT_H_
