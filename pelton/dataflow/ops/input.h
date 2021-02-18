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
  explicit InputOperator(const std::string &input_name)
      : Operator(Operator::Type::INPUT), input_name_(input_name) {}

  const std::string &input_name() const { return this->input_name_; }

  bool ProcessAndForward(NodeIndex source,
                         const std::vector<Record> &records) override;

 protected:
  bool Process(NodeIndex source, const std::vector<Record> &records,
               std::vector<Record> *output) override;

 private:
  std::string input_name_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_INPUT_H_