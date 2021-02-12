#ifndef PELTON_DATAFLOW_OPS_INPUT_H_
#define PELTON_DATAFLOW_OPS_INPUT_H_

#include <string>
#include <vector>

#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

class InputOperator : public Operator {
 public:
  explicit InputOperator(const std::string& table_name)
      : Operator(), table_name_(table_name) {}

  const std::string& table_name() const { return this->table_name_; }
  OperatorType type() const override { return OperatorType::INPUT; }
  bool process(std::vector<Record>& rs, std::vector<Record>& out_rs) override;

 private:
  std::string table_name_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_INPUT_H_
