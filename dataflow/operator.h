#ifndef PELTON_DATAFLOW_OPERATOR_H_
#define PELTON_DATAFLOW_OPERATOR_H_

#include "dataflow/record.h"

namespace dataflow {

enum OperatorType {
  INPUT,
  IDENTITY,
  MAT_VIEW,
  FILTER,
};

class Operator {
 public:
  virtual ~Operator(){};
  virtual OperatorType type() = 0;
  virtual bool process(std::vector<Record>& rs,
                       std::vector<Record>& out_rs) = 0;
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_OPERATOR_H_
