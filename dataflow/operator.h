#ifndef PELTON_DATAFLOW_OPERATOR_H_
#define PELTON_DATAFLOW_OPERATOR_H_

class Operator {
  virtual bool process() = 0;
};

#endif  // PELTON_DATAFLOW_OPERATOR_H_
