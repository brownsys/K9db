#ifndef PELTON_DATAFLOW_OPERATOR_H_
#define PELTON_DATAFLOW_OPERATOR_H_

enum OperatorType {
  IDENTITY,
};

class Operator {
 public:
  virtual ~Operator(){};
  virtual bool process() = 0;
};

#endif  // PELTON_DATAFLOW_OPERATOR_H_
