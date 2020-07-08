#ifndef PELTON_DATAFLOW_OPS_IDENTITY_H_
#define PELTON_DATAFLOW_OPS_IDENTITY_H_

#include "dataflow/operator.h"

class IdentityOperator : Operator {
  bool process();
};

#endif  // PELTON_DATAFLOW_OPS_IDENTITY_H_
