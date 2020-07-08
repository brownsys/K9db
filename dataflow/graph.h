#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <vector>

#include "dataflow/operator.h"

class DataFlowGraph {
  public:
    DataFlowGraph();
  private:
    std::vector<Operator> nodes_;
};

#endif  // PELTON_DATAFLOW_GRAPH_H_
