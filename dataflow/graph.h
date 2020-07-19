#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <unordered_map>

#include "dataflow/operator.h"

typedef uint32_t NodeIndex;

class DataFlowGraph {
 public:
  DataFlowGraph();

  bool AddNode(OperatorType type, Operator* op);

 private:
  std::unordered_map<NodeIndex, Operator*> nodes_;

  NodeIndex mint_node_index() { return nodes_.size(); }
};

#endif  // PELTON_DATAFLOW_GRAPH_H_
