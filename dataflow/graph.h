#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include <unordered_map>

#include "dataflow/operator.h"

namespace dataflow {

typedef uint32_t NodeIndex;

class DataFlowGraph {
 public:
  DataFlowGraph();

  bool AddNode(OperatorType type, std::shared_ptr<Operator> op);

 private:
  std::unordered_map<NodeIndex, std::shared_ptr<Operator>> nodes_;

  NodeIndex mint_node_index() { return nodes_.size(); }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_GRAPH_H_
