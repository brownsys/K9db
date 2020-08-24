#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include "absl/container/node_hash_map.h"

#include "dataflow/edge.h"
#include "dataflow/operator.h"

namespace dataflow {

typedef uint32_t EdgeIndex;
typedef uint32_t NodeIndex;

class DataFlowGraph {
 public:
  DataFlowGraph();

  bool AddNode(OperatorType type, std::shared_ptr<Operator> op);
  bool AddEdge(std::shared_ptr<Operator> op1, std::shared_ptr<Operator> op2);
  bool AddEdge(NodeIndex ni1, NodeIndex ni2);

 private:
  absl::node_hash_map<NodeIndex, std::shared_ptr<Operator>> nodes_;
  absl::node_hash_map<EdgeIndex, std::shared_ptr<Edge>> edges_;

  EdgeIndex mint_edge_index() { return edges_.size(); }
  NodeIndex mint_node_index() { return nodes_.size(); }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_GRAPH_H_
