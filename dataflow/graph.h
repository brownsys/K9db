#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include "absl/container/node_hash_map.h"

#include "dataflow/edge.h"
#include "dataflow/operator.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/types.h"

namespace dataflow {

class DataFlowGraph {
 public:
  DataFlowGraph();

  bool AddInputNode(std::shared_ptr<InputOperator> op);
  bool AddNode(std::shared_ptr<Operator> op, std::vector<std::shared_ptr<Operator>> parents);
  inline bool AddNode(std::shared_ptr<Operator> op,
               std::shared_ptr<Operator> parent) {
      return AddNode(op, std::vector<std::shared_ptr<Operator>>{parent});
  }
  bool AddEdge(std::shared_ptr<Operator> op1, std::shared_ptr<Operator> op2);
  bool Process(InputOperator& input, std::vector<Record> records);

  std::vector<std::shared_ptr<InputOperator>> inputs() const;
  std::vector<std::shared_ptr<MatViewOperator>> outputs() const;

  inline std::shared_ptr<Operator> GetNode(NodeIndex node_index) const {
    auto it = nodes_.find(node_index);
    return it == nodes_.end() ? nullptr : it->second;
  }

 private:
  absl::node_hash_map<NodeIndex, std::shared_ptr<Operator>> nodes_;
  absl::node_hash_map<EdgeIndex, std::shared_ptr<Edge>> edges_;

  EdgeIndex mint_edge_index() { return edges_.size(); }
  NodeIndex mint_node_index() { return nodes_.size(); }
};

}  // namespace dataflow

#endif  // PELTON_DATAFLOW_GRAPH_H_
