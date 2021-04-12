#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/edge.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class DataFlowGraph {
 public:
  DataFlowGraph() = default;

  bool AddNode(std::shared_ptr<Operator> op,
               std::vector<std::shared_ptr<Operator>> parents);

  // Special case: input node has no parents.
  inline bool AddInputNode(std::shared_ptr<InputOperator> op) {
    CHECK(this->inputs_.count(op->input_name()) == 0)
        << "An operator for this input already exists";
    this->inputs_.emplace(op->input_name(), op);
    return AddNode(op, std::vector<std::shared_ptr<Operator>>{});
  }
  // To be used for initiating the operator, parents should be added
  // subsequently
  inline bool AddNode(std::shared_ptr<Operator> op) {
    return AddNode(op, std::vector<std::shared_ptr<Operator>>{});
  }
  // Special case: node with single parent.
  inline bool AddNode(std::shared_ptr<Operator> op,
                      std::shared_ptr<Operator> parent) {
    return AddNode(op, std::vector<std::shared_ptr<Operator>>{parent});
  }
  // Special case: output operator is added to outputs_.
  inline bool AddOutputOperator(std::shared_ptr<MatViewOperator> op,
                                std::shared_ptr<Operator> parent) {
    this->outputs_.emplace_back(op);
    return AddNode(op, parent);
  }

  bool Process(const std::string &input_name,
               const std::vector<Record> &records);

  // Accessors.
  const std::unordered_map<std::string, std::shared_ptr<InputOperator>>
      &inputs() const {
    return this->inputs_;
  }
  const std::vector<std::shared_ptr<MatViewOperator>> &outputs() const {
    return this->outputs_;
  }

  // Get node by its index.
  inline std::shared_ptr<Operator> GetNode(NodeIndex node_index) const {
    auto it = nodes_.find(node_index);
    return it == nodes_.end() ? nullptr : it->second;
  }

 private:
  bool AddEdge(std::shared_ptr<Operator> parent,
               std::shared_ptr<Operator> child);
  // Maps input name to associated input operator.
  std::unordered_map<std::string, std::shared_ptr<InputOperator>> inputs_;
  std::vector<std::shared_ptr<MatViewOperator>> outputs_;

  std::unordered_map<NodeIndex, std::shared_ptr<Operator>> nodes_;
  std::unordered_map<EdgeIndex, std::shared_ptr<Edge>> edges_;

  inline EdgeIndex MintEdgeIndex() { return this->edges_.size(); }
  inline NodeIndex MintNodeIndex() { return this->nodes_.size(); }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GRAPH_H_
