#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class Operator;
class InputOperator;
class MatViewOperator;

class DataFlowGraph {
 public:
  DataFlowGraph() = default;

  bool AddNode(std::shared_ptr<Operator> op,
               std::vector<std::shared_ptr<Operator>> parents);

  // Special case: input node has no parents.
  bool AddInputNode(std::shared_ptr<InputOperator> op);

  // Special case: node with single parent.
  inline bool AddNode(std::shared_ptr<Operator> op,
                      std::shared_ptr<Operator> parent) {
    return AddNode(op, std::vector<std::shared_ptr<Operator>>{parent});
  }
  // Special case: output operator is added to outputs_.
  bool AddOutputOperator(std::shared_ptr<MatViewOperator> op,
                         std::shared_ptr<Operator> parent);

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

  // Debugging.
  std::string DebugString() const;

  uint64_t SizeInMemory() const;

 private:
  bool AddEdge(std::shared_ptr<Operator> parent,
               std::shared_ptr<Operator> child);
  // Maps input name to associated input operator.
  std::unordered_map<std::string, std::shared_ptr<InputOperator>> inputs_;
  std::vector<std::shared_ptr<MatViewOperator>> outputs_;

  std::unordered_map<NodeIndex, std::shared_ptr<Operator>> nodes_;
  // 0th item of the tuple is source and 1st item is destination
  std::vector<std::pair<NodeIndex, NodeIndex>> edges_;

  inline NodeIndex MintNodeIndex() { return this->nodes_.size(); }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GRAPH_H_
