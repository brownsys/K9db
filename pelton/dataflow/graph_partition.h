#ifndef PELTON_DATAFLOW_GRAPH_PARTITION_H_
#define PELTON_DATAFLOW_GRAPH_PARTITION_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/types.h"

namespace pelton {
namespace dataflow {

class Operator;
class InputOperator;
class MatViewOperator;

class DataFlowGraphPartition {
 public:
  DataFlowGraphPartition() = default;

  bool AddNode(std::unique_ptr<Operator> &&op,
               const std::vector<Operator *> &parents);

  // Special case: input node has no parents.
  bool AddInputNode(std::unique_ptr<InputOperator> &&op);

  // Special case: node with single parent.
  inline bool AddNode(std::unique_ptr<Operator> &&op, Operator *parent) {
    return AddNode(std::move(op), std::vector<Operator *>{parent});
  }
  // Special case: output operator is added to outputs_.
  bool AddOutputOperator(std::unique_ptr<MatViewOperator> &&op,
                         Operator *parent);

  NodeIndex LastOperatorIndex() const { return this->nodes_.size() - 1; }

  void Process(const std::string &input_name, std::vector<Record> &&records);

  // Accessors.
  const std::unordered_map<std::string, InputOperator *> &inputs() const {
    return this->inputs_;
  }
  const std::vector<MatViewOperator *> &outputs() const {
    return this->outputs_;
  }

  // Get node by its index.
  inline Operator *GetNode(NodeIndex node_index) const {
    auto it = nodes_.find(node_index);
    return it == nodes_.end() ? nullptr : it->second.get();
  }

  // Debugging.
  std::string DebugString() const;
  uint64_t SizeInMemory() const;

  // Cloning to create more partitions.
  std::unique_ptr<DataFlowGraphPartition> Clone() const;

 private:
  // Maps input name to associated input operator.
  std::unordered_map<std::string, InputOperator *> inputs_;
  std::vector<MatViewOperator *> outputs_;

  // Nodes have doubly edges from parent to children and back stored inside.
  std::unordered_map<NodeIndex, std::unique_ptr<Operator>> nodes_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GRAPH_PARTITION_H_
