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
  explicit DataFlowGraphPartition(PartitionIndex id) : id_(id) {}

  // Not copyable or movable.
  DataFlowGraphPartition(const DataFlowGraphPartition &) = delete;
  DataFlowGraphPartition(DataFlowGraphPartition &&) = delete;
  DataFlowGraphPartition &operator=(const DataFlowGraphPartition &) = delete;
  DataFlowGraphPartition &operator=(DataFlowGraphPartition &&) = delete;

  // Add a node with some parents.
  inline bool AddNode(std::unique_ptr<Operator> &&op,
                      const std::vector<Operator *> &parents) {
    return this->AddNode(std::move(op), this->nodes_.size(), parents);
  }
  // Special case: node with single parent.
  inline bool AddNode(std::unique_ptr<Operator> &&op, Operator *parent) {
    return AddNode(std::move(op), std::vector<Operator *>{parent});
  }
  // Special cases: input and output nodes.
  inline bool AddInputNode(std::unique_ptr<InputOperator> &&op) {
    return this->AddInputNode(std::move(op), this->nodes_.size());
  }
  inline bool AddOutputOperator(std::unique_ptr<MatViewOperator> &&op,
                                Operator *parent) {
    return this->AddOutputOperator(std::move(op), this->nodes_.size(), parent);
  }

  // Inserting in the middle of the flow (i.e. in lieu of an edge).
  bool InsertNode(std::unique_ptr<Operator> &&op, Operator *parent,
                  Operator *child);

  NodeIndex LastOperatorIndex() const { return this->nodes_.size() - 1; }
  NodeIndex Size() const { return this->nodes_.size(); }

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
  InputOperator *GetInputNode(const std::string &input_name) const {
    return this->inputs_.at(input_name);
  }

  // Debugging.
  std::string DebugString() const;
  std::vector<Record> DebugRecords() const;
  uint64_t SizeInMemory(const std::string &flow_name,
                        std::vector<Record> *output) const;

  // Cloning to create more partitions.
  std::unique_ptr<DataFlowGraphPartition> Clone(
      PartitionIndex partition_index) const;

 private:
  // Private versions for adding nodes with a given ID.
  bool AddNode(std::unique_ptr<Operator> &&op, NodeIndex idx,
               const std::vector<Operator *> &parents);
  bool AddInputNode(std::unique_ptr<InputOperator> &&op, NodeIndex idx);
  bool AddOutputOperator(std::unique_ptr<MatViewOperator> &&op, NodeIndex idx,
                         Operator *parent);

  PartitionIndex id_;
  // Maps input name to associated input operator.
  std::unordered_map<std::string, InputOperator *> inputs_;
  std::vector<MatViewOperator *> outputs_;

  // Nodes have doubly edges from parent to children and back stored inside.
  std::unordered_map<NodeIndex, std::unique_ptr<Operator>> nodes_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GRAPH_PARTITION_H_
