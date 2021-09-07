#ifndef PELTON_DATAFLOW_GRAPH_H_
#define PELTON_DATAFLOW_GRAPH_H_

#include <memory>
#include <optional>
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
class Channel;

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
  // Insert a new node after initial construction of graph. This will be
  // primarily used to insert exchange operators.
  bool InsertNodeAfter(std::shared_ptr<Operator> op,
                       std::shared_ptr<Operator> parent);
  // Special case: output operator is added to outputs_.
  bool AddOutputOperator(std::shared_ptr<MatViewOperator> op,
                         std::shared_ptr<Operator> parent);

  // Process records that are meant for an input operator
  void Process(const std::string &input_name,
               const std::vector<Record> &records) const;
  // Process records that are meant for a node specified by @destination_index
  // and perceived as if they were sent by @source_index
  void Process(const NodeIndex destination_index,
               const std::optional<NodeIndex> source_index,
               const std::vector<Record> &records) const;

  // Accessors.
  const std::unordered_map<std::string, std::shared_ptr<InputOperator>>
      &inputs() const {
    return this->inputs_;
  }
  const std::vector<std::shared_ptr<MatViewOperator>> &outputs() const {
    return this->outputs_;
  }
  const GraphIndex &index() { return this->index_; }
  const PartitionID &partition_id() { return this->partition_id_; }
  const size_t node_count() { return this->nodes_.size(); }

  // Get node by its index.
  inline std::shared_ptr<Operator> GetNode(NodeIndex node_index) const {
    auto it = nodes_.find(node_index);
    return it == nodes_.end() ? nullptr : it->second;
  }

  void SetIndex(GraphIndex graph_index) { this->index_ = graph_index; }
  void SetPartitionID(PartitionID partition_id) {
    this->partition_id_ = partition_id;
  }
  std::shared_ptr<DataFlowGraph> Clone();

  // Debugging.
  std::string DebugString() const;

  uint64_t SizeInMemory() const;

 private:
  GraphIndex index_;
  PartitionID partition_id_;
  void AddEdge(std::shared_ptr<Operator> parent,
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
