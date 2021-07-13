// Defines the state of the dataflow system.
//
// The state includes the currently installed flows, including their operators
// and state.

#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"

#ifndef PELTON_DATAFLOW_STATE_H_
#define PELTON_DATAFLOW_STATE_H_

namespace pelton {
namespace dataflow {

// Type aliases.
using TableName = std::string;
using FlowName = std::string;

class DataFlowState {
 public:
  DataFlowState() = default;

  // Manage schemas.
  void AddTableSchema(const sqlast::CreateTable &create);
  void AddTableSchema(const std::string &table_name, SchemaRef schema);

  std::vector<std::string> GetTables() const;
  SchemaRef GetTableSchema(const TableName &table_name) const;

  // Add and manage flows.
  void AddFlow(const FlowName &name, const std::shared_ptr<DataFlowGraph> flow);

  const std::shared_ptr<DataFlowGraph> GetFlow(const FlowName &name) const;

  bool HasFlow(const FlowName &name) const;

  bool HasFlowsFor(const TableName &table_name) const;

  // Save state to durable file.
  void Save(const std::string &dir_path);

  // Load state from its durable file (if exists).
  void Load(const std::string &dir_path);

  Record CreateRecord(const sqlast::Insert &insert_stmt) const;

  // Process raw data from sharder and use it to update flows.
  bool ProcessRecords(const TableName &table_name,
                      const std::vector<Record> &records);

  uint64_t SizeInMemory() const;

  ~DataFlowState();

 private:
  // Maps every table to its logical schema.
  // The logical schema is the contract between client code and our DB.
  // The stored schema may not matched the concrete/physical one due to sharding
  // or other transformations.
  std::unordered_map<TableName, SchemaRef> schema_;

  // Base graphs supplied to the engine
  std::unordered_map<FlowName, std::shared_ptr<DataFlowGraph>> base_graphs_;
  // DataFlow graphs and views.
  std::unordered_map<FlowName, std::shared_ptr<DataFlowGraph>> flows_;
  std::unordered_map<TableName, std::vector<FlowName>> flows_per_input_table_;

  absl::flat_hash_map<
      FlowName, absl::flat_hash_map<uint16_t, std::shared_ptr<DataFlowGraph>>>
      partitioned_graphs_;
  absl::flat_hash_map<FlowName,
                      absl::flat_hash_map<uint16_t, std::shared_ptr<Channel>>>
      partition_chans_;
  absl::flat_hash_map<FlowName,
                      absl::flat_hash_map<TableName, std::vector<ColumnID>>>
      input_partitioned_by_;
  uint16_t partition_count_ = 3;
  // Just an object to store threads, will probably only use them to join and
  // terminate the threads gracefully.
  std::vector<std::thread> threads_;

  void TraverseBaseGraph(FlowName name);
  void VisitNode(std::shared_ptr<Operator> node, FlowName name);
  std::pair<bool, std::vector<ColumnID>> GetRecentPartionBoundary(
      std::shared_ptr<Operator> node, bool check_self, FlowName name);
  void AddExchangeAfter(std::shared_ptr<Operator> node,
                        std::vector<ColumnID> partition_key, FlowName name);
  // Get input operators for the subgraph whose root is @node
  std::vector<std::shared_ptr<InputOperator>> GetSubgraphInputs(
      std::shared_ptr<Operator> node) const;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_STATE_H_
