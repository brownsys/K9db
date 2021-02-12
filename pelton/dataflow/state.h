// Defines the state of the dataflow system.
//
// The state includes the currently installed flows, including their operators
// and state.

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/schema.h"

#ifndef PELTON_DATAFLOW_STATE_H_
#define PELTON_DATAFLOW_STATE_H_

namespace pelton {
namespace dataflow {

// Type aliases.
using TableName = std::string;
using FlowName = std::string;
using InputOperators = std::vector<std::shared_ptr<dataflow::InputOperator>>;

class DataflowState {
 public:
  DataflowState() {}

  // Add and manage flows.
  void AddFlow(const FlowName &name, const dataflow::DataFlowGraph &flow);

  const dataflow::DataFlowGraph &GetFlow(const FlowName &name) const;

  bool HasInputsFor(const TableName &table_name) const;

  const InputOperators &InputsFor(const TableName &table_name) const;

  // Save state to durable file.
  void Save(const std::string &dir_path);

  // Load state from its durable file (if exists).
  void Load(const std::string &dir_path);

 private:
  // Maps every table to its logical schema.
  // The logical schema is the contract between client code and our DB.
  // The stored schema may not matched the concrete/physical one due to sharding
  // or other transformations.
  std::unordered_map<TableName, dataflow::Schema> schema_;

  // Dataflow graphs and views.
  std::unordered_map<FlowName, dataflow::DataFlowGraph> flows_;
  std::unordered_map<TableName, InputOperators> inputs_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_STATE_H_
