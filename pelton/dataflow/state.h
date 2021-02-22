// Defines the state of the dataflow system.
//
// The state includes the currently installed flows, including their operators
// and state.

#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"

#ifndef PELTON_DATAFLOW_STATE_H_
#define PELTON_DATAFLOW_STATE_H_

namespace pelton {
namespace dataflow {

// Type aliases.
using TableName = std::string;
using FlowName = std::string;

class DataflowState {
 public:
  DataflowState() = default;

  // Manage schemas.
  void AddTableSchema(const sqlast::CreateTable &create);

  SchemaRef GetTableSchema(const TableName &table_name) const;

  // Add and manage flows.
  void AddFlow(const FlowName &name, const DataFlowGraph &flow);

  const DataFlowGraph &GetFlow(const FlowName &name) const;

  bool HasFlowsFor(const TableName &table_name) const;

  // Save state to durable file.
  void Save(const std::string &dir_path);

  // Load state from its durable file (if exists).
  void Load(const std::string &dir_path);

  // Process raw records from sharder into flows.
  bool ProcessRawRecords(const std::vector<shards::RawRecord> &raw_records);

 private:
  bool ProcessRecords(const TableName &table_name,
                      const std::vector<Record> &records);

  // Creating and processing records from raw data.
  Record CreateRecord(const shards::RawRecord &raw_record) const;

  // Maps every table to its logical schema.
  // The logical schema is the contract between client code and our DB.
  // The stored schema may not matched the concrete/physical one due to sharding
  // or other transformations.
  std::unordered_map<TableName, SchemaOwner> schema_;

  // Dataflow graphs and views.
  std::unordered_map<FlowName, DataFlowGraph> flows_;
  std::unordered_map<TableName, std::vector<FlowName>> flows_per_input_table_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_STATE_H_
