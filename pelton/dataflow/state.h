// Defines the state of the dataflow system.
//
// The state includes the currently installed flows, including their operators
// and state.

#include <atomic>
#include <list>
#include <memory>
#include <string>
// NOLINTNEXTLINE
#include <thread>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/graph_partition.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

#ifndef PELTON_DATAFLOW_STATE_H_
#define PELTON_DATAFLOW_STATE_H_

namespace pelton {
namespace dataflow {

// Type aliases.
using TableName = std::string;
using FlowName = std::string;

class DataFlowState {
 private:
  class Worker {
   public:
    Worker(size_t id, Channel *chan, DataFlowState *state, int max);
    ~Worker();
    void Shutdown();
    void Join();

   private:
    size_t id_;
    DataFlowState *state_;
    std::thread thread_;
    std::atomic<bool> stop_;
    int max_;
  };

 public:
  explicit DataFlowState(size_t workers, int max = -1);
  ~DataFlowState();

  // Manage schemas.
  void AddTableSchema(const sqlast::CreateTable &create);
  void AddTableSchema(const std::string &table_name, SchemaRef schema);

  std::vector<std::string> GetTables() const;
  SchemaRef GetTableSchema(const TableName &table_name) const;

  // Add and manage flows.
  void AddFlow(const FlowName &name,
               std::unique_ptr<DataFlowGraphPartition> &&flow);

  const DataFlowGraph &GetFlow(const FlowName &name) const;

  bool HasFlow(const FlowName &name) const;

  bool HasFlowsFor(const TableName &table_name) const;

  // Process raw data from sharder and use it to update flows.
  Record CreateRecord(const sqlast::Insert &insert_stmt) const;

  // Process raw data from sharder and use it to update flows.
  void ProcessRecords(const TableName &table_name,
                      std::vector<Record> &&records);

  void ProcessRecordsByFlowName(const FlowName &flow_name,
                                const TableName &table_name,
                                std::vector<Record> &&records);

  sql::SqlResult SizeInMemory() const;
  sql::SqlResult FlowDebug(const std::string &flow_name) const;

  // Shutdown all worker threads.
  size_t workers() const { return this->workers_; }
  void Shutdown();
  void Join();

 private:
  // The number of worker threads.
  size_t workers_;
  bool joined_;
  // Channel for every worker.
  std::vector<std::unique_ptr<Channel>> channels_;
  std::list<std::unique_ptr<Worker>> threads_;

  // Maps every table to its logical schema.
  // The logical schema is the contract between client code and our DB.
  // The stored schema may not matched the concrete/physical one due to sharding
  // or other transformations.
  std::unordered_map<TableName, SchemaRef> schema_;

  // DataFlow graphs and views.
  std::unordered_map<FlowName, std::unique_ptr<DataFlowGraph>> flows_;
  std::unordered_map<TableName, std::vector<FlowName>> flows_per_input_table_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_STATE_H_
