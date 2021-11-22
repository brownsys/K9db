// Defines the state of the dataflow system.
//
// The state includes the currently installed flows, including their operators
// and state.

#include "pelton/dataflow/state.h"

#include <fstream>
#include <iostream>
#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/util/fs.h"

#define STATE_FILE_NAME ".dataflow.state"

namespace pelton {
namespace dataflow {

// DataFlowState::Worker.
DataFlowState::Worker::Worker(size_t id, DataFlowState *state, int max)
    : id_(id), state_(state), stop_(false), max_(max) {
  this->thread_ = std::thread([this]() {
    std::cout << "Starting dataflow worker " << this->id_ << std::endl;
    while (!this->stop_ && this->max_ != 0) {
      std::vector<Channel::Message> messages =
          this->state_->channels_.at(this->id_)->Read();
      for (Channel::Message &msg : messages) {
        if (this->max_ != -1) {
          this->max_ -= msg.records.size();
        }
        auto &flow = this->state_->flows_.at(msg.flow_name);
        Operator *op = flow->GetPartition(this->id_)->GetNode(msg.target);
        op->ProcessAndForward(msg.source, std::move(msg.records));
      }
    }
  });
}
void DataFlowState::Worker::Shutdown() {
  if (!this->stop_) {
    this->stop_ = true;
    this->state_->channels_.at(this->id_)->Shutdown();
    this->thread_.join();
  }
}
void DataFlowState::Worker::Join() { this->thread_.join(); }

// DataFlowState.
// Constructors and destructors.
DataFlowState::~DataFlowState() { this->Shutdown(); }
DataFlowState::DataFlowState(size_t workers, int max) : workers_(workers) {
  // Create a channel for every worker.
  for (size_t i = 0; i < workers; i++) {
    this->channels_.emplace_back(std::make_unique<Channel>(workers));
    this->threads_.emplace_back(i, this, max);
  }
}

// Worker related functions.
void DataFlowState::Shutdown() {
  for (Worker &worker : this->threads_) {
    worker.Shutdown();
  }
}
void DataFlowState::Join() {
  for (Worker &worker : this->threads_) {
    worker.Join();
  }
}

// Manage schemas.
void DataFlowState::AddTableSchema(const sqlast::CreateTable &create) {
  this->schema_.emplace(create.table_name(), SchemaFactory::Create(create));
}
void DataFlowState::AddTableSchema(const std::string &table_name,
                                   SchemaRef schema) {
  this->schema_.emplace(table_name, schema);
}

std::vector<std::string> DataFlowState::GetTables() const {
  std::vector<std::string> result;
  result.reserve(this->schema_.size());
  for (const auto &[table_name, _] : this->schema_) {
    result.push_back(table_name);
  }
  return result;
}
SchemaRef DataFlowState::GetTableSchema(const TableName &table_name) const {
  if (this->schema_.count(table_name) > 0) {
    return this->schema_.at(table_name);
  } else {
    LOG(FATAL) << "Tried to get schema for non-existent table " << table_name;
  }
}

// Manage flows.
void DataFlowState::AddFlow(const FlowName &name,
                            std::unique_ptr<DataFlowGraphPartition> &&flow) {
  // Map input names to this flow.
  for (const auto &[input_name, input] : flow->inputs()) {
    this->flows_per_input_table_[input_name].push_back(name);
  }

  // Create a non-owning vector of channels.
  std::vector<Channel *> chans;
  for (auto &channel : this->channels_) {
    chans.push_back(channel.get());
  }

  // Turn the given partition into a graph with many partitions.
  auto graph = std::make_unique<DataFlowGraph>(name, this->workers_);
  graph->Initialize(std::move(flow), chans);
  this->flows_.emplace(name, std::move(graph));
}

const DataFlowGraph &DataFlowState::GetFlow(const FlowName &name) const {
  return *this->flows_.at(name);
}

bool DataFlowState::HasFlow(const FlowName &name) const {
  return this->flows_.count(name) == 1;
}

bool DataFlowState::HasFlowsFor(const TableName &table_name) const {
  return this->flows_per_input_table_.count(table_name) == 1;
}

Record DataFlowState::CreateRecord(const sqlast::Insert &insert_stmt) const {
  // Create an empty positive record with appropriate schema.
  const std::string &table_name = insert_stmt.table_name();
  SchemaRef schema = this->schema_.at(table_name);
  Record record{schema, true};

  // Fill in record with data.
  bool has_cols = insert_stmt.HasColumns();
  const std::vector<std::string> &cols = insert_stmt.GetColumns();
  const std::vector<std::string> &vals = insert_stmt.GetValues();
  for (size_t i = 0; i < vals.size(); i++) {
    size_t schema_index = i;
    if (has_cols) {
      schema_index = schema.IndexOf(cols.at(i));
    }
    if (vals.at(i) == "NULL") {
      record.SetNull(true, schema_index);
    } else {
      record.SetValue(vals.at(i), schema_index);
    }
  }

  return record;
}

void DataFlowState::ProcessRecords(const TableName &table_name,
                                   std::vector<Record> &&records) {
  if (records.size() > 0 && this->HasFlowsFor(table_name)) {
    // Send copies per flow (except last flow).
    const std::vector<FlowName> &flow_names =
        this->flows_per_input_table_.at(table_name);
    for (size_t i = 0; i < flow_names.size() - 1; i++) {
      const std::string &flow_name = flow_names.at(i);
      std::vector<Record> copy;
      copy.reserve(records.size());
      for (const Record &r : records) {
        copy.push_back(r.Copy());
      }
      auto &flow = this->flows_.at(flow_name);
      auto partitions = flow->PartitionInputs(table_name, std::move(copy));
      for (auto &[partition, vec] : partitions) {
        this->channels_.at(partition)->WriteInput(
            {flow_name, std::move(vec), UNDEFINED_NODE_INDEX,
             flow->GetPartition(partition)->GetInputNode(table_name)->index()});
      }
    }

    // Move into last flow.
    const std::string &flow_name = flow_names.back();
    auto &flow = this->flows_.at(flow_name);
    auto partitions = flow->PartitionInputs(table_name, std::move(records));
    for (auto &[partition, vec] : partitions) {
      this->channels_.at(partition)->WriteInput(
          {flow_name, std::move(vec), UNDEFINED_NODE_INDEX,
           flow->GetPartition(partition)->GetInputNode(table_name)->index()});
    }
  }
}

// Size in memory of all the dataflow graphs.
void DataFlowState::PrintSizeInMemory() const {
  uint64_t total_size = 0;
  for (const auto &[fname, flow] : this->flows_) {
    uint64_t flow_size = flow->SizeInMemory();
    uint64_t flow_size_mb = flow_size / 1024 / 1024;
    std::cout << fname << ": " << flow_size_mb << "MB" << std::endl;
    total_size += flow_size;
  }
  std::cout << "Total size: " << (total_size / 1204 / 1024) << "MB"
            << std::endl;
}

}  // namespace dataflow
}  // namespace pelton
