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
DataFlowState::Worker::Worker(size_t id, Channel *chan, DataFlowState *state)
    : id_(id), state_(state), stop_(false) {
  this->thread_ = std::thread([this, chan]() {
    LOG(INFO) << "Starting dataflow worker " << this->id_;
    while (!this->stop_) {
      std::vector<Channel::Message> messages = chan->Read();
      for (Channel::Message &msg : messages) {
        this->state_->mtx_.lock_shared();
        auto &flow = this->state_->flows_.at(msg.flow_name);
        this->state_->mtx_.unlock_shared();
        Operator *op = flow->GetPartition(this->id_)->GetNode(msg.target);
        if (auto dp_options = pelton::dp::ParseViewForDP(msg.flow_name)) {
          op->ProcessAndForward(msg.source, std::move(msg.records),
                                std::move(msg.promise), &dp_options.value());
        } else {
          op->ProcessAndForward(msg.source, std::move(msg.records),
                                std::move(msg.promise), nullptr);
        }
      }
    }
  });
}
DataFlowState::Worker::~Worker() { CHECK(this->stop_); }
void DataFlowState::Worker::Shutdown() {
  if (!this->stop_) {
    LOG(INFO) << "Shutting down dataflow worker " << this->id_;
    this->stop_ = true;
    this->state_->channels_.at(this->id_)->Shutdown();
    this->thread_.join();
    LOG(INFO) << "Terminated dataflow worker " << this->id_;
  }
}
void DataFlowState::Worker::Join() { this->thread_.join(); }

// DataFlowState.
// Constructors and destructors.
DataFlowState::~DataFlowState() { CHECK(this->joined_); }
DataFlowState::DataFlowState(size_t workers, bool consistent)
    : workers_(workers), consistent_(consistent), joined_(false) {
  // Create a channel for every worker.
  for (size_t i = 0; i < workers; i++) {
    this->channels_.emplace_back(std::make_unique<Channel>(workers));
    this->threads_.emplace_back(
        std::make_unique<Worker>(i, this->channels_.at(i).get(), this));
  }
}

// Worker related functions.
void DataFlowState::Shutdown() {
  for (auto &worker : this->threads_) {
    worker->Shutdown();
  }
  this->joined_ = true;
}
void DataFlowState::Join() {
  for (auto &worker : this->threads_) {
    worker->Join();
  }
  this->joined_ = true;
}

// Manage schemas.
void DataFlowState::AddTableSchema(const sqlast::CreateTable &create) {
  std::unique_lock lock(this->mtx_);
  this->schema_.emplace(create.table_name(), SchemaFactory::Create(create));
}
void DataFlowState::AddTableSchema(const std::string &table_name,
                                   SchemaRef schema) {
  std::unique_lock lock(this->mtx_);
  this->schema_.emplace(table_name, schema);
}

std::vector<std::string> DataFlowState::GetTables() const {
  std::shared_lock lock(this->mtx_);
  std::vector<std::string> result;
  result.reserve(this->schema_.size());
  for (const auto &[table_name, _] : this->schema_) {
    result.push_back(table_name);
  }
  return result;
}
SchemaRef DataFlowState::GetTableSchema(const TableName &table_name) const {
  std::shared_lock lock(this->mtx_);
  if (this->schema_.count(table_name) > 0) {
    return this->schema_.at(table_name);
  } else {
    LOG(FATAL) << "Tried to get schema for non-existent table " << table_name;
  }
}

// Manage flows.
void DataFlowState::AddFlow(const FlowName &name,
                            std::unique_ptr<DataFlowGraphPartition> &&flow) {
  std::unique_lock lock(this->mtx_);
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

void DataFlowState::RemoveFlow(const FlowName &name,
                               std::vector<std::string> &flow_input_keys) {
  std::unique_lock lock(this->mtx_);
  for (auto input_name : flow_input_keys) {
    std::vector<FlowName> input_flows =
        this->flows_per_input_table_[input_name];
    std::vector<FlowName>::iterator it;
    it = std::find(input_flows.begin(), input_flows.end(), name);
    if (it != input_flows.end()) {
      input_flows.erase(it);
      this->flows_per_input_table_[input_name] = input_flows;
      if (input_flows.size() == 0) {
        this->flows_per_input_table_.erase(input_name);
      }
    }
  }
}

const DataFlowGraph &DataFlowState::GetFlow(const FlowName &name) const {
  std::shared_lock lock(this->mtx_);
  return *this->flows_.at(name);
}

bool DataFlowState::HasFlow(const FlowName &name) const {
  std::shared_lock lock(this->mtx_);
  return this->flows_.count(name) == 1;
}

bool DataFlowState::HasFlowsFor(const TableName &table_name) const {
  std::shared_lock lock(this->mtx_);
  return this->flows_per_input_table_.count(table_name) == 1;
}

Record DataFlowState::CreateRecord(const sqlast::Insert &insert_stmt) const {
  // Create an empty positive record with appropriate schema.
  const std::string &table_name = insert_stmt.table_name();
  this->mtx_.lock_shared();
  SchemaRef schema = this->schema_.at(table_name);
  this->mtx_.unlock_shared();
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
    // One future for the entirety of the record processing.
    Future future(this->consistent_);

    // Send copies per flow (except last flow).
    this->mtx_.lock_shared();
    const std::vector<FlowName> &flow_names =
        this->flows_per_input_table_.at(table_name);
    this->mtx_.unlock_shared();
    for (size_t i = 0; i < flow_names.size() - 1; i++) {
      const std::string &flow_name = flow_names.at(i);
      std::vector<Record> copy;
      copy.reserve(records.size());
      for (const Record &r : records) {
        copy.push_back(r.Copy());
      }
      this->ProcessRecordsByFlowName(flow_name, table_name, std::move(copy),
                                     future.GetPromise());
    }

    // Move into last flow.
    const std::string &flow_name = flow_names.back();
    this->ProcessRecordsByFlowName(flow_name, table_name, std::move(records),
                                   future.GetPromise());

    // Wait for the future to get resolved.
    future.Wait();
  }
}

void DataFlowState::ProcessRecordsByFlowName(const FlowName &flow_name,
                                             const TableName &table_name,
                                             std::vector<Record> &&records,
                                             Promise &&promise) {
  this->mtx_.lock_shared();
  auto &flow = this->flows_.at(flow_name);
  this->mtx_.unlock_shared();
  auto partitions = flow->PartitionInputs(table_name, std::move(records));
  for (auto &[partition, vec] : partitions) {
    Channel::Message msg = {
        flow_name, std::move(vec), UNDEFINED_NODE_INDEX,
        flow->GetPartition(partition)->GetInputNode(table_name)->index(),
        promise.Derive()};
    this->channels_.at(partition)->WriteInput(std::move(msg));
  }
  promise.Resolve();
}

// Size in memory of all the dataflow graphs.
sql::SqlResult DataFlowState::SizeInMemory() const {
  std::vector<Record> records;
  uint64_t total_size = 0;
  this->mtx_.lock_shared();
  for (const auto &[fname, flow] : this->flows_) {
    total_size += flow->SizeInMemory(&records);
  }
  this->mtx_.unlock_shared();
  records.emplace_back(SchemaFactory::MEMORY_SIZE_SCHEMA, true,
                       std::make_unique<std::string>("TOTAL"),
                       std::make_unique<std::string>("TOTAL"), total_size);
  return sql::SqlResult(
      sql::SqlResultSet(SchemaFactory::MEMORY_SIZE_SCHEMA, std::move(records)));
}

sql::SqlResult DataFlowState::FlowDebug(const std::string &flow_name) const {
  std::shared_lock lock(this->mtx_);
  const auto &flow = this->flows_.at(flow_name);
  return sql::SqlResult(sql::SqlResultSet(SchemaFactory::FLOW_DEBUG_SCHEMA,
                                          flow->DebugRecords()));
}

}  // namespace dataflow
}  // namespace pelton
