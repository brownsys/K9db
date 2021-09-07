// Execution engine for the sharded dataflow. It also maintains the dataflow
// state.
//
// The state includes the currently installed flows, including their operators
// and state.

#include "pelton/dataflow/engine.h"

#include <chrono>
#include <fstream>
#include <memory>
#include <queue>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/exchange.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/partition.h"
#include "pelton/dataflow/stop_message.h"
#include "pelton/util/fs.h"

#define STATE_FILE_NAME ".dataflow.state"

namespace pelton {
namespace dataflow {

DataFlowEngine::DataFlowEngine(PartitionID partition_count)
    : partition_count_(partition_count) {
  // Initialize worker objects
  for (PartitionID i = 0; i < this->partition_count_; i++) {
    this->workers_.emplace(i,
                           std::make_shared<Worker>(
                               i, std::make_shared<std::condition_variable>()));
  }
}

// Manage schemas.
void DataFlowEngine::AddTableSchema(const sqlast::CreateTable &create) {
  this->schema_.emplace(create.table_name(), SchemaFactory::Create(create));
}
void DataFlowEngine::AddTableSchema(const std::string &table_name,
                                    SchemaRef schema) {
  this->schema_.emplace(table_name, schema);
}

std::vector<std::string> DataFlowEngine::GetTables() const {
  std::vector<std::string> result;
  result.reserve(this->schema_.size());
  for (const auto &[table_name, _] : this->schema_) {
    result.push_back(table_name);
  }
  return result;
}
SchemaRef DataFlowEngine::GetTableSchema(const TableName &table_name) const {
  if (this->schema_.count(table_name) > 0) {
    return this->schema_.at(table_name);
  } else {
    LOG(FATAL) << "Tried to get schema for non-existent table " << table_name;
  }
}

// Manage flows.
void DataFlowEngine::AddFlow(const FlowName &name,
                             const std::shared_ptr<DataFlowGraph> flow) {
  flow->SetIndex(this->flows_.size());
  this->flows_.insert({name, flow});
  for (const auto &[input_name, input] : flow->inputs()) {
    this->flows_per_input_table_[input_name].push_back(name);
  }
  // Tasks related to the dataflow engine
  this->partitioned_graphs_.emplace(
      name, absl::flat_hash_map<PartitionID, std::shared_ptr<DataFlowGraph>>{});
  this->partition_chans_.emplace(
      name, absl::flat_hash_map<PartitionID, std::shared_ptr<Channel>>{});
  for (PartitionID i = 0; i < this->partition_count_; i++) {
    std::shared_ptr<Channel> comm_chan = std::make_shared<Channel>(
        this->workers_.at(i)->condition_variable(), this->workers_.at(i));
    comm_chan->SetGraphIndex(flow->index());
    this->partition_chans_.at(name).emplace(i, comm_chan);
    std::shared_ptr<DataFlowGraph> partition = flow->Clone();
    partition->SetPartitionID(i);
    this->partitioned_graphs_.at(name).emplace(i, partition);
  }
  // Initialize data structures for input partitions
  // NOTE: Since every flow at least has a matview, each input is expected to be
  // partitioned on some key. Hence, no default partitioning key will be
  // specified for the inputs.
  this->input_partitioned_by_.emplace(
      name, absl::flat_hash_map<TableName, std::vector<ColumnID>>{});
  for (auto item : flow->inputs()) {
    this->input_partitioned_by_.at(name).emplace(item.first,
                                                 std::vector<ColumnID>{});
  }

  this->TraverseBaseGraph(name);

  // Add partitioned graphs to appropriate workers
  for (const auto &[partition, graph] : this->partitioned_graphs_.at(name)) {
    this->workers_.at(partition)->AddPartitionedGraph(graph);
  }
  // The workers must monitor channels that have been reserved for
  // clients as well.
  for (const auto &[partition, worker] : this->workers_) {
    worker->MonitorChannel(this->partition_chans_.at(name).at(partition));
  }
  // Deploy one worker per thread
  for (const auto &[_, worker] : this->workers_) {
    this->threads_.push_back(std::thread(&Worker::Start, worker));
  }
}

void DataFlowEngine::TraverseBaseGraph(const FlowName &name) {
  // Annotate base graph
  this->AnnotateBaseGraph(this->flows_.at(name));

  // A single instance of this is used during the entire traversal
  std::optional<std::shared_ptr<Operator>> tracking_union = std::nullopt;
  auto matview_op = this->flows_.at(name)->outputs().at(0);
  // Start a DFS traversal that inserts exchange operators where necessary.
  this->VisitNode(matview_op, matview_op->key_cols(), &tracking_union, name);
}

void DataFlowEngine::AnnotateBaseGraph(
    const std::shared_ptr<DataFlowGraph> graph) {
  for (size_t i = 0; i < graph->node_count(); i++) {
    const auto node = graph->GetNode(i);
    switch (node->type()) {
      case Operator::Type::FILTER:
      case Operator::Type::PROJECT:
      case Operator::Type::UNION:
      case Operator::Type::INPUT: {
        node->partitioned_by_ = std::nullopt;
      } break;
      case Operator::Type::MAT_VIEW: {
        auto matview_op = std::dynamic_pointer_cast<MatViewOperator>(node);
        node->partitioned_by_ = matview_op->key_cols();
      } break;
      case Operator::Type::EQUIJOIN: {
        auto equijoin_op = std::dynamic_pointer_cast<EquiJoinOperator>(node);
        node->partitioned_by_ =
            std::vector<ColumnID>{equijoin_op->join_column()};
      } break;
      case Operator::Type::AGGREGATE: {
        auto agg_op = std::dynamic_pointer_cast<AggregateOperator>(node);
        node->partitioned_by_ = agg_op->OutPartitionCols();
      } break;
      case Operator::Type::EXCHANGE:
        LOG(FATAL) << "Not expected here, exchange should be inserted after "
                      "traversal.";
      default:
        LOG(FATAL) << "Unsupported Operator";
    }
  }
}

void DataFlowEngine::VisitNode(
    std::shared_ptr<Operator> node, std::vector<ColumnID> recent_partition,
    std::optional<std::shared_ptr<Operator>> *tracking_union,
    const FlowName &name) {
  if (node->visited_) {
    return;
  }
  node->visited_ = true;
  switch (node->type()) {
    case Operator::Type::FILTER:
    case Operator::Type::PROJECT:
    case Operator::Type::MAT_VIEW:
      return this->VisitNode(node->GetParents().at(0), recent_partition,
                             tracking_union, name);
    case Operator::Type::UNION:
      // Union requires both inputs to be partitioned by the same key. But
      // that key is not known at this point, it could be a couple of operators
      // down the line. Hence, start tracking this union until a partition
      // boundary is reached. (At a later point in time) whenever a partitioning
      // boundary is encountered, an exchange will be inserted after the union
      // that is being tracked.
      {
        // If a union is already being tracked it implies that there is no
        // partitioning boundary present between the tracked union and this
        // union. Hence, a single exchange operator (inserted after the
        // tracked union) would be sufficient, and hence there is no need to
        // track this union.
        if (!*tracking_union) {
          *tracking_union = node;
        }
        this->VisitNode(node->GetParents().at(0), recent_partition,
                        tracking_union, name);
        this->VisitNode(node->GetParents().at(1), recent_partition,
                        tracking_union, name);
      }
      return;
    case Operator::Type::INPUT: {
      auto input_op = std::dynamic_pointer_cast<InputOperator>(node);
      const auto &partition_cols =
          this->input_partitioned_by_.at(name).at(input_op->input_name());
      if (partition_cols.size() == 0) {
        // A decision has not been made on the paritioning key for this input
        if (*tracking_union) {
          tracking_union->value()->partitioned_by_ = recent_partition;
          *tracking_union = std::nullopt;
        }
        this->input_partitioned_by_.at(name).at(input_op->input_name()) =
            recent_partition;
      } else if (partition_cols != recent_partition) {
        if (*tracking_union) {
          this->AddExchangeAfter(tracking_union->value()->index(),
                                 partition_cols, name);
          *tracking_union = std::nullopt;
        }
        this->AddExchangeAfter(input_op->index(), partition_cols, name);
      }
    }
      return;
    case Operator::Type::EQUIJOIN: {
      auto equijoin_op = std::dynamic_pointer_cast<EquiJoinOperator>(node);
      auto partition_cols = std::vector<ColumnID>{equijoin_op->join_column()};
      if (*tracking_union) {
        tracking_union->value()->partitioned_by_ = partition_cols;
        if (recent_partition != partition_cols) {
          this->AddExchangeAfter(tracking_union->value()->index(),
                                 partition_cols, name);
        }
        *tracking_union = std::nullopt;
      } else if (partition_cols != recent_partition) {
        this->AddExchangeAfter(node->index(), recent_partition, name);
      }
      this->VisitNode(equijoin_op->GetParents().at(0),
                      std::vector<ColumnID>{equijoin_op->left_id()},
                      tracking_union, name);
      this->VisitNode(equijoin_op->GetParents().at(1),
                      std::vector<ColumnID>{equijoin_op->right_id()},
                      tracking_union, name);
    } break;
    case Operator::Type::AGGREGATE: {
      auto agg_op = std::dynamic_pointer_cast<AggregateOperator>(node);
      auto partition_cols = agg_op->OutPartitionCols();
      if (*tracking_union) {
        tracking_union->value()->partitioned_by_ = partition_cols;
        if (recent_partition != partition_cols) {
          this->AddExchangeAfter(tracking_union->value()->index(),
                                 partition_cols, name);
        }
        *tracking_union = std::nullopt;
      } else if (partition_cols != recent_partition) {
        this->AddExchangeAfter(node->index(), recent_partition, name);
      }
      this->VisitNode(agg_op->GetParents().at(0), agg_op->group_columns(),
                      tracking_union, name);
    } break;
    case Operator::Type::EXCHANGE:
      LOG(FATAL)
          << "Not expected here, exchange should be inserted after traversal.";
    default:
      LOG(FATAL) << "Unsupported Operator";
  }
}

// Each exchange operator has it's own set of dedicated channels to communicate
// with other partitions.
absl::flat_hash_map<PartitionID, std::shared_ptr<Channel>>
DataFlowEngine::ConstructChansForExchange(PartitionID current_partition,
                                          const FlowName &name) const {
  absl::flat_hash_map<PartitionID, std::shared_ptr<Channel>> chans;
  for (PartitionID i = 0; i < this->partition_count_; i++) {
    if (i == current_partition) {
      // The exchange operator does not need a channel to forward records in
      // it's own partition. It simply uses the recursive process function call.
      continue;
    }
    auto chan = std::make_shared<Channel>(
        this->workers_.at(i)->condition_variable(), this->workers_.at(i));
    chan->SetGraphIndex(this->flows_.at(name)->index());
    chans.emplace(i, chan);
  }
  return chans;
}

void DataFlowEngine::AddExchangeAfter(NodeIndex parent_index,
                                      std::vector<ColumnID> partition_key,
                                      const FlowName &name) {
  for (PartitionID partition = 0; partition < this->partition_count_;
       partition++) {
    auto partitioned_graph = this->partitioned_graphs_.at(name).at(partition);
    auto exchange_chans = this->ConstructChansForExchange(partition, name);
    auto exchange_op = std::make_shared<ExchangeOperator>(
        exchange_chans, partition_key, partition, this->partition_count_);
    partitioned_graph->InsertNodeAfter(
        exchange_op, partitioned_graph->GetNode(parent_index));

    // Workers should monitor appropriate channels
    for (const auto &[partition, chan] : exchange_chans) {
      this->workers_.at(partition)->MonitorChannel(chan);
    }
  }
}

const std::shared_ptr<DataFlowGraph> DataFlowEngine::GetFlow(
    const FlowName &name) const {
  return this->flows_.at(name);
}

const std::shared_ptr<DataFlowGraph> DataFlowEngine::GetPartitionedFlow(
    const FlowName &name, PartitionID partition_id) const {
  return this->partitioned_graphs_.at(name).at(partition_id);
}

const std::shared_ptr<MatViewOperator> DataFlowEngine::GetPartitionedMatView(
    const FlowName &name, const Key &key) const {
  uint64_t partition_index =
      partition::GetPartition(key, this->partition_count_);
  // Currently, only one matview is supported per flow.
  CHECK_EQ(
      this->partitioned_graphs_.at(name).at(partition_index)->outputs().size(),
      static_cast<size_t>(1));
  return this->partitioned_graphs_.at(name)
      .at(partition_index)
      ->outputs()
      .at(0);
}

const std::vector<std::shared_ptr<MatViewOperator>>
DataFlowEngine::GetPartitionedMatViews(const FlowName &name) {
  std::vector<std::shared_ptr<MatViewOperator>> matviews;
  for (const auto item : this->partitioned_graphs_.at(name)) {
    // Currently, only one matview is supported per flow.
    CHECK_EQ(item.second->outputs().size(), static_cast<size_t>(1));
    matviews.push_back(item.second->outputs().at(0));
  }
  return matviews;
}

const SchemaRef DataFlowEngine::GetOutputSchema(const FlowName &name) {
  const auto flow = this->flows_.at(name);
  // Currently, we only support a single matview per flow.
  CHECK_EQ(flow->outputs().size(), static_cast<size_t>(1));
  return flow->outputs().at(0)->output_schema();
}

const std::vector<ColumnID> &DataFlowEngine::GetMatViewKeyCols(
    const FlowName &name) {
  const auto flow = this->flows_.at(name);
  // Currently, we only support a single matview per flow.
  CHECK_EQ(flow->outputs().size(), static_cast<size_t>(1));
  return flow->outputs().at(0)->key_cols();
}

bool DataFlowEngine::HasFlow(const FlowName &name) const {
  return this->flows_.count(name) == 1;
}

bool DataFlowEngine::HasFlowsFor(const TableName &table_name) const {
  return this->flows_per_input_table_.count(table_name) == 1;
}

Record DataFlowEngine::CreateRecord(const sqlast::Insert &insert_stmt) const {
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

void DataFlowEngine::ProcessRecords(const TableName &table_name,
                                    std::vector<Record> &&records) const {
  if (records.size() > 0 && this->HasFlowsFor(table_name)) {
    for (size_t i = 0; i < this->flows_per_input_table_.at(table_name).size();
         i++) {
      const FlowName &name = this->flows_per_input_table_.at(table_name).at(i);
      // Partition records based on key specified by the input operator
      auto input_partition_key =
          this->input_partitioned_by_.at(name).at(table_name);
      absl::flat_hash_map<PartitionID, std::vector<Record>>
          partitioned_records = partition::HashPartition(
              records, input_partition_key, this->partition_count_);
      // Send batch messages to appropriate partitions
      auto flow = this->flows_.at(name);
      for (auto &item : partitioned_records) {
        auto input_op = flow->inputs().at(table_name);
        auto batch_msg = std::make_shared<BatchMessage>(input_op->index(),
                                                        std::move(item.second));
        this->partition_chans_.at(name).at(item.first)->Send(batch_msg);
      }
    }
  }
}

// Size in memory of all the dataflow graphs.
uint64_t DataFlowEngine::SizeInMemory() const {
  uint64_t size = 0;
  for (const auto &[_, flow] : this->flows_) {
    size += flow->SizeInMemory();
  }
  return size;
}

// Load state from its durable file (if exists).
void DataFlowEngine::Load(const std::string &dir_path) {
  // State file does not exists: this is a fresh database that was not
  // created previously!
  std::string state_file_path = dir_path + STATE_FILE_NAME;
  if (!util::FileExists(state_file_path)) {
    return;
  }

  // Open file for reading.
  std::ifstream state_file;
  util::OpenRead(&state_file, state_file_path);

  // Read content of state file and fill them into state!
  std::string line;

  // Read schema_.
  getline(state_file, line);
  while (line != "") {
    std::vector<std::string> names;
    std::vector<sqlast::ColumnDefinition::Type> types;
    std::string colname;
    std::underlying_type_t<sqlast::ColumnDefinition::Type> coltype;
    // Read the column names and types in schema.
    getline(state_file, colname);
    while (colname != "") {
      state_file >> coltype;
      names.push_back(colname);
      types.push_back(static_cast<sqlast::ColumnDefinition::Type>(coltype));
      getline(state_file, colname);
      getline(state_file, colname);
    }
    // Read the key column ids.
    std::vector<ColumnID> keys;
    ColumnID key;
    size_t keys_size;
    state_file >> keys_size;
    for (size_t i = 0; i < keys_size; i++) {
      state_file >> key;
      keys.push_back(key);
    }

    // Construct schema in place.
    this->schema_.insert({line, SchemaFactory::Create(names, types, keys)});

    // Next table.
    getline(state_file, line);
    getline(state_file, line);
  }

  // Done reading!
  state_file.close();
}

// Save state to durable file.
void DataFlowEngine::Save(const std::string &dir_path) {
  // Open state file for writing.
  std::ofstream state_file;
  util::OpenWrite(&state_file, dir_path + STATE_FILE_NAME);

  for (const auto &[table_name, schema] : this->schema_) {
    state_file << table_name << "\n";
    for (size_t i = 0; i < schema.size(); i++) {
      auto type =
          static_cast<std::underlying_type_t<sqlast::ColumnDefinition::Type>>(
              schema.TypeOf(i));
      state_file << schema.NameOf(i) << "\n" << type << "\n";
    }
    state_file << "\n";
    state_file << schema.keys().size() << " ";
    for (auto key_id : schema.keys()) {
      state_file << key_id << " ";
    }
    state_file << "\n";
  }
  state_file << "\n";

  // Done writing.
  state_file.close();
}

DataFlowEngine::~DataFlowEngine() {
  // Give some time for the previously sent records to get processed
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  std::shared_ptr<StopMessage> stop_msg = std::make_shared<StopMessage>();
  // Only send stop messages to all partitions of any flow. This will ensure
  // that all workers get exactly one stop message which will cause them to
  // terminate.
  for (auto const &item1 : this->partition_chans_) {
    for (auto const &item2 : item1.second) {
      item2.second->Send(stop_msg);
    }
    break;
  }

  for (auto &thread : this->threads_) {
    thread.join();
  }
}

}  // namespace dataflow
}  // namespace pelton
