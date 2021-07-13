// Defines the state of the dataflow system.
//
// The state includes the currently installed flows, including their operators
// and state.

#include "pelton/dataflow/state.h"

#include <chrono>
#include <fstream>
#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/exchange.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/stop_message.h"
#include "pelton/util/fs.h"

#define STATE_FILE_NAME ".dataflow.state"

namespace pelton {
namespace dataflow {

void DataFlowState::TraverseBaseGraph(FlowName name) {
  // Traverse depth first starting from the matview
  // TODO(Ishan): Instances where the following assertion will fail?
  assert(this->flows_.at(name)->outputs().size() == 1);
  for (auto const &matview : this->flows_.at(name)->outputs()) {
    if (matview->visited_) {
      continue;
    }
    this->VisitNode(matview, name);
  }
}

void DataFlowState::VisitNode(std::shared_ptr<Operator> node, FlowName name) {
  if (node->visited_) {
    return;
  }
  node->visited_ = true;
  switch (node->type()) {
    case Operator::Type::FILTER:
    case Operator::Type::PROJECT:
      // These operators have no partioning requirements, hence continue
      // visiting.
      return VisitNode(node->GetParents()[0], name);
    case Operator::Type::UNION:
      // TODO(Ishan): Think about edge cases for union.
      LOG(FATAL) << "Not supported yet";
    case Operator::Type::INPUT:
      // By default the input operators will partition records by the record
      // key. But this can be changed later based on the requirement of an
      // upstream stateful operator (if any). Reached the bottom, hence no more
      // nodes to visit.
      return;
    case Operator::Type::MAT_VIEW: {
      auto matview_op = std::dynamic_pointer_cast<MatViewOperator>(node);
      std::pair<bool, std::vector<ColumnID>> boundary =
          GetRecentPartionBoundary(node, false, name);
      if (!boundary.first) {
        // Simply specify partitioning at the inputs of the dataflow graph
        // TODO(Ishan): Check on this again, Following is based on the
        // assumption that one flow will have a single matview op.
        this->input_partitioned_by_.emplace(
            name, absl::flat_hash_map<TableName, std::vector<ColumnID>>{});
        for (auto const &item : this->flows_.at(name)->inputs()) {
          this->input_partitioned_by_.at(name).emplace(item.first,
                                                       matview_op->key_cols());
        }
      } else if (boundary.second != matview_op->key_cols()) {
        AddExchangeAfter(matview_op->GetParents()[0], matview_op->key_cols(),
                         name);
      }
    }
      return VisitNode(node->GetParents()[0], name);
    case Operator::Type::AGGREGATE: {
      auto aggregate_op = std::dynamic_pointer_cast<AggregateOperator>(node);
      auto boundary = GetRecentPartionBoundary(node, false, name);
      if (!boundary.first) {
        for (auto input_op : GetSubgraphInputs(node)) {
          this->input_partitioned_by_.at(name).emplace(
              input_op->input_name(), aggregate_op->group_columns());
        }
      } else if (boundary.second != aggregate_op->group_columns()) {
        AddExchangeAfter(aggregate_op->GetParents()[0],
                         aggregate_op->group_columns(), name);
      }
    }
      return VisitNode(node->GetParents()[0], name);
    case Operator::Type::EQUIJOIN: {
      auto left_op = node->GetParents()[0];
      auto right_op = node->GetParents()[1];
      auto left_boundary = GetRecentPartionBoundary(left_op, true, name);
      auto right_boundary = GetRecentPartionBoundary(right_op, true, name);
      auto equijoin_op = std::dynamic_pointer_cast<EquiJoinOperator>(node);
      auto left_partition_cols = std::vector<ColumnID>{equijoin_op->left_id()};
      auto right_partition_cols = std::vector<ColumnID>{equijoin_op->left_id()};

      if (!left_boundary.first) {
        // Partition at left op's inputs
        for (auto input_op : GetSubgraphInputs(left_op)) {
          this->input_partitioned_by_.at(name).emplace(input_op->input_name(),
                                                       left_partition_cols);
        }
      } else {
        AddExchangeAfter(left_op, left_partition_cols, name);
      }

      if (!right_boundary.first) {
        // Partition at right op's inputs
        for (auto input_op : GetSubgraphInputs(right_op)) {
          this->input_partitioned_by_.at(name).emplace(input_op->input_name(),
                                                       right_partition_cols);
        }
      } else {
        AddExchangeAfter(right_op, right_partition_cols, name);
      }
    } break;
    case Operator::Type::EXCHANGE:
      LOG(FATAL)
          << "Not expected here, exchange should be inserted after traversal.";
    default:
      LOG(FATAL) << "Unsupported Operator";
  }
}

std::pair<bool, std::vector<ColumnID>> DataFlowState::GetRecentPartionBoundary(
    std::shared_ptr<Operator> node, bool check_self, FlowName name) {
  if (check_self) {
    // Start from one level up. This flag is primarily used when obtaining
    // the partition boundary for inputs of a join.
    return GetRecentPartionBoundary(node->GetChildren()[0], false, name);
    // return std::pair<bool, std::vector<ColumnID>>();
  }
  std::pair<bool, std::vector<ColumnID>> response;
  auto parent = node->GetParents()[0];
  switch (parent->type()) {
    case Operator::Type::FILTER:
    case Operator::Type::PROJECT:
      return this->GetRecentPartionBoundary(parent, false, name);
    case Operator::Type::UNION:
      // TODO(Ishan): Any restrictions on the partitioning for the children of
      // union? should they be the same?
      return response;
    case Operator::Type::INPUT: {
      auto input_op = std::dynamic_pointer_cast<InputOperator>(node);
      if (!this->input_partitioned_by_.contains(name) ||
          !this->input_partitioned_by_.at(name).contains(
              input_op->input_name())) {
        // A decision has not been made on the paritioning key for this input
        return response;
      }
      response = {true, this->input_partitioned_by_.at(name).at(
                            input_op->input_name())};
      return response;
    }
    case Operator::Type::MAT_VIEW: {
      auto matview_op = std::dynamic_pointer_cast<MatViewOperator>(node);
      response = {true, matview_op->key_cols()};
    }
      return response;
    case Operator::Type::EQUIJOIN: {
      auto equijoin_op = std::dynamic_pointer_cast<EquiJoinOperator>(node);
      response = {true, std::vector<ColumnID>{equijoin_op->join_column()}};
    }
      return response;
    case Operator::Type::AGGREGATE: {
      auto agg_op = std::dynamic_pointer_cast<AggregateOperator>(node);
      response = {true, agg_op->group_columns()};
    }
      return response;
    case Operator::Type::EXCHANGE:
      LOG(FATAL)
          << "Not expected here, exchange should be inserted after traversal.";
    default:
      LOG(FATAL) << "Unsupported Operator";
  }
}

void DataFlowState::AddExchangeAfter(std::shared_ptr<Operator> node,
                                     std::vector<ColumnID> partition_key,
                                     FlowName name) {
  for (uint16_t partition = 0; partition < this->partition_count_;
       partition++) {
    auto exchange_op = std::make_shared<ExchangeOperator>(
        this->partition_chans_.at(name), partition_key, partition,
        this->partition_count_);
    this->partitioned_graphs_.at(name).at(partition)->InsertNodeAfter(
        exchange_op, node);
  }
}

std::vector<std::shared_ptr<InputOperator>> DataFlowState::GetSubgraphInputs(
    std::shared_ptr<Operator> node) const {
  if (node->type() == Operator::Type::INPUT) {
    return std::vector<std::shared_ptr<InputOperator>>{
        std::dynamic_pointer_cast<InputOperator>(node)};
  }
  std::vector<std::shared_ptr<InputOperator>> inputs;
  for (auto parent : node->GetParents()) {
    auto parent_inputs = GetSubgraphInputs(parent);
    inputs.insert(std::end(inputs), std::begin(parent_inputs),
                  std::end(parent_inputs));
  }
  return inputs;
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
                            const std::shared_ptr<DataFlowGraph> flow) {
  this->flows_.insert({name, flow});
  for (const auto &[input_name, input] : flow->inputs()) {
    this->flows_per_input_table_[input_name].push_back(name);
  }

  // Tasks related to the dataflow engine
  this->partitioned_graphs_.emplace(
      name, absl::flat_hash_map<uint16_t, std::shared_ptr<DataFlowGraph>>{});
  this->partition_chans_.emplace(
      name, absl::flat_hash_map<uint16_t, std::shared_ptr<Channel>>{});
  for (uint16_t i = 0; i < this->partition_count_; i++) {
    std::shared_ptr<Channel> comm_chan = std::make_shared<Channel>();
    this->partition_chans_.at(name).emplace(i, comm_chan);
    std::shared_ptr<DataFlowGraph> partition = flow->Clone();
    this->partitioned_graphs_.at(name).emplace(i, partition);
    // this->threads_.push_back(std::thread(&DataFlowGraph::Start, partition,
    // comm_chan));
  }
}

const std::shared_ptr<DataFlowGraph> DataFlowState::GetFlow(
    const FlowName &name) const {
  return this->flows_.at(name);
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

bool DataFlowState::ProcessRecords(const TableName &table_name,
                                   const std::vector<Record> &records) {
  if (records.size() > 0 && this->HasFlowsFor(table_name)) {
    for (const FlowName &name : this->flows_per_input_table_.at(table_name)) {
      std::shared_ptr<DataFlowGraph> graph = this->flows_.at(name);
      if (!graph->Process(table_name, records)) {
        return false;
      }
    }
  }
  return true;
}

// Size in memory of all the dataflow graphs.
uint64_t DataFlowState::SizeInMemory() const {
  uint64_t size = 0;
  for (const auto &[_, flow] : this->flows_) {
    size += flow->SizeInMemory();
  }
  return size;
}

// Load state from its durable file (if exists).
void DataFlowState::Load(const std::string &dir_path) {
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
void DataFlowState::Save(const std::string &dir_path) {
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

DataFlowState::~DataFlowState() {
  // Give some time for the previously sent records to get processed
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  std::shared_ptr<StopMessage> stop_msg = std::make_shared<StopMessage>();
  for (auto const &item1 : this->partition_chans_) {
    for (auto const &item2 : item1.second) {
      item2.second->Send(stop_msg);
    }
  }
  // Give some time for the stop message to propagate before joining partition
  // threads.
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  for (auto &thread : this->threads_) {
    thread.join();
  }
}

}  // namespace dataflow
}  // namespace pelton
