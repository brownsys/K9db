// Defines the state of the dataflow system.
//
// The state includes the currently installed flows, including their operators
// and state.

#include "pelton/dataflow/state.h"

#include <fstream>
#include <utility>

#include "pelton/util/fs.h"

#define STATE_FILE_NAME ".dataflow.state"

namespace pelton {
namespace dataflow {

// Manage schemas.
void DataFlowState::AddTableSchema(const sqlast::CreateTable &create) {
  this->schema_.emplace(create.table_name(), create);
}
void DataFlowState::AddTableSchema(const std::string &table_name,
                                   SchemaOwner &&schema) {
  this->schema_.emplace(table_name, std::move(schema));
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
  return SchemaRef(this->schema_.at(table_name));
}

// Manage flows.
void DataFlowState::AddFlow(const FlowName &name, const DataFlowGraph &flow) {
  this->flows_.insert({name, flow});
  for (const auto &[input_name, input] : flow.inputs()) {
    this->flows_per_input_table_[input_name].push_back(name);
  }
}

const DataFlowGraph &DataFlowState::GetFlow(const FlowName &name) const {
  return this->flows_.at(name);
}

bool DataFlowState::HasFlow(const FlowName &name) const {
  return this->flows_.count(name) == 1;
}

bool DataFlowState::HasFlowsFor(const TableName &table_name) const {
  return this->flows_per_input_table_.count(table_name) == 1;
}

// Process raw records from sharder into flows.
bool DataFlowState::ProcessRawRecords(
    const std::vector<shards::RawRecord> &raw_records) {
  // Construct schema-full records.
  std::unordered_map<std::string, std::vector<Record>> records;
  for (shards::RawRecord raw : raw_records) {
    records[raw.table_name].push_back(this->CreateRecord(raw));
  }

  // Process them via flow.
  for (const auto &[table_name, rs] : records) {
    if (!this->ProcessRecords(table_name, rs)) {
      return false;
    }
  }
  return true;
}

bool DataFlowState::ProcessRecords(const TableName &table_name,
                                   const std::vector<Record> &records) {
  if (this->HasFlowsFor(table_name)) {
    for (const FlowName &name : this->flows_per_input_table_.at(table_name)) {
      DataFlowGraph &graph = this->flows_.at(name);
      if (!graph.Process(table_name, records)) {
        return false;
      }
    }
  }
  return true;
}

// Creating and processing records from raw data.
Record DataFlowState::CreateRecord(const shards::RawRecord &raw_record) const {
  SchemaRef schema = SchemaRef(this->schema_.at(raw_record.table_name));
  Record record{schema, raw_record.positive};
  for (size_t i = 0; i < raw_record.values.size(); i++) {
    size_t schema_index = i;
    if (raw_record.columns.size() > 0) {
      schema_index = schema.IndexOf(raw_record.columns.at(i));
    }
    record.SetValue(raw_record.values.at(i), schema_index);
  }
  return record;
}

// Load state from its durable file (if exists).
void DataFlowState::Load(const std::string &dir_path) {
  // State file does not exists: this is a fresh database that was not
  // created previously!
  std::string state_file_path = dir_path + STATE_FILE_NAME;
  if (dir_path == ":memory:" || !util::FileExists(state_file_path)) {
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
    this->schema_.emplace(std::piecewise_construct, std::forward_as_tuple(line),
                          std::forward_as_tuple(names, types, keys));

    // Next table.
    getline(state_file, line);
    getline(state_file, line);
  }

  // Done reading!
  state_file.close();
}

// Save state to durable file.
void DataFlowState::Save(const std::string &dir_path) {
  if (dir_path == ":memory:") {
    return;
  }

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

}  // namespace dataflow
}  // namespace pelton
