// This file contains logic to read the existing state of a sharded database,
// which was previously created, closed, and then later on re-opened.

// TODO(babman): save and load state from default DB instead of file.

#include <fstream>
#include <string>
#include <vector>

#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/fs.h"

#define STATE_FILE_NAME ".shards.state"

namespace pelton {
namespace shards {

void SharderState::Load(const std::string &dir_path) {
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
  getline(state_file, line);

  // Read schemas.
  while (line != "") {
    std::string serialized_schema;
    getline(state_file, serialized_schema);
    sqlast::SQLParser parser;
    auto ptr = std::move(parser.Parse(serialized_schema).value());
    sqlast::CreateTable *schema = static_cast<sqlast::CreateTable *>(ptr.get());
    this->schema_.insert({line, *schema});
    getline(state_file, line);
  }

  // Read kinds_.
  getline(state_file, line);
  while (line != "") {
    std::string pk;
    getline(state_file, pk);
    this->kinds_.insert({line, pk});
    getline(state_file, line);
  }

  // Read kind_to_tables_.
  getline(state_file, line);
  while (line != "") {
    this->kind_to_tables_.insert({line, {}});
    std::string table_name;
    getline(state_file, table_name);
    while (table_name != "") {
      this->kind_to_tables_.at(line).push_back(table_name);
      getline(state_file, table_name);
    }
    getline(state_file, line);
  }

  // Read kind_to_names_.
  getline(state_file, line);
  while (line != "") {
    this->kind_to_names_.insert({line, {}});
    std::string table_name;
    getline(state_file, table_name);
    while (table_name != "") {
      this->kind_to_names_.at(line).insert(table_name);
      getline(state_file, table_name);
    }
    getline(state_file, line);
  }

  // Read sharded_by_.
  getline(state_file, line);
  while (line != "") {
    this->sharded_by_.insert({line, {}});
    std::string shard_kind;
    getline(state_file, shard_kind);
    while (shard_kind != "") {
      ShardingInformation info;
      info.shard_kind = shard_kind;
      getline(state_file, info.sharded_table_name);
      getline(state_file, info.shard_by);
      state_file >> info.shard_by_index;
      getline(state_file, shard_kind);
      state_file >> info.distance_from_shard;
      getline(state_file, shard_kind);
      getline(state_file, info.next_table);
      getline(state_file, info.next_column);
      this->sharded_by_.at(line).push_back(info);
      getline(state_file, shard_kind);
    }
    getline(state_file, line);
  }

  // Read shards_.
  getline(state_file, line);
  while (line != "") {
    this->shards_.insert({line, {}});
    std::string user_id;
    getline(state_file, user_id);
    while (user_id != "") {
      this->shards_.at(line).insert(user_id);
      getline(state_file, user_id);
    }
    getline(state_file, line);
  }

  // Read sharded_schema_.
  getline(state_file, line);
  while (line != "") {
    std::string serialized_create_statement;
    getline(state_file, serialized_create_statement);
    sqlast::SQLParser parser;
    auto ptr = std::move(parser.Parse(serialized_create_statement).value());
    sqlast::CreateTable *stmt = static_cast<sqlast::CreateTable *>(ptr.get());
    this->sharded_schema_.insert({line, *stmt});
    getline(state_file, line);
  }

  // Read all create index statements.
  getline(state_file, line);
  while (line != "") {
    std::string serialized_create_index;
    getline(state_file, serialized_create_index);
    while (serialized_create_index != "") {
      sqlast::SQLParser parser;
      auto ptr = std::move(parser.Parse(serialized_create_index).value());
      sqlast::CreateIndex *stmt = static_cast<sqlast::CreateIndex *>(ptr.get());
      // TODO(babman): put this back in when flows are also loaded at restart.
      // this->create_index_[line].push_back(*stmt);
      getline(state_file, serialized_create_index);
    }
    getline(state_file, line);
  }

  // Read secondary indices.
  getline(state_file, line);
  while (line != "") {
    std::string column_name;
    std::string shard_by;
    std::string flow_name;
    getline(state_file, column_name);
    getline(state_file, shard_by);
    getline(state_file, flow_name);
    // TODO(babman): put this back in when flows are also loaded at restart.
    // this->indices_[line].insert(column_name);
    // this->index_to_flow_[line][column_name][shard_by] = flow_name;
    getline(state_file, line);
  }

  // Done reading!
  state_file.close();
}

void SharderState::Save(const std::string &dir_path) {
  // Open state file for writing.
  std::ofstream state_file;
  util::OpenWrite(&state_file, dir_path + STATE_FILE_NAME);

  // Begin writing.
  for (const auto &[table, schema] : this->schema_) {
    sqlast::Stringifier stringifier{""};
    state_file << table << "\n" << schema.Visit(&stringifier) << "\n";
  }
  state_file << "\n";

  for (const auto &[kind, pk] : this->kinds_) {
    state_file << kind << "\n" << pk << "\n";
  }
  state_file << "\n";

  for (const auto &[kind, table_names] : this->kind_to_tables_) {
    state_file << kind << "\n";
    for (const auto &table_name : table_names) {
      state_file << table_name << "\n";
    }
    state_file << "\n";
  }
  state_file << "\n";

  for (const auto &[kind, table_names] : this->kind_to_names_) {
    state_file << kind << "\n";
    for (const auto &table_name : table_names) {
      state_file << table_name << "\n";
    }
    state_file << "\n";
  }
  state_file << "\n";

  for (const auto &[table_name, infos] : this->sharded_by_) {
    state_file << table_name << "\n";
    for (const auto &info : infos) {
      state_file << info.shard_kind << "\n";
      state_file << info.sharded_table_name << "\n";
      state_file << info.shard_by << "\n";
      state_file << info.shard_by_index << "\n";
      state_file << info.distance_from_shard << "\n";
      state_file << info.next_table << "\n";
      state_file << info.next_column << "\n";
    }
    state_file << "\n";
  }
  state_file << "\n";

  for (const auto &[kind, users] : this->shards_) {
    state_file << kind << "\n";
    for (const auto &user : users) {
      state_file << user << "\n";
    }
    state_file << "\n";
  }
  state_file << "\n";

  for (const auto &[table_name, create_statement] : this->sharded_schema_) {
    sqlast::Stringifier stringifier{""};
    state_file << table_name << "\n"
               << create_statement.Visit(&stringifier) << "\n";
  }
  state_file << "\n";

  for (const auto &[shard_kind, index_vec] : this->create_index_) {
    state_file << shard_kind << "\n";
    for (const auto &create_index : index_vec) {
      sqlast::Stringifier stringifier{""};
      state_file << create_index.Visit(&stringifier) << "\n";
    }
    state_file << "\n";
  }
  state_file << "\n";

  for (const auto &[table_name, col_to_ind] : this->index_to_flow_) {
    for (const auto &[column_name, shard_to_ind] : col_to_ind) {
      for (const auto &[shard_by, index_flow] : shard_to_ind) {
        state_file << table_name << "\n"
                   << column_name << "\n"
                   << shard_by << "\n"
                   << index_flow << "\n";
      }
    }
  }
  state_file << "\n";

  // Done writing.
  state_file.close();
}

}  // namespace shards
}  // namespace pelton
