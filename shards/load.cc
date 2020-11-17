// This file contains logic to read the existing state of a sharded database,
// which was previously created, closed, and then later on re-opened.

#include <sys/stat.h>

#include <fstream>
#include <string>

#include "shards/state.h"

namespace shards {

#define STATE_FILE_NAME ".state.txt";

namespace {

bool FileExists(const std::string &file) {
  struct stat buffer;
  return stat(file.c_str(), &buffer) == 0;
}

}  // namespace

void SharderState::Load() {
  // State file does not exists: this is a fresh database that was not
  // created previously!
  std::string state_file_path = this->dir_path_ + STATE_FILE_NAME;
  if (!FileExists(state_file_path)) {
    return;
  }

  // Read content of state file and fill them into state!
  std::ifstream state_file;
  state_file.open(state_file_path, std::ios::in);
  std::string line;
  getline(state_file, line);

  // Read kinds_.
  while (line != "") {
    std::string pk;
    getline(state_file, pk);
    this->kinds_.insert({line, pk});
    getline(state_file, line);
  }

  // Read kind_to_tables.
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
      this->sharded_by_.at(line).push_back(info);
      getline(state_file, shard_kind);
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

  // Read schema_.
  getline(state_file, line);
  while (line != "") {
    std::string create_statement;
    getline(state_file, create_statement);
    this->schema_.insert({line, create_statement});
    getline(state_file, line);
  }

  // Done reading!
  state_file.close();
}

void SharderState::Save() {
  // Open state file for writing.
  std::string state_file_path = this->dir_path_ + STATE_FILE_NAME;
  std::ofstream state_file;
  state_file.open(state_file_path, std::ios::out | std::ios::trunc);

  // Begin writing.
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

  for (const auto &[table_name, infos] : this->sharded_by_) {
    state_file << table_name << "\n";
    for (const auto &info : infos) {
      state_file << info.shard_kind << "\n";
      state_file << info.sharded_table_name << "\n";
      state_file << info.shard_by << "\n";
      state_file << info.shard_by_index << "\n";
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

  for (const auto &[table_name, create_statement] : this->schema_) {
    state_file << table_name << "\n" << create_statement << "\n";
  }
  state_file << "\n";

  // Done writing.
  state_file.close();
}

}  // namespace shards