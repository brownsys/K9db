// Defines the state of our adapter.
//
// The state includes information about how the schema is sharded,
// as well as an in memory cache of user data, and which shards are
// already created.

#include "shards/state.h"

#include <stdlib.h>
#include <string.h>

#include <utility>

namespace shards {

// Initialization.
void SharderState::Initialize(const std::string &dir_path) {
  this->dir_path_ = dir_path;
  if (dir_path.back() != '/') {
    this->dir_path_ += "/";
  }
  this->executor_.Initialize(this->dir_path_);
}

// Schema manipulations.
void SharderState::AddShardKind(const ShardKind &kind, const ColumnName &pk) {
  this->kinds_.insert({kind, pk});
  this->kind_to_tables_.insert({kind, {}});
  this->shards_.insert({kind, {}});
}

void SharderState::AddUnshardedTable(const UnshardedTableName &table,
                                     const CreateStatement &create_str,
                                     const dataflow::Schema &schema) {
  this->concrete_schema_.insert({table, create_str});
  this->logical_schema_.insert({table, schema});
}

void SharderState::AddShardedTable(
    const UnshardedTableName &table,
    const ShardingInformation &sharding_information,
    const CreateStatement &sharded_create_statement,
    const dataflow::Schema &schema) {
  // Record that the shard kind contains this sharded table.
  this->kind_to_tables_.at(sharding_information.shard_kind)
      .push_back(sharding_information.sharded_table_name);
  // Map the unsharded name to its sharding information.
  this->sharded_by_[table].push_back(sharding_information);
  // Store the sharded schema.
  this->concrete_schema_.insert(
      {sharding_information.sharded_table_name, sharded_create_statement});
  this->logical_schema_.insert({table, schema});
}

std::list<CreateStatement> SharderState::CreateShard(
    const ShardKind &shard_kind, const UserId &user) {
  // Mark shard for this user as created!
  this->shards_.at(shard_kind).insert(user);
  // Return the create table statements.
  std::list<CreateStatement> result;
  for (const ShardedTableName &table : this->kind_to_tables_.at(shard_kind)) {
    result.push_back(this->concrete_schema_.at(table));
  }
  return result;
}

void SharderState::RemoveUserFromShard(const ShardKind &kind,
                                       const UserId &user_id) {
  this->shards_.at(kind).erase(user_id);
}

// Schema lookups.
bool SharderState::Exists(const UnshardedTableName &table) const {
  return this->concrete_schema_.count(table) > 0 ||
         this->sharded_by_.count(table) > 0;
}

CreateStatement SharderState::ConcreteSchemaOf(
    const ShardedTableName &table) const {
  return this->concrete_schema_.at(table);
}

const dataflow::Schema &SharderState::LogicalSchemaOf(
    const UnshardedTableName &table) const {
  return this->logical_schema_.at(table);
}

bool SharderState::IsSharded(const UnshardedTableName &table) const {
  return this->sharded_by_.count(table) == 1;
}

const std::list<ShardingInformation> &SharderState::GetShardingInformation(
    const UnshardedTableName &table) const {
  return this->sharded_by_.at(table);
}

bool SharderState::IsPII(const UnshardedTableName &table) const {
  return this->kinds_.count(table) > 0;
}

const ColumnName &SharderState::PkOfPII(const UnshardedTableName &table) const {
  return this->kinds_.at(table);
}

bool SharderState::ShardExists(const ShardKind &shard_kind,
                               const UserId &user) const {
  return this->shards_.at(shard_kind).count(user) > 0;
}

const std::unordered_set<UserId> &SharderState::UsersOfShard(
    const ShardKind &kind) const {
  return this->shards_.at(kind);
}

// SQL Executor.
sqlexecutor::SQLExecutor *SharderState::SQLExecutor() {
  return &this->executor_;
}

// Flows.
void SharderState::AddFlow(const FlowName &name,
                           const dataflow::DataFlowGraph &flow) {
  this->flows_.insert({name, flow});
  for (auto input : flow.inputs()) {
    this->inputs_[input->table_name()].push_back(input);
  }
}

const dataflow::DataFlowGraph &SharderState::GetFlow(
    const FlowName &name) const {
  return this->flows_.at(name);
}

bool SharderState::HasInputsFor(const UnshardedTableName &table_name) const {
  return this->inputs_.count(table_name) > 0;
}

const std::vector<std::shared_ptr<dataflow::InputOperator>>
    &SharderState::InputsFor(const UnshardedTableName &table_name) const {
  return this->inputs_.at(table_name);
}

}  // namespace shards
