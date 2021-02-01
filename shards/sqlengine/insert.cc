// INSERT statements sharding and rewriting.

#include "shards/sqlengine/insert.h"

#include <algorithm>
#include <cassert>
#include <list>
#include <vector>

#include "absl/strings/match.h"
#include "dataflow/record.h"
#include "dataflow/schema.h"
#include "shards/sqlengine/util.h"

namespace shards {
namespace sqlengine {
namespace insert {

absl::StatusOr<std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>>>
Rewrite(const sqlast::Insert &stmt, SharderState *state) {
  // Make sure table exists in the schema first.
  const std::string &table_name = stmt.table_name();
  if (!state->Exists(table_name)) {
    throw "Table does not exist!";
  }

  // Turn inserted values into a record and process it via corresponding flows.
  // TODO(babman): schema lifetime should be improved.
  // TODO(babman): this should only be executed after physical insert is
  //               successfull.
  const sqlast::CreateTable &create = state->LogicalSchemaOf(table_name);
  std::vector<dataflow::DataType> types;
  for (const auto &col : create.GetColumns()) {
    if (absl::EqualsIgnoreCase(col.column_type(), "int")) {
      types.push_back(dataflow::DataType::kInt);
    } else if (absl::StartsWithIgnoreCase(col.column_type(), "varchar")) {
      types.push_back(dataflow::DataType::kText);
    } else {
      throw "Unsupported datatype!";
    }
  }

  dataflow::Schema *schema = new dataflow::Schema(types);
  schema->set_key_columns({0});

  std::vector<dataflow::Record> records;
  records.emplace_back(*schema, true);
  if (stmt.HasColumns()) {
    const std::vector<std::string> &cols = stmt.GetColumns();
    const std::vector<std::string> &values = stmt.GetValues();
    for (const auto &col : create.GetColumns()) {
      auto it = std::find(cols.cbegin(), cols.cend(), col.column_name());
      size_t i = std::distance(cols.cbegin(), it);
      if (schema->TypeOf(i) == dataflow::DataType::kInt) {
        int64_t v = std::stoll(values.at(i));
        records.at(0).set_int(i, v);
      } else {
        std::string *v = new std::string(values.at(i));
        records.at(0).set_string(i, v);
      }
    }
  } else {
    const std::vector<std::string> &values = stmt.GetValues();
    assert(values.size() == schema->num_columns());
    for (size_t i = 0; i < values.size(); i++) {
      if (schema->TypeOf(i) == dataflow::DataType::kInt) {
        int64_t v = std::stoll(values.at(i));
        records.at(0).set_int(i, v);
      } else {
        std::string *v = new std::string(values.at(i));
        records.at(0).set_string(i, v);
      }
    }
  }

  if (state->HasInputsFor(table_name)) {
    for (auto input : state->InputsFor(table_name)) {
      input->ProcessAndForward(records);
    }
  }

  // Shard the insert statement so it is executable against the physical 
  // sharded database.
  sqlast::Stringifier stringifier;
  std::list<std::unique_ptr<sqlexecutor::ExecutableStatement>> result;

  // Case 1: table is not in any shard.
  bool is_sharded = state->IsSharded(table_name);
  if (!is_sharded) {
    // The insertion statement is unmodified.
    std::string insert_str = stmt.Visit(&stringifier);
    result.push_back(std::make_unique<sqlexecutor::SimpleExecutableStatement>(
        DEFAULT_SHARD_NAME, insert_str));
  }

  // Case 2: table is sharded!
  if (is_sharded) {
    // Duplicate the value for every shard this table has.
    for (const ShardingInformation &sharding_info :
         state->GetShardingInformation(table_name)) {
      sqlast::Insert cloned = stmt;
      cloned.table_name() = sharding_info.sharded_table_name;
      // Find the value corresponding to the shard by column.
      std::string value;
      if (cloned.HasColumns()) {
        value = cloned.RemoveValue(sharding_info.shard_by);
      } else {
        value = cloned.RemoveValue(sharding_info.shard_by_index);
      }

      // TODO(babman): better to do this after user insert rather than user data
      //               insert.
      std::string shard_name = NameShard(sharding_info.shard_kind, value);
      if (!state->ShardExists(sharding_info.shard_kind, value)) {
        for (auto create_stmt :
             state->CreateShard(sharding_info.shard_kind, value)) {
          result.push_back(
              std::make_unique<sqlexecutor::SimpleExecutableStatement>(
                  shard_name, create_stmt));
        }
      }

      // Add the modified insert statement.
      std::string insert_str = cloned.Visit(&stringifier);
      result.push_back(std::make_unique<sqlexecutor::SimpleExecutableStatement>(
          shard_name, insert_str));
    }
  }
  return result;
}

}  // namespace insert
}  // namespace sqlengine
}  // namespace shards
