#include "k9db/policy/policy_engine.h"

#include <memory>
#include <utility>

#include "glog/logging.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"
#include "k9db/policy/abstract_policy.h"
#include "k9db/policy/policy_builder.h"
#include "k9db/policy/policy_state.h"
#include "k9db/shards/state.h"
#include "k9db/shards/types.h"
#include "k9db/sqlast/ast_policy.h"
#include "k9db/sqlast/ast_value.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace policy {

void AddPolicy(const std::string &stmt, Connection *connection) {
  // 0. Acquire global writer lock: this is a schema operation; this is ok!
  util::UniqueLock lock = connection->state->WriterLock();
  shards::SharderState &state = connection->state->SharderState();
  dataflow::DataFlowState &dstate = connection->state->DataflowState();

  // 1. Parse stmt
  sqlast::PolicyStatement parsed = sqlast::PolicyStatement::Parse(stmt);

  // 2. Create any views
  PolicyBuilder builder(parsed.schema());
  builder.MakeFlows(connection, &lock);

  // 3. Add to table.
  CHECK(state.TableExists(parsed.table()));
  shards::Table &table = state.GetTable(parsed.table());
  table.policy_state.AddPolicy(parsed.column(), std::move(builder));
}

void MakePolicies(const std::string &table_name, Connection *connection,
                  std::vector<dataflow::Record> *records) {
  // Get the states
  const shards::SharderState &state = connection->state->SharderState();
  const dataflow::DataFlowState &dstate = connection->state->DataflowState();

  // Look up table and the policy builder.
  const shards::Table &table = state.GetTable(table_name);
  const PolicyState &pstate = table.policy_state;

  // Add policies to every record.
  for (const auto &[column, policy_builder] : pstate.map()) {
    for (dataflow::Record &record : *records) {
      record.SetPolicy(column, policy_builder.MakePolicy(record, dstate));
    }
  }
}

std::vector<dataflow::Record> SerializePolicies(
    const std::vector<dataflow::Record> &records) {
  // Find new schema.
  const dataflow::Record &record = records.at(0);
  const dataflow::SchemaRef &schema = record.schema();

  std::vector<std::string> names = schema.column_names();
  std::vector<sqlast::ColumnDefinition::Type> types = schema.column_types();
  const std::vector<dataflow::ColumnID> &keys = schema.keys();
  const auto &policies = record.policies();
  for (size_t i = 0; i < policies.size(); i++) {
    const auto &policy = policies.at(i);
    if (policy != nullptr) {
      for (std::string &column : policy->ColumnNames(schema.NameOf(i))) {
        names.push_back(std::move(column));
        types.push_back(sqlast::ColumnDefinition::Type::TEXT);
      }
    }
  }

  // Output schema.
  dataflow::SchemaRef out_schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Serialize policies of each row into new schema.
  std::vector<dataflow::Record> output;
  output.reserve(records.size());
  for (size_t i = 0; i < records.size(); i++) {
    const dataflow::Record &record = records.at(i);
    dataflow::Record new_record(out_schema, record.IsPositive());
    // Add column values.
    for (size_t i = 0; i < schema.size(); i++) {
      new_record.SetValue(record.GetValue(i), i);
    }
    // Add policy serialized values.
    size_t j = schema.size();
    for (const auto &policy : record.policies()) {
      if (policy != nullptr) {
        for (std::string &column : policy->SerializeValues()) {
          new_record.SetString(std::make_unique<std::string>(std::move(column)),
                               j++);
        }
      }
    }

    output.push_back(std::move(new_record));
  }

  return output;
}

}  // namespace policy
}  // namespace k9db
