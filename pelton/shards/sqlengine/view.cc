// Creation and management of dataflows.
#include "pelton/shards/sqlengine/view.h"

#include <cstring>
#include <iterator>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/value.h"
#include "pelton/dp/dp_util.h"
#include "pelton/planner/planner.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

namespace {

using Constraint = std::vector<std::pair<size_t, std::string>>;

bool SetEqual(const std::vector<uint32_t> &cols, const Constraint &constraint) {
  for (const auto &[col1, _] : constraint) {
    bool found = false;
    for (uint32_t col2 : cols) {
      if (col1 == col2) {
        found = true;
        break;
      }
    }
    if (!found) {
      return false;
    }
  }
  return true;
}

struct LookupCondition {
  std::optional<dataflow::Record> greater_record;
  std::optional<dataflow::Key> greater_key;
  std::optional<std::vector<dataflow::Key>> equality_keys;
};

std::vector<dataflow::Record> LookupRecords(const dataflow::DataFlowGraph &flow,
                                            const LookupCondition &condition,
                                            int limit, size_t offset) {
  if (limit == -1) {
    limit = flow.GetPartition(0)->outputs().at(0)->limit();
  }
  if (offset == 0) {
    offset = flow.GetPartition(0)->outputs().at(0)->offset();
  }
  // By ordered key.
  if (condition.greater_key) {
    return flow.LookupKeyGreater(*condition.greater_key, limit, offset);
  }
  if (condition.greater_record) {
    if (!condition.equality_keys) {
      return flow.AllRecordGreater(*condition.greater_record, limit, offset);
    } else {
      return flow.LookupRecordGreater(*condition.equality_keys,
                                      *condition.greater_record, limit, offset);
    }
  }
  if (condition.equality_keys) {
    return flow.Lookup(*condition.equality_keys, limit, offset);
  }
  return flow.All(limit, offset);
}

// Constructing a comparator record from values.
dataflow::Record MakeRecord(const Constraint &constraint,
                            const dataflow::SchemaRef &schema) {
  dataflow::Record record{schema, true};
  for (const auto &[index, value] : constraint) {
    record.SetValue(value, index);
  }
  return record;
}
// Constructing keys from values.
dataflow::Key MakeKey(const Constraint &constraint,
                      const std::vector<uint32_t> &key_cols,
                      const dataflow::SchemaRef &schema) {
  dataflow::Key key(key_cols.size());
  for (const auto &[index, value] : constraint) {
    if (value == "NULL") {
      key.AddNull(schema.TypeOf(index));
    } else {
      switch (schema.TypeOf(index)) {
        case sqlast::ColumnDefinition::Type::UINT:
          key.AddValue(static_cast<uint64_t>(std::stoull(value)));
          break;
        case sqlast::ColumnDefinition::Type::INT:
          key.AddValue(static_cast<int64_t>(std::stoll(value)));
          break;
        case sqlast::ColumnDefinition::Type::TEXT:
        case sqlast::ColumnDefinition::Type::DATETIME:
          key.AddValue(dataflow::Value::Dequote(value));
          break;
        default:
          LOG(FATAL) << "Unsupported type in view read";
      }
    }
  }
  return key;
}

// sqlast casts and extractions.
const sqlast::ColumnExpression *ExtractColumnFromConstraint(
    const sqlast::BinaryExpression *expr) {
  if (expr->GetLeft()->type() == expr->GetRight()->type()) {
    LOG(FATAL) << "Binary expression left and right have same type!";
  }
  if (expr->GetLeft()->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(expr->GetLeft());
  } else if (expr->GetRight()->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(expr->GetRight());
  } else {
    LOG(FATAL) << "No column in binary expression!";
  }
}
const sqlast::LiteralExpression *ExtractLiteralFromConstraint(
    const sqlast::BinaryExpression *expr) {
  if (expr->GetLeft()->type() == expr->GetRight()->type()) {
    LOG(FATAL) << "Binary expression left and right have same type!";
  }
  if (expr->GetLeft()->type() == sqlast::Expression::Type::LITERAL) {
    return static_cast<const sqlast::LiteralExpression *>(expr->GetLeft());
  } else if (expr->GetRight()->type() == sqlast::Expression::Type::LITERAL) {
    return static_cast<const sqlast::LiteralExpression *>(expr->GetRight());
  } else {
    LOG(FATAL) << "No literal in binary expression!";
  }
}

// Constraint extractions.
Constraint FindGreaterThanConstraints(const dataflow::DataFlowGraph &flow,
                                      const sqlast::BinaryExpression *expr) {
  Constraint result;
  switch (expr->type()) {
    // Make sure Greater than makes sense.
    case sqlast::Expression::Type::GREATER_THAN: {
      if (expr->GetLeft()->type() != sqlast::Expression::Type::COLUMN) {
        LOG(FATAL) << "Bad greater than in view select";
      }
      const auto &column = ExtractColumnFromConstraint(expr)->column();
      size_t col = flow.output_schema().IndexOf(column);
      const std::string &val = ExtractLiteralFromConstraint(expr)->value();
      result.emplace_back(col, val);
      break;
    }
    // Check sub expressions of AND.
    case sqlast::Expression::Type::AND: {
      auto l = static_cast<const sqlast::BinaryExpression *>(expr->GetLeft());
      auto r = static_cast<const sqlast::BinaryExpression *>(expr->GetRight());
      auto c1 = FindGreaterThanConstraints(flow, l);
      auto c2 = FindGreaterThanConstraints(flow, r);
      result.insert(result.end(), std::make_move_iterator(c1.begin()),
                    std::make_move_iterator(c1.end()));
      result.insert(result.end(), std::make_move_iterator(c2.begin()),
                    std::make_move_iterator(c2.end()));
      break;
    }
    // Do nothing for EQ or IN.
    case sqlast::Expression::Type::EQ:
      break;
    case sqlast::Expression::Type::IN:
      break;
    // LITERAL, COLUMN, LIST.
    default:
      LOG(FATAL) << "Bad where condition in view select";
  }
  return result;
}

std::vector<Constraint> FindEqualityConstraints(
    const dataflow::DataFlowGraph &flow, const sqlast::BinaryExpression *expr) {
  std::vector<Constraint> result;
  switch (expr->type()) {
    // EQ: easy, one constraint!
    case sqlast::Expression::Type::EQ: {
      const auto &column = ExtractColumnFromConstraint(expr)->column();
      size_t col = flow.output_schema().IndexOf(column);
      const std::string &val = ExtractLiteralFromConstraint(expr)->value();
      result.push_back({std::make_pair(col, val)});
      break;
    }
    // IN: each value is a separate OR constraint!
    case sqlast::Expression::Type::IN: {
      const auto &column = ExtractColumnFromConstraint(expr)->column();
      size_t col = flow.output_schema().IndexOf(column);
      auto l =
          static_cast<const sqlast::LiteralListExpression *>(expr->GetRight());
      for (const std::string &val : l->values()) {
        result.push_back({std::make_pair(col, val)});
      }
      break;
    }

    // Combine AND conditions.
    case sqlast::Expression::Type::AND: {
      auto l = static_cast<const sqlast::BinaryExpression *>(expr->GetLeft());
      auto r = static_cast<const sqlast::BinaryExpression *>(expr->GetRight());
      auto v1 = FindEqualityConstraints(flow, l);
      auto v2 = FindEqualityConstraints(flow, r);
      for (const Constraint &c1 : v1) {
        for (const Constraint &c2 : v2) {
          Constraint combined;
          combined.reserve(c1.size() + c2.size());
          combined.insert(combined.end(), std::make_move_iterator(c1.begin()),
                          std::make_move_iterator(c1.end()));
          combined.insert(combined.end(), std::make_move_iterator(c2.begin()),
                          std::make_move_iterator(c2.end()));
          result.push_back(std::move(combined));
        }
      }
      break;
    }

    // Do nothing for GREATER_THAN.
    case sqlast::Expression::Type::GREATER_THAN:
      break;
    // LITERAL, COLUMN, LIST.
    default:
      LOG(FATAL) << "Bad where condition in view select";
  }
  return result;
}

// Find the different components of the WHERE condition.
LookupCondition ConstraintKeys(const dataflow::DataFlowGraph &flow,
                               const sqlast::BinaryExpression *where) {
  LookupCondition condition;
  if (where == nullptr) {
    return condition;
  }

  const dataflow::SchemaRef &schema = flow.output_schema();
  const std::vector<uint32_t> &key_cols = flow.matview_keys();

  Constraint greater = FindGreaterThanConstraints(flow, where);
  if (greater.size() > 0) {
    if (SetEqual(key_cols, greater)) {
      condition.greater_key = MakeKey(greater, key_cols, schema);
    } else {
      condition.greater_record = MakeRecord(greater, schema);
    }
  }

  std::vector<Constraint> equality = FindEqualityConstraints(flow, where);
  if (equality.size() > 0) {
    condition.equality_keys = std::vector<dataflow::Key>();
    for (const Constraint &constraint : equality) {
      condition.equality_keys->push_back(MakeKey(constraint, key_cols, schema));
    }
  }

  // Special pattern: ... WHERE key = <v> AND key > <x>.
  if (condition.equality_keys && condition.greater_key) {
    LOG(FATAL) << "Where clause has equality and greater conditions on key";
  }

  return condition;
}

}  // namespace

absl::Status CreateView(const sqlast::CreateView &stmt, Connection *connection,
                        UniqueLock *lock) {
  // Try to extract DP options from the release view name
  std::optional<pelton::dp::DPOptions> options_check = pelton::dp::ParseViewForDP(stmt.view_name());
  const bool is_dp = options_check.has_value();
  pelton::dp::DPOptions options = options_check.value_or(pelton::dp::DPOptions());

  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  shards::SharderState *state = connection->state->sharder_state();

  // Plan the query using calcite and generate a concrete graph for it.
  std::unique_ptr<dataflow::DataFlowGraphPartition> graph =
      planner::PlanGraph(dataflow_state, stmt.query());

  // For DP: save the flow input keys for later
  std::vector<std::string> flow_input_keys;
  if (is_dp) {
    auto graph_inputs = graph->inputs();
    flow_input_keys.reserve(graph_inputs.size());
    for (auto kv : graph_inputs) {
      flow_input_keys.push_back(kv.first);
    }
  }

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  std::string flow_name = stmt.view_name();
  dataflow_state->AddFlow(flow_name, std::move(graph));

  // Update new View with existing data in tables.
  std::vector<std::pair<std::string, std::vector<dataflow::Record>>> data;

  // Iterate through each table in the new View's flow.
  dataflow::Future future(true);
  for (const auto &table_name : dataflow_state->GetFlow(flow_name).Inputs()) {
    if (is_dp) {
      // This is a DP View, so it is neccesary to check to see if source tables are
      // budgeted. Do this by getting the name of the tracker table and checking to
      // see if that table exists in the database.
      std::string tracker_name = pelton::dp::tracker_name(table_name);
      if (state->Exists(tracker_name)) {
        // A tracker for the current table exists, implying the table is budgeted.

        // Generate a query to select the epsilon values from the tracker
        const std::string tracker_select_query =
            pelton::dp::tracker_select_query(tracker_name);

        // Parse that query string into a sqlast::Select
        sqlast::SQLParser parser;
        absl::StatusOr<std::unique_ptr<sqlast::AbstractStatement> >
            tracker_statement = parser.Parse(tracker_select_query);
        auto *stmt_tracker =
            static_cast<sqlast::Select *>(tracker_statement->get());

        // Get the SchemaRef for the tracker select query
        dataflow::SchemaRef schema = select::ResultSchemaTracker(
            *stmt_tracker, dataflow_state->GetTableSchema(tracker_name));
        
        // Execute the selection query, get the resultant records
        sql::SqlResult result(schema);
        auto &exec = connection->executor;
        result = exec.Default(stmt_tracker, schema);
        std::vector<pelton::dataflow::Record> records =
            result.ResultSets().at(0).Vec();
        
        // Iterate through the resultant records to calculate the sum of the `e`
        // column of the tracker, representing the sum of the epsilon values of
        // all previous DP queries made against the table the tracker manages
        // the budget for.
        int spent = 0;
        for (auto &record : records) {
          spent += record.GetInt(schema.IndexOf("e"));
        }

        // Get the initial budget of the table, based on the table name
        const double initial_budget =
            pelton::dp::ParseTableForDP(table_name).value();
        
        // Calculate the remaining budget
        const int remaining_budget = (initial_budget) - ((double)spent);

        // Check if there's remaining budget available for the DP release
        // view creation
        if (options.epsilon.value() <= remaining_budget) {
          // There's enough budget from this source table to continue with
          // the DP release

          // Generate a query string to update the tracker table
          const std::string tracker_insert_query = pelton::dp::tracker_insert_query(tracker_name, std::to_string((int)options.epsilon.value()), stmt.view_name(), stmt.query());

          // Parse that query string into a sqlast::Insert
          absl::StatusOr<std::unique_ptr<sqlast::AbstractStatement> >
              tracker_insert = parser.Parse(tracker_insert_query);
          auto *stmt_insert =
              static_cast<sqlast::Insert *>(tracker_insert->get());

          // Execute the insertion. The result is not useful.
          // It is safe to execute here, because if later tables have spent
          // their budgets, the current behavior is a fatal error. If this is
          // changed later, tracker insert statements will need to be
          // genereated and saved for all relevant tables first, and then
          // executed once it has been verified that no table has blown its
          // budget and once the view creation has succeeded without failure
          // (a bad view creation query will also trigger a fatal error at
          // present, so this is also not implemented).
          sql::SqlResult result(schema);
          exec.Default(stmt_insert, schema);
        } else {
          LOG(FATAL)
              << "Privacy Budget for budgeted table " + table_name +
                    " has been depleted from " + std::to_string(initial_budget) +
                    " to " + std::to_string(remaining_budget) +
                    ", which is less than the epsilon value of " +
                    std::to_string(options.epsilon.value()) +
                    " specified for differentially private release view " +
                    stmt.view_name() + " with query '" + stmt.query() +
                    "'. A history of budget expenditures is available in "
                    "table " +
                    tracker_name + ".";
        }
      } else {
        LOG(INFO) << "Differentially private release " + stmt.view_name() + " with query " + stmt.query() + " uses table " + table_name + ", which is unbudgeted.";
      }
    }

    // Generate and execute select * query for current table
    pelton::sqlast::Select select{table_name};
    select.AddColumn("*");
    MOVE_OR_RETURN(sql::SqlResult result,
                   select::Shard(select, connection, false));

    // Vectorize and store records.
    std::vector<pelton::dataflow::Record> records =
        result.ResultSets().at(0).Vec();

    // Records will be negative here, need to turn them to positive records.
    for (auto &record : records) {
      record.SetPositive(true);
    }

    dataflow_state->ProcessRecordsByFlowName(
        flow_name, table_name, std::move(records), future.GetPromise());
  }

  // If this is a DP view, then this flow needs to be removed for each of its
  // sources now that the initial view state has been created. This means that
  // future updates to source tables will NOT be passed to update the DP view,
  // so the view will effectively be static.
  if (is_dp) {
    dataflow_state->RemoveFlow(flow_name, flow_input_keys);
  }

  future.Wait();

  return absl::OkStatus();
}

absl::StatusOr<sql::SqlResult> SelectView(const sqlast::Select &stmt,
                                          Connection *connection) {
  sqlast::ListCountVisitor listcount;
  std::string count = std::to_string(listcount.Visit(&stmt));
  SharderState *state = connection->state->sharder_state();
  dataflow::DataFlowState *dataflow_state = connection->state->dataflow_state();
  SharedLock lock = state->ReaderLock();

  // Get the corresponding flow.
  const std::string &view_name = stmt.table_name();
  const dataflow::DataFlowGraph &flow = dataflow_state->GetFlow(view_name);

  // Transform WHERE statement to conditions on matview keys.
  LookupCondition condition = ConstraintKeys(flow, stmt.GetWhereClause());
  std::vector<dataflow::Record> records =
      LookupRecords(flow, condition, stmt.limit(), stmt.offset());

  return sql::SqlResult(
      sql::SqlResultSet(flow.output_schema(), {std::move(records), {}}));
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
