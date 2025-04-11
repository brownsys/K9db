// Creation and management of dataflows.
#include "k9db/shards/sqlengine/view.h"

#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "k9db/dataflow/future.h"
#include "k9db/dataflow/graph.h"
#include "k9db/dataflow/graph_partition.h"
#include "k9db/dataflow/key.h"
#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/ops/forward_view.h"
#include "k9db/dataflow/ops/matview.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"
#include "k9db/planner/planner.h"
#include "k9db/sql/connection.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {
namespace view {

/*
 * View creation.
 */

absl::StatusOr<sql::SqlResult> CreateView(const sqlast::CreateView &stmt,
                                          Connection *connection,
                                          util::UniqueLock *lock) {
  std::string flow_name = stmt.view_name();

  // Make sure a table or a view with the same name does not exist.
  const SharderState &sstate = connection->state->SharderState();
  dataflow::DataFlowState &dataflow_state = connection->state->DataflowState();
  ASSERT_RET(!sstate.TableExists(flow_name), InvalidArgument,
             "Table with the same name exists!");
  ASSERT_RET(!dataflow_state.HasFlow(flow_name), InvalidArgument,
             "View with the same name exists!");

  // Plan the query using calcite and generate a concrete graph for it.
  std::unique_ptr<dataflow::DataFlowGraphPartition> graph =
      planner::PlanGraph(&dataflow_state, stmt.query());

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  dataflow_state.AddFlow(flow_name, std::move(graph));

  // Update new View with existing data in tables.
  std::vector<std::pair<std::string, std::vector<dataflow::Record>>> data;

  // Begin transaction (for reading tables content).
  connection->session->BeginTransaction(false);

  // Iterate through each table in the new View's flow.
  dataflow::Future future(true);
  for (const auto &table_name : dataflow_state.GetFlow(flow_name).Inputs()) {
    // Generate and execute select * query for current table
    sql::SqlResultSet result = connection->session->GetAll(table_name);

    // Vectorize and store records.
    std::vector<k9db::dataflow::Record> records = std::move(result.Vec());

    // Records will be negative here, need to turn them to positive records.
    for (auto &record : records) {
      record.SetPositive(true);
    }

    // Assign records their policy.
    // TODO(babman): can infinite loop when dealing with creating a new
    // view for a policy expression or when replaying creation after a
    // K9db restart.
    // policy::MakePolicies(table_name, connection, &records);

    dataflow_state.ProcessRecordsByFlowName(
        flow_name, table_name, std::move(records), future.GetPromise());
  }

  // Populate nested view upon creation.
  for (size_t p = 0; p < dataflow_state.workers(); p++) {
    k9db::dataflow::DataFlowGraphPartition *partition =
        dataflow_state.GetFlow(flow_name).GetPartition(p);
    for (auto *forward : partition->forwards()) {
      dataflow::Operator *node = forward->parents().at(0);
      dataflow::MatViewOperator *parent =
          reinterpret_cast<dataflow::MatViewOperator *>(node);
      forward->ProcessAndForward(parent->index(), parent->All(),
                                 future.GetPromise());
    }
  }

  future.Wait();

  // Nothing to commit.
  connection->session->RollbackTransaction();
  return sql::SqlResult(true);
}

/*
 * View querying.
 */

namespace {

using Constraint = std::vector<std::pair<size_t, sqlast::Value>>;

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
dataflow::Record MakeRecord(Constraint *constraint,
                            const dataflow::SchemaRef &schema) {
  dataflow::Record record{schema, true};
  for (auto &[index, value] : *constraint) {
    value.ConvertTo(schema.TypeOf(index));
    record.SetValue(value, index);
  }
  return record;
}
// Constructing keys from values.
dataflow::Key MakeKey(Constraint *constraint,
                      const std::vector<uint32_t> &key_cols,
                      const dataflow::SchemaRef &schema) {
  dataflow::Key key(key_cols.size());
  for (uint32_t column_id : key_cols) {
    for (auto &[index, value] : *constraint) {
      if (index == column_id) {
        value.ConvertTo(schema.TypeOf(index));
        key.AddValue(value);
        break;
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
      const sqlast::Value &val = ExtractLiteralFromConstraint(expr)->value();
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
      const sqlast::Value &val = ExtractLiteralFromConstraint(expr)->value();
      result.push_back({std::make_pair(col, val)});
      break;
    }
    // IN: each value is a separate OR constraint!
    case sqlast::Expression::Type::IN: {
      const auto &column = ExtractColumnFromConstraint(expr)->column();
      size_t col = flow.output_schema().IndexOf(column);
      auto l =
          static_cast<const sqlast::LiteralListExpression *>(expr->GetRight());
      for (const sqlast::Value &val : l->values()) {
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
      condition.greater_key = MakeKey(&greater, key_cols, schema);
    } else {
      condition.greater_record = MakeRecord(&greater, schema);
    }
  }

  std::vector<Constraint> equality = FindEqualityConstraints(flow, where);
  if (equality.size() > 0) {
    condition.equality_keys = std::vector<dataflow::Key>();
    for (Constraint &constraint : equality) {
      condition.equality_keys->push_back(
          MakeKey(&constraint, key_cols, schema));
    }
  }

  // Special pattern: ... WHERE key = <v> AND key > <x>.
  if (condition.equality_keys && condition.greater_key) {
    LOG(FATAL) << "Where clause has equality and greater conditions on key";
  }

  return condition;
}

}  // namespace

absl::StatusOr<sql::SqlResult> SelectView(const sqlast::Select &stmt,
                                          Connection *connection,
                                          util::SharedLock *lock) {
  const dataflow::DataFlowState &dstate = connection->state->DataflowState();

  // Get the corresponding flow.
  const std::string &view_name = stmt.table_name();
  const dataflow::DataFlowGraph &flow = dstate.GetFlow(view_name);

  // Transform WHERE statement to conditions on matview keys.
  LookupCondition condition = ConstraintKeys(flow, stmt.GetWhereClause());

  // Read from view.
  std::vector<dataflow::Record> records =
      LookupRecords(flow, condition, stmt.limit(), stmt.offset());

  return sql::SqlResult(sql::SqlResultSet(view_name, flow.output_schema(),
                                          {std::move(records), {}}));
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
