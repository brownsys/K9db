// Creation and management of dataflows.

#include "pelton/shards/sqlengine/view.h"

#include <cstring>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/value.h"
#include "pelton/planner/planner.h"
#include "pelton/util/perf.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

namespace {

absl::StatusOr<std::string> GetColumnName(const sqlast::Expression *exp) {
  if (exp->type() != sqlast::Expression::Type::EQ &&
      exp->type() != sqlast::Expression::Type::IN &&
      exp->type() != sqlast::Expression::Type::GREATER_THAN) {
    return absl::InvalidArgumentError("Expected binary expression");
  }

  const sqlast::BinaryExpression *bin =
      static_cast<const sqlast::BinaryExpression *>(exp);
  const sqlast::Expression *left = bin->GetLeft();
  const sqlast::Expression *right = bin->GetRight();
  if (left->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(left)->column();
  }
  if (right->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(right)->column();
  }
  return absl::InvalidArgumentError("Invalid view where clause: no column!");
}

absl::StatusOr<std::vector<std::string>> GetCondValues(
    const sqlast::Expression *exp) {
  if (exp->type() != sqlast::Expression::Type::EQ &&
      exp->type() != sqlast::Expression::Type::IN &&
      exp->type() != sqlast::Expression::Type::GREATER_THAN) {
    return absl::InvalidArgumentError("Expected binary expression");
  }

  const sqlast::BinaryExpression *bin =
      static_cast<const sqlast::BinaryExpression *>(exp);
  const sqlast::Expression *left = bin->GetLeft();
  const sqlast::Expression *right = bin->GetRight();
  if (left->type() == sqlast::Expression::Type::LITERAL) {
    return std::vector<std::string>{
        static_cast<const sqlast::LiteralExpression *>(left)->value()};
  }
  if (right->type() == sqlast::Expression::Type::LITERAL) {
    return std::vector<std::string>{
        static_cast<const sqlast::LiteralExpression *>(right)->value()};
  }
  if (right->type() == sqlast::Expression::Type::LIST) {
    return static_cast<const sqlast::LiteralListExpression *>(right)->values();
  }
  return absl::InvalidArgumentError("Invalid view where clause: no literal!");
}

absl::Status ExtractConstraintsFromAnd(
    const sqlast::Expression *exp, dataflow::SchemaRef schema,
    std::unordered_map<dataflow::ColumnID, std::vector<std::string>> *cnstrs) {
  const sqlast::BinaryExpression *bin =
      static_cast<const sqlast::BinaryExpression *>(exp);
  const sqlast::Expression *left = bin->GetLeft();
  const sqlast::Expression *right = bin->GetRight();
  if (left->type() == sqlast::Expression::Type::AND) {
    CHECK_STATUS(ExtractConstraintsFromAnd(left, schema, cnstrs));
  } else {
    ASSIGN_OR_RETURN(std::string lcolumn, GetColumnName(left));
    MOVE_OR_RETURN(std::vector<std::string> lvalues, GetCondValues(left));
    cnstrs->insert({schema.IndexOf(lcolumn), std::move(lvalues)});
  }
  if (right->type() == sqlast::Expression::Type::AND) {
    CHECK_STATUS(ExtractConstraintsFromAnd(right, schema, cnstrs));
  } else {
    ASSIGN_OR_RETURN(std::string rcolumn, GetColumnName(right));
    MOVE_OR_RETURN(std::vector<std::string> rvalues, GetCondValues(right));
    cnstrs->insert({schema.IndexOf(rcolumn), std::move(rvalues)});
  }
  return absl::OkStatus();
}

// Unconstrained select (no where).
absl::StatusOr<std::vector<dataflow::Record>> SelectViewUnconstrained(
    std::shared_ptr<dataflow::MatViewOperator> matview, int limit,
    size_t offset) {
  // No where condition: we are reading everything (keys and records).
  std::vector<dataflow::Record> records;
  for (const auto &key : matview->Keys()) {
    for (const auto &record : matview->Lookup(key, limit, offset)) {
      records.push_back(record.Copy());
    }
  }
  return records;
}

// Constrained with equality or related operations.
absl::StatusOr<std::vector<dataflow::Record>> SelectViewConstrained(
    std::shared_ptr<dataflow::MatViewOperator> matview,
    const sqlast::BinaryExpression *where, int limit, size_t offset) {
  const dataflow::SchemaRef &schema = matview->output_schema();
  std::unordered_map<dataflow::ColumnID, std::vector<std::string>> constraints;
  std::vector<dataflow::Record> records;
  switch (where->type()) {
    // LITERAL, COLUMN, EQ, AND, GREATER_THAN, IN, LIST
    case sqlast::Expression::Type::EQ:
    case sqlast::Expression::Type::IN: {
      ASSIGN_OR_RETURN(std::string column, GetColumnName(where));
      MOVE_OR_RETURN(std::vector<std::string> values, GetCondValues(where));
      constraints.insert({schema.IndexOf(column), values});
      break;
    }
    case sqlast::Expression::Type::AND: {
      ExtractConstraintsFromAnd(where, schema, &constraints);
      break;
    }
    default:
      return absl::InvalidArgumentError(
          "Illegal where condition for keyed matview");
  }

  std::vector<dataflow::Key> keys;
  keys.emplace_back(matview->key_cols().size());
  for (dataflow::ColumnID col : matview->key_cols()) {
    if (constraints.count(col) == 0) {
      return absl::InvalidArgumentError(
          "Where condition does not constraint column " + schema.NameOf(col));
    }

    // Cross product of different possible values for keys.
    std::vector<dataflow::Key> tmp;
    for (std::string &val : constraints.at(col)) {
      for (const auto &key : keys) {
        // Copy key and put val in it
        dataflow::Key copy = key;
        if (val == "NULL") {
          copy.AddNull(schema.TypeOf(col));
          continue;
        }
        switch (schema.TypeOf(col)) {
          case sqlast::ColumnDefinition::Type::UINT:
            copy.AddValue(static_cast<uint64_t>(std::stoull(val)));
            break;
          case sqlast::ColumnDefinition::Type::INT:
            copy.AddValue(static_cast<int64_t>(std::stoll(val)));
            break;
          case sqlast::ColumnDefinition::Type::TEXT:
          case sqlast::ColumnDefinition::Type::DATETIME:
            copy.AddValue(dataflow::Record::Dequote(val));
            break;
          default:
            return absl::InvalidArgumentError("Unsupported type in view read");
        }
        tmp.push_back(std::move(copy));
      }
    }
    keys = std::move(tmp);
  }

  // Read records attached to key.
  for (const auto &key : keys) {
    for (const auto &record : matview->Lookup(key, limit, offset)) {
      records.push_back(record.Copy());
    }
  }

  return records;
}

// Ordered view.
absl::StatusOr<std::vector<dataflow::Record>> SelectViewOrdered(
    std::shared_ptr<dataflow::MatViewOperator> matview,
    const sqlast::BinaryExpression *where, int limit, size_t offset) {
  switch (where->type()) {
    case sqlast::Expression::Type::GREATER_THAN: {
      std::vector<dataflow::Record> records;
      const dataflow::SchemaRef &schema = matview->output_schema();

      ASSIGN_OR_RETURN(std::string column, GetColumnName(where));
      MOVE_OR_RETURN(std::vector<std::string> values, GetCondValues(where));
      size_t column_index = schema.IndexOf(column);
      CHECK_EQ(values.size(), 1);

      // Read records > cmp.
      dataflow::Record cmp{schema};
      cmp.SetValue(values.at(0), column_index);
      for (const auto &k : matview->Keys()) {
        for (const auto &r : matview->LookupGreater(k, cmp, limit, offset)) {
          records.push_back(r.Copy());
        }
      }

      return records;
    }
    case sqlast::Expression::Type::EQ:
      return SelectViewConstrained(matview, where, limit, offset);

    default:
      return absl::InvalidArgumentError(
          "Illegal WHERE condition for sorted view");
  }
}

}  // namespace

absl::Status CreateView(const sqlast::CreateView &stmt,
                        Connection *connection) {
  perf::Start("CreateView");
  dataflow::DataFlowState *dataflow_state =
      connection->pelton_state->GetDataFlowState();
  // Plan the query using calcite and generate a concrete graph for it.
  std::shared_ptr<dataflow::DataFlowGraph> graph =
      planner::PlanGraph(dataflow_state, stmt.query());

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  dataflow_state->AddFlow(stmt.view_name(), graph);

  perf::End("CreateView");
  return absl::OkStatus();
}

absl::StatusOr<sql::SqlResult> SelectView(const sqlast::Select &stmt,
                                          Connection *connection) {
  // TODO(babman): fix this.
  perf::Start("SelectView");
  dataflow::DataFlowState *dataflow_state =
      connection->pelton_state->GetDataFlowState();

  // Get the corresponding flow.
  const std::string &view_name = stmt.table_name();
  const std::shared_ptr<dataflow::DataFlowGraph> flow =
      dataflow_state->GetFlow(view_name);

  // Only support flow ending with a single output matview for now.
  if (flow->outputs().size() == 0) {
    return absl::InvalidArgumentError("Read from flow with no matviews");
  } else if (flow->outputs().size() > 1) {
    return absl::InvalidArgumentError("Read from flow with several matviews");
  }
  auto matview = flow->outputs().at(0);
  const dataflow::SchemaRef &schema = matview->output_schema();

  std::vector<dataflow::Record> records;

  // Check parameters.
  size_t offset = stmt.offset();
  int limit = stmt.limit();

  if (stmt.HasWhereClause()) {
    const sqlast::BinaryExpression *where = stmt.GetWhereClause();
    if (matview->RecordOrdered()) {
      MOVE_OR_RETURN(records, SelectViewOrdered(matview, where, limit, offset));
    } else {
      MOVE_OR_RETURN(records,
                     SelectViewConstrained(matview, where, limit, offset));
    }
  } else {
    MOVE_OR_RETURN(records, SelectViewUnconstrained(matview, limit, offset));
  }

  perf::End("SelectView");
  return sql::SqlResult(std::make_unique<sql::_result::SqlInlineResultSet>(
      schema, std::move(records)));
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
