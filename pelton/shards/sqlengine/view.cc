// Creation and management of dataflows.

#include "pelton/shards/sqlengine/view.h"

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "absl/status/statusor.h"
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

void CopyColumns(const std::vector<std::string> cols, char **out) {
  for (size_t i = 0; i < cols.size(); i++) {
    const std::string &str = cols.at(i);
    out[i] = new char[str.size() + 1];
    // NOLINTNEXTLINE
    strcpy(out[i], str.c_str());
  }
}

void CopyValues(const dataflow::Record &record, char **out) {
  for (size_t i = 0; i < record.schema().size(); i++) {
    std::string str = record.GetValueString(i);
    out[i] = new char[str.size() + 1];
    // NOLINTNEXTLINE
    strcpy(out[i], str.c_str());
  }
}

void DeleteValues(size_t n, char **vs) {
  for (size_t i = 0; i < n; i++) {
    delete[] vs[i];
  }
}

absl::StatusOr<std::string> GetColumnName(const sqlast::BinaryExpression *exp) {
  const sqlast::Expression *left = exp->GetLeft();
  const sqlast::Expression *right = exp->GetRight();
  if (left->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(left)->column();
  }
  if (right->type() == sqlast::Expression::Type::COLUMN) {
    return static_cast<const sqlast::ColumnExpression *>(right)->column();
  }
  return absl::InvalidArgumentError("Invalid view where clause: no column!");
}

absl::StatusOr<std::string> GetCondValue(const sqlast::BinaryExpression *exp) {
  const sqlast::Expression *left = exp->GetLeft();
  const sqlast::Expression *right = exp->GetRight();
  if (left->type() == sqlast::Expression::Type::LITERAL) {
    return static_cast<const sqlast::LiteralExpression *>(left)->value();
  }
  if (right->type() == sqlast::Expression::Type::LITERAL) {
    return static_cast<const sqlast::LiteralExpression *>(right)->value();
  }
  return absl::InvalidArgumentError("Invalid view where clause: no literal!");
}

}  // namespace

absl::Status CreateView(const sqlast::CreateView &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state) {
  perf::Start("CreateView");
  // Plan the query using calcite and generate a concrete graph for it.
  dataflow::DataFlowGraph graph =
      planner::PlanGraph(dataflow_state, stmt.query());

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  dataflow_state->AddFlow(stmt.view_name(), graph);

  perf::End("CreateView");
  return absl::OkStatus();
}

absl::Status SelectView(const sqlast::Select &stmt, SharderState *state,
                        dataflow::DataFlowState *dataflow_state,
                        const OutputChannel &output) {
  perf::Start("SelectView");

  // Get the corresponding flow.
  const std::string &view_name = stmt.table_name();
  const dataflow::DataFlowGraph &flow = dataflow_state->GetFlow(view_name);

  // Only support flow ending with a single output matview for now.
  if (flow.outputs().size() == 0) {
    return absl::InvalidArgumentError("Read from flow with no matviews");
  } else if (flow.outputs().size() > 1) {
    return absl::InvalidArgumentError("Read from flow with several matviews");
  }
  auto matview = flow.outputs().at(0);
  const dataflow::SchemaRef &schema = matview->output_schema();

  // Allocate memory for data to read into.
  std::vector<std::string> cols = schema.column_names();
  size_t colnum = cols.size();
  char **colnames = new char *[colnum];
  char **colvals = new char *[colnum];
  CopyColumns(cols, colnames);

  // Check parameters.
  size_t offset = stmt.offset();
  int limit = stmt.limit();
  if (stmt.HasWhereClause()) {
    const sqlast::BinaryExpression *where = stmt.GetWhereClause();
    ASSIGN_OR_RETURN(std::string column, GetColumnName(where));
    ASSIGN_OR_RETURN(std::string value, GetCondValue(where));
    size_t column_index = schema.IndexOf(column);

    // If value is a string, trim surrounding quotes.
    if (schema.TypeOf(column_index) == sqlast::ColumnDefinition::Type::TEXT) {
      value = value.substr(1, value.size() - 2);
    }

    switch (where->type()) {
      // Reading by greater than: the query provides us with a base record and
      // we read queries greater than that record.
      case sqlast::Expression::Type::GREATER_THAN: {
        if (!matview->RecordOrdered()) {
          return absl::InvalidArgumentError(
              "Record ordered read into unordered view");
        }

        // Read records > cmp.
        dataflow::Record cmp{schema};
        cmp.SetValue(value, column_index);
        for (const auto &key : matview->Keys()) {
          for (const auto &record :
               matview->LookupGreater(key, cmp, limit, offset)) {
            CopyValues(record, colvals);
            output.callback(output.context, colnum, colvals, colnames);
            DeleteValues(colnum, colvals);
          }
        }
        break;
      }
      // Reading by equality: the query provides us with a key that we read with
      // directly from the matview.
      case sqlast::Expression::Type::EQ: {
        // Key must be of the right type.
        dataflow::Key key{1};
        switch (schema.TypeOf(column_index)) {
          case sqlast::ColumnDefinition::Type::UINT:
            key.AddValue(static_cast<uint64_t>(std::stoull(value)));
            break;
          case sqlast::ColumnDefinition::Type::INT:
            key.AddValue(static_cast<int64_t>(std::stoll(value)));
            break;
          case sqlast::ColumnDefinition::Type::TEXT:
            key.AddValue(value);
            break;
          default:
            return absl::InvalidArgumentError("Unsupported type in view read");
        }

        // Read records attached to key.
        for (const auto &record : matview->Lookup(key, limit, offset)) {
          CopyValues(record, colvals);
          output.callback(output.context, colnum, colvals, colnames);
          DeleteValues(colnum, colvals);
        }
        break;
      }
      default: {
        return absl::InvalidArgumentError("Unsupported read from view query");
      }
    }
  } else {
    // No where condition: we are reading everything (keys and records).
    for (const auto &key : matview->Keys()) {
      for (const auto &record : matview->Lookup(key, limit, offset)) {
        CopyValues(record, colvals);
        output.callback(output.context, colnum, colvals, colnames);
        DeleteValues(colnum, colvals);
      }
    }
  }

  DeleteValues(colnum, colnames);
  delete[] colnames;
  delete[] colvals;

  perf::End("SelectView");

  return absl::OkStatus();
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
