// Defines the API for our SQLite-adapter.
#include "pelton/prepared.h"

#include <cassert>
// NOLINTNEXTLINE
#include <regex>

#include "glog/logging.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"

namespace pelton {
namespace prepared {

namespace {

#define TABLE_COLUMN_NAME "\\b(?:([A-Za-z0-9_]+)\\.|)([A-Za-z0-9_]+)"
#define COLUMN_NAME "\\b([A-Za-z0-9_]+)"
#define OP "\\s*(=|<|>|<=|>=)\\s*"
#define Q "\\?"

#define IN "\\s+[Ii][Nn]\\s*"
#define MQ "\\(\\s*\\?\\s*((?:\\s*,\\s*\\?)*)\\)"

static std::regex CANONICAL_PARAM_REGEX{TABLE_COLUMN_NAME OP Q};
static std::regex CANONICAL_REGEX{IN MQ};
static std::regex PARAM_REGEX{COLUMN_NAME "(?:" OP Q "|" IN MQ ")"};

}  // namespace

// Turn query into canonical form.
CanonicalQuery Canonicalize(const std::string &query) {
  CanonicalQuery canonical;
  std::smatch sm;
  std::string q = query;
  while (regex_search(q, sm, CANONICAL_REGEX)) {
    canonical.append(sm.prefix().str());
    canonical.append(" = ?");
    q = sm.suffix().str();
  }
  canonical.append(q);
  if (canonical.back() == ';') {
    canonical.pop_back();
  }
  return canonical;
}

// Extract canonical statement information from canonical query.
CanonicalDescriptor MakeCanonical(const CanonicalQuery &query) {
  CanonicalDescriptor descriptor;
  descriptor.canonical_query = query;
  descriptor.args_count = 0;
  // Find arguments.
  std::smatch sm;
  std::string q = query;
  while (regex_search(q, sm, CANONICAL_PARAM_REGEX)) {
    descriptor.args_count++;
    descriptor.stems.push_back(sm.prefix().str());
    descriptor.arg_tables.push_back(sm[1].str());
    descriptor.arg_names.push_back(sm[2].str());
    descriptor.arg_ops.push_back(sm[3].str());
    // Next match.
    q = sm.suffix().str();
  }
  descriptor.stems.push_back(q);
  return descriptor;
}

// Turn query into a prepared statement struct.
PreparedStatementDescriptor MakeStmt(const std::string &query) {
  PreparedStatementDescriptor descriptor;
  descriptor.total_count = 0;
  // Find the count of values to each arguments.
  std::smatch sm;
  std::string q = query;
  while (regex_search(q, sm, PARAM_REGEX)) {
    auto question_marks = sm[3];
    size_t parameter_count = 1;
    for (auto it = question_marks.first; it != question_marks.second; ++it) {
      if (*it == '?') {
        parameter_count++;
      }
    }
    descriptor.total_count += parameter_count;
    descriptor.arg_value_count.push_back(parameter_count);
    // Next match.
    q = sm.suffix().str();
  }
  return descriptor;
}

// Find out if a query needs to be served from a flow.
bool NeedsFlow(const CanonicalQuery &query) { return true; }

// Populate prepared statement with concrete values.
std::string PopulateStatement(const PreparedStatementDescriptor &stmt,
                              const std::vector<std::string> &args) {
  const std::vector<std::string> &stems = stmt.canonical->stems;
  std::string query = stems.at(0);
  size_t v = 0;
  for (size_t i = 0; i < stmt.canonical->args_count; i++) {
    const auto &name = stmt.canonical->arg_names.at(i);
    query.push_back(' ');
    query.append(name);
    // Operator: = and IN are sometimes interchanegable.
    const auto &op = stmt.canonical->arg_ops.at(i);
    size_t count = stmt.arg_value_count.at(i);
    if (count > 1) {  // IN (?, ...)
      assert(op == "=");
      query.append(" IN (");
    } else {
      query.append(op);
    }
    // Add the values assigned to this parameter.
    for (size_t j = 0; j < count; j++) {
      query.append(args.at(v + j));
      if (j < count - 1) {
        query.append(", ");
      }
    }
    v += count;
    // Added all values for this parameter.
    if (count > 1) {
      query.push_back(')');
    }
    // Next stem.
    query.append(stems.at(i + 1));
  }
  return query;
}

// Extract type information about ? arguments from flow.
void FromFlow(const std::string &flow_name,
              const dataflow::DataFlowGraph &graph, CanonicalDescriptor *stmt) {
  auto *partition = graph.GetPartition(0);
  const auto &matviews = partition->outputs();
  const auto &inputs = partition->inputs();
  // Go over all ? parameters and figure out their types.
  for (size_t i = 0; i < stmt->args_count; i++) {
    const std::string &table_name = stmt->arg_tables.at(i);
    const std::string &column_name = stmt->arg_names.at(i);
    // Look up the column in the base tables.
    if (table_name != "") {
      if (inputs.count(table_name) == 0) {
        LOG(FATAL) << "Prepared stmt refers to invalid table " << table_name;
      }
      const auto &schema = inputs.at(table_name)->output_schema();
      stmt->arg_types.push_back(schema.TypeOf(schema.IndexOf(column_name)));
    } else {
      // Look up the column name in the output keys.
      const auto *matview = matviews.at(0);
      const auto &schema = matview->output_schema();
      stmt->arg_types.push_back(schema.TypeOf(schema.IndexOf(column_name)));
    }
  }
  // Re-build the stems so that they query the view.
  stmt->stems.clear();
  stmt->stems.push_back("SELECT * FROM " + flow_name);
  if (stmt->args_count > 0) {
    stmt->stems.front().append(" WHERE");
    for (size_t i = 0; i < stmt->args_count - 1; i++) {
      stmt->stems.push_back("AND");
    }
    stmt->stems.push_back(";");
  }
}

}  // namespace prepared
}  // namespace pelton
