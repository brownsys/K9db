// Defines the API for our SQLite-adapter.
#include "k9db/prepared.h"

#include <cassert>
#include <memory>
// NOLINTNEXTLINE
#include <regex>
#include <unordered_set>
#include <utility>

#include "absl/strings/match.h"
#include "glog/logging.h"
#include "k9db/dataflow/ops/input.h"
#include "k9db/dataflow/ops/matview.h"
#include "k9db/sqlast/hacky.h"
#include "k9db/util/status.h"

namespace k9db {
namespace prepared {

namespace {

// Regexes for parsing components of SELECT or UPDATE statements.
// Find table name in a "SELECT ... FROM <table>" or "UPDATE <table>".
#define FROM_OR_UPDATE "(?:\\s[Ff][Rr][Oo][Mm]|\\b[Uu][Pp][Dd][Aa][Tt][Ee])"
#define FROM_TABLE FROM_OR_UPDATE "\\s+([A-Za-z0-9_]+)\\b"

// Find a column name (identifier) qualified with an optional table name.
#define TABLE_COLUMN_NAME "\\b(?:([A-Za-z0-9_]+)\\.|)([A-Za-z0-9_]+)"

// Components for matching expressions with ? in WHERE clauses.
#define OP "\\s*(=|<|>|<=|>=)\\s*"
#define Q "\\?"

// Regexes made out of above components.
static std::regex FROM_TABLE_REGEX{FROM_TABLE};
static std::regex CANONICAL_PARAM_REGEX{TABLE_COLUMN_NAME OP Q};

// Regexes to figure out if a view is needed or not.
#define SPACE_OR_OPEN_PAR "(\\s|\\()"
#define SUM_OR_COUNT "( SUM\\s*\\(| COUNT\\s*\\()"
#define JOIN " JOIN" SPACE_OR_OPEN_PAR
#define GROUP_BY " GROUP BY "
#define ORDER_BY " ORDER BY "
#define EXPENSIVE "(" JOIN "|" ORDER_BY "|" GROUP_BY "|>|<|>=|<=|\\+|-)"
#define SELECT "(^|\\s|\\(|\\))SELECT" SPACE_OR_OPEN_PAR

static std::regex VIEW_FEATURES_REGEX{SUM_OR_COUNT "|" EXPENSIVE};
static std::regex NESTED_QUERY{SELECT ".*" SELECT};

// Helper function that:
// 1) Removes all quoted text ("", '', ``)
// 2) Standardizes white space by changing \n and \t to ' '
// 3) turns everything into upper case.
std::string CleanCanonicalQuery(const CanonicalQuery &query) {
  char current_quote = ' ';
  bool in_quote = false;
  std::string cleaned_query = "";
  for (size_t i = 0; i < query.size(); i++) {
    char c = query.at(i);

    // Ignore whats inside quotes.
    if (in_quote) {
      // If we are inside quotes and we encounter \, we escape next char.
      if (c == '\\') {
        i++;
        continue;
      }
      // Found matching closing quote.
      if (current_quote == c) {
        current_quote = ' ';
        in_quote = false;
      }
      continue;
    }

    // We are not inside quotes.
    // But maybe we are at the opening quote!
    if (c == '"' || c == '`' || c == '\'') {
      current_quote = c;
      in_quote = true;
      continue;
    }

    // Character is legit, we push it after cleaning it.
    if (c == '\t' || c == '\n' || c == '\r') {
      cleaned_query.push_back(' ');
    } else if (c >= 97 && c <= 122) {
      cleaned_query.push_back(c - 32);
    } else {
      cleaned_query.push_back(c);
    }
  }

  return cleaned_query;
}

}  // namespace

// Turn query into canonical form.
std::pair<CanonicalQuery, std::vector<size_t>> Canonicalize(
    const std::string &query) {
  char c = query[0];
  if (c == 'I' || c == 'i' || c == 'R' || c == 'r') {
    // Insert/Replace are already canonical.
    return std::make_pair(query, std::vector<size_t>{});
  } else {
    std::vector<size_t> arg_value_count;
    // Select/Update needs to canonicalize IN (?, ...).
    CanonicalQuery canonical;
    canonical.reserve(query.size());
    // Iterate over query, append everything to canonical except IN (?, ?, ...).
    bool token_start = true;
    bool in = false;
    bool quotes = false;
    size_t current_value_count = 0;
    for (size_t i = 0; i < query.size(); i++) {
      char c = query.at(i);
      // Parsing an in.
      if (in) {
        if (c == ')') {
          in = false;
          canonical.push_back('=');
          canonical.push_back(' ');
          canonical.push_back('?');
          arg_value_count.push_back(current_value_count);
        } else if (c == '?') {
          current_value_count++;
        } else if (c != ',' && c != ' ' && c != '(') {
          LOG(FATAL) << "Mixed ? and values in IN";
        }
        continue;
      }
      if (!quotes) {
        // Space between tokens.
        if (c == ' ') {
          token_start = true;
          canonical.push_back(' ');
          continue;
        }
        // Start of a string.
        if (c == '\'') {
          quotes = true;
          token_start = false;
          canonical.push_back('\'');
          continue;
        }
        // A ? parameter.
        if (c == '?') {
          arg_value_count.push_back(1);
        }
        // Start of a token, maybe it is IN!
        if (token_start && i + 2 < query.size()) {
          if ((c == 'I' || c == 'i') &&
              (query.at(i + 1) == 'N' || query.at(i + 1) == 'n') &&
              (query.at(i + 2) == ' ' || query.at(i + 2) == '(')) {
            token_start = false;
            in = true;
            i += 2;
            continue;
          }
        }
      } else {
        // inside quotes, check for terminating quote.
        if (c == '\'' && query.at(i - 1) != '\\') {
          quotes = false;
        }
      }
      // Not IN and no a space.
      token_start = false;
      canonical.push_back(c);
    }
    if (canonical.at(canonical.size() - 1) == ';') {
      canonical.pop_back();
    }
    return std::make_pair(canonical, std::move(arg_value_count));
  }
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
PreparedStatementDescriptor MakeStmt(const std::string &query,
                                     const CanonicalDescriptor *canonical,
                                     std::vector<size_t> &&arg_value_count) {
  PreparedStatementDescriptor descriptor;
  descriptor.canonical = canonical;
  // Check type of query.
  char c = query[0];
  if (c == 'I' || c == 'i' || c == 'R' || c == 'r') {
    // Insert/Replace are already canonical, each canonical ? corresponds to a
    // single value.
    descriptor.total_count = canonical->args_count;
    for (size_t i = 0; i < descriptor.total_count; i++) {
      descriptor.arg_value_count.push_back(1);
    }
  } else {
    // Select/Update can have IN (?, ...) statements where many values are
    // assigned to the same canonical ?.
    descriptor.total_count = 0;
    for (size_t count : arg_value_count) {
      descriptor.total_count += count;
    }
    descriptor.arg_value_count = std::move(arg_value_count);
  }
  return descriptor;
}

// Find out if a query needs to be served from a flow.
bool NeedsFlow(const CanonicalQuery &query) {
  std::string cleaned_query = CleanCanonicalQuery(query);
  if (!absl::StartsWith(cleaned_query, "SELECT")) {
    return false;
  }

  std::smatch sm;
  if (regex_search(cleaned_query, sm, VIEW_FEATURES_REGEX)) {
    return true;
  } else if (regex_search(cleaned_query, sm, NESTED_QUERY)) {
    return true;
  }
  return false;
}

// Populate prepared statement with concrete values.
sqlast::SQLCommand PopulateStatement(const PreparedStatementDescriptor &stmt,
                                     const std::vector<std::string> &args) {
  const std::vector<std::string> &stems = stmt.canonical->stems;

  sqlast::SQLCommand command;
  command.AddStem(stems.at(0));
  size_t v = 0;
  for (size_t i = 0; i < stmt.canonical->args_count; i++) {
    const auto &name = stmt.canonical->arg_names.at(i);
    command.AddChar(' ');
    command.AddStem(name);
    // Operator: = and IN are sometimes interchanegable.
    const std::string &op = stmt.canonical->arg_ops.at(i);
    size_t count = stmt.arg_value_count.at(i);
    if (count > 1) {  // IN (?, ...)
      assert(op == "=");
      command.AddStem(" IN (");
    } else {
      command.AddStem(op);
    }
    // Add the values assigned to this parameter.
    for (size_t j = 0; j < count; j++) {
      command.AddArg(args.at(v + j));
      if (j < count - 1) {
        command.AddStem(", ");
      }
    }
    v += count;
    // Added all values for this parameter.
    if (count > 1) {
      command.AddChar(')');
    }
    // Next stem.
    command.AddChar(' ');
    command.AddStem(stems.at(i + 1));
  }
  return command;
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
  stmt->view_name = flow_name;
}

// Extract type information about ? arguments from the underlying tables.
void FromTables(const CanonicalQuery &query,
                const dataflow::DataFlowState &dstate,
                CanonicalDescriptor *stmt) {
  // Find source table.
  std::smatch sm;
  if (!regex_search(query, sm, FROM_TABLE_REGEX)) {
    LOG(FATAL) << "Cannot find source table in prepared statement " << query;
  }
  // Look up table schema.
  std::string table_name = sm[1].str();
  dataflow::SchemaRef schema;
  if (dstate.HasFlow(table_name)) {
    const auto &flow = dstate.GetFlow(table_name);
    schema = flow.output_schema();
  } else {
    schema = dstate.GetTableSchema(table_name);
  }
  // Go over all ? parameters and figure out their types.
  for (size_t i = 0; i < stmt->args_count; i++) {
    const std::string &column_name = stmt->arg_names.at(i);
    // Look up the column in the base tables.
    if (stmt->arg_tables.at(i) != "" && stmt->arg_tables.at(i) != table_name) {
      LOG(FATAL) << "Prepared stmt refers to invalid direct table " << query;
    }
    stmt->arg_types.push_back(schema.TypeOf(schema.IndexOf(column_name)));
  }
  stmt->view_name = {};
}

// For handling insert/replace prepared statements.
absl::StatusOr<CanonicalDescriptor> MakeInsertCanonical(
    const std::string &insert, const dataflow::DataFlowState &dstate) {
  // Parse the statement using the hacky parser.
  ASSIGN_OR_RETURN(
      sqlast::InsertOrReplace & components,
      sqlast::HackyInsertOrReplace(insert.data(), insert.size(), {}));
  // Find table schema.
  auto schema = dstate.GetTableSchema(components.table_name);
  // Begin constructing the descriptor.
  CanonicalDescriptor descriptor;
  descriptor.canonical_query = insert;
  descriptor.args_count = 0;
  // Build initial stem.
  std::string stem = components.replace ? "REPLACE " : "INSERT ";
  stem += "INTO " + components.table_name;
  if (components.columns.size() > 0) {
    stem.push_back('(');
    for (const std::string &column : components.columns) {
      stem += column + ", ";
    }
    stem.pop_back();
    stem.pop_back();
    stem.push_back(')');
  }
  stem += " VALUES (";
  // Go over the values.
  for (size_t i = 0; i < components.values.size(); i++) {
    const std::string &value = components.values.at(i);
    if (value != "?") {
      // Not a ? arg, append it to stem.
      stem += value + (i < components.values.size() - 1 ? ", " : ");");
      continue;
    } else {
      // Is a ? arg, need to add it to the descriptor.
      descriptor.args_count++;
      descriptor.stems.push_back(std::move(stem));
      stem = i < components.values.size() - 1 ? "," : ");";
      // Population for insert is simpler, need only to insert , between values.
      descriptor.arg_tables.push_back("");
      descriptor.arg_names.push_back("");
      descriptor.arg_ops.push_back("");
      // Get type of argument.
      sqlast::ColumnDefinition::Type type;
      if (components.columns.size() > 0) {
        const std::string &column_name = components.columns.at(i);
        type = schema.TypeOf(schema.IndexOf(column_name));
      } else {
        type = schema.TypeOf(i);
      }
      descriptor.arg_types.push_back(type);
    }
  }
  descriptor.stems.push_back(std::move(stem));

  // Move the descriptor.
  return std::move(descriptor);
}

}  // namespace prepared
}  // namespace k9db
