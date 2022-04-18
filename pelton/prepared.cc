// Defines the API for our SQLite-adapter.
#include "pelton/prepared.h"

#include <cassert>
#include <memory>
// NOLINTNEXTLINE
#include <regex>
#include <unordered_set>
#include <utility>

#include "absl/strings/match.h"
#include "glog/logging.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/sqlast/hacky.h"
#include "pelton/util/status.h"

namespace pelton {
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

// Prepared statements that should not be served from views.
std::unordered_set<std::string> NO_VIEWS = {
    // PreparedTest.java.
    "SELECT * FROM tbl WHERE id = ?",
    "SELECT * FROM tbl WHERE id = ? AND age > ?",
    // Lobsters.
    // Q1.
    "SELECT 1 AS `one` FROM users WHERE users.PII_username = ?",
    // Q2.
    "SELECT 1 AS `one`, stories.short_id FROM stories WHERE stories.short_id = "
    "?",
    // Q3.
    "SELECT tags.* FROM tags WHERE tags.inactive = 0 AND tags.tag = ?",
    // Q4.
    "SELECT keystores.* FROM keystores WHERE keystores.keyX = ?",
    // Q5.
    "SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND "
    "votes.story_id = ? AND votes.comment_id IS NULL",
    // Q7.
    "SELECT stories.* FROM stories WHERE stories.short_id = ?",
    // Q8.
    "SELECT users.* FROM users WHERE users.id = ?",
    // Q9.
    "SELECT 1 AS `one`, comments.short_id FROM comments WHERE "
    "comments.short_id = ?",
    // Q10.
    "SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND "
    "votes.story_id = ? AND votes.comment_id = ?",
    // Q14.
    "SELECT comments.* FROM comments WHERE comments.story_id = ? AND "
    "comments.short_id = ?",
    // Q15.
    "SELECT read_ribbons.* FROM read_ribbons WHERE read_ribbons.user_id = ? "
    "AND read_ribbons.story_id = ?",
    // Q18.
    "SELECT hidden_stories.story_id FROM hidden_stories WHERE "
    "hidden_stories.user_id = ?",
    // Q19.
    "SELECT users.* FROM users WHERE users.PII_username = ?",
    // Q20.
    "SELECT hidden_stories.* FROM hidden_stories WHERE hidden_stories.user_id "
    "= ? AND hidden_stories.story_id = ?",
    // Q21.
    "SELECT tag_filters.* FROM tag_filters WHERE tag_filters.user_id = ?",
    // Q23.
    "SELECT taggings.story_id FROM taggings WHERE taggings.story_id = ?",
    // Q24.
    "SELECT saved_stories.* FROM saved_stories WHERE saved_stories.user_id = ? "
    "AND saved_stories.story_id = ?",
    // Q31.
    "SELECT 1, user_id, story_id FROM hidden_stories WHERE "
    "hidden_stories.user_id = ? AND hidden_stories.story_id = ?",
    // Q33.
    "SELECT votes.* FROM votes WHERE votes.OWNER_user_id = ? AND "
    "votes.comment_id = ?",
    // Q34.
    "SELECT comments.* FROM comments WHERE comments.short_id = ?",
    // GDPRBench.
    "SELECT * FROM usertable WHERE YCSB_KEY = ?",
    // Owncloud.
    "SELECT * FROM file_view WHERE share_target = ?"};

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
  return absl::StartsWith(query, "SELECT") && NO_VIEWS.count(query) == 0;
}

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
}

// For handling insert/replace prepared statements.
absl::StatusOr<CanonicalDescriptor> MakeInsertCanonical(
    const std::string &insert, const dataflow::DataFlowState &dstate) {
  // Parse the statement using the hacky parser.
  MOVE_OR_RETURN(std::unique_ptr<sqlast::AbstractStatement> parsed,
                 sqlast::HackyInsert(insert.data(), insert.size()));
  sqlast::Insert *stmt = static_cast<sqlast::Insert *>(parsed.get());
  // Find table schema.
  auto schema = dstate.GetTableSchema(stmt->table_name());
  // Begin constructing the descriptor.
  CanonicalDescriptor descriptor;
  descriptor.canonical_query = insert;
  descriptor.args_count = 0;
  // Build initial stem.
  std::string stem = stmt->replace() ? "REPLACE " : "INSERT ";
  stem += "INTO " + stmt->table_name();
  if (stmt->HasColumns()) {
    stem.push_back('(');
    for (const std::string &column : stmt->GetColumns()) {
      stem += column + ", ";
    }
    stem.pop_back();
    stem.pop_back();
    stem.push_back(')');
  }
  stem += " VALUES (";
  // Go over the values.
  const std::vector<std::string> &values = stmt->GetValues();
  for (size_t i = 0; i < values.size(); i++) {
    if (values.at(i) != "?") {
      // Not a ? arg, append it to stem.
      stem += values.at(i) + (i < values.size() - 1 ? ", " : ");");
      continue;
    } else {
      // Is a ? arg, need to add it to the descriptor.
      descriptor.args_count++;
      descriptor.stems.push_back(std::move(stem));
      stem = i < values.size() - 1 ? ", " : ");";
      // Population for insert is simpler, need only to insert , between values.
      descriptor.arg_tables.push_back("");
      descriptor.arg_names.push_back("");
      descriptor.arg_ops.push_back("");
      // Get type of argument.
      sqlast::ColumnDefinition::Type type;
      if (stmt->HasColumns()) {
        const std::string &column_name = stmt->GetColumns().at(i);
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
}  // namespace pelton
