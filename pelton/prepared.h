#ifndef PELTON_PREPARED_H_
#define PELTON_PREPARED_H_

#include <string>
#include <vector>

#include "pelton/dataflow/graph.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace prepared {

// Prepared statement API.
using StatementID = size_t;
using CanonicalQuery = std::string;

// Contains information about prepared statements in canonical form.
struct CanonicalDescriptor {
  CanonicalQuery canonical_query;
  size_t args_count;
  std::vector<std::string> stems;  // size = args_count + 1
  // Statement arguments.
  std::vector<std::string> arg_tables;  // size = args_count
  std::vector<std::string> arg_names;   // size = args_count
  std::vector<std::string> arg_ops;     // size = args_count
  std::vector<sqlast::ColumnDefinition::Type> arg_types;
};

// Contains information about a connection-specific prepared statement
// in form identical to what the SQL client uses (not always canonical).
struct PreparedStatementDescriptor {
  StatementID stmt_id;
  size_t total_count;                   // Total count of ?
  std::vector<size_t> arg_value_count;  // size = canonical->args_count
  // Points to corresponding canonical statement descriptor.
  const CanonicalDescriptor *canonical;
};

// Turn query into canonical form.
CanonicalQuery Canonicalize(const std::string &query);

// Extract canonical statement information from canonical query.
CanonicalDescriptor MakeCanonical(const CanonicalQuery &query);

// Turn query into a prepared statement struct.
PreparedStatementDescriptor MakeStmt(const std::string &query);

// Find out if a query needs to be served from a flow.
bool NeedsFlow(const CanonicalQuery &query);

// Populate prepared statement with concrete values.
std::string PopulateStatement(const PreparedStatementDescriptor &stmt,
                              const std::vector<std::string> &args);

// Extract type information about ? arguments from flow.
void FromFlow(const std::string &flow_name,
              const dataflow::DataFlowGraph &graph, CanonicalDescriptor *stmt);

}  // namespace prepared
}  // namespace pelton

#endif  // PELTON_PREPARED_H_
