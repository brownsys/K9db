// A graph generator is used to build a DataFlowGraphPartition incrementally.
// This is used by the planner (Java with calcite) to build a
// DataFlowGraphPartition using our C++ class structure directly without going
// through intermediates.
#ifndef K9DB_DATAFLOW_GENERATOR_H_
#define K9DB_DATAFLOW_GENERATOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "k9db/dataflow/ops/aggregate_enum.h"
#include "k9db/dataflow/ops/equijoin_enum.h"
#include "k9db/dataflow/ops/filter_enum.h"
#include "k9db/dataflow/ops/project_enum.h"
#include "k9db/dataflow/types.h"
#include "k9db/sqlast/ast_schema_enums.h"

namespace k9db {
namespace dataflow {

// Forward decleration to avoid include k9db/dataflow/graph.h and
// k9db/dataflow/state.h here.
// Including these header files throws off javacpp, which is responsible for
// generating the java native API and its shims.
class DataFlowGraphPartition;
class DataFlowState;

using DataFlowGraphPartitionJavaPtr = uint64_t;
using DataFlowStateJavaPtr = uint64_t;

class DataFlowGraphGenerator {
 public:
  DataFlowGraphGenerator(DataFlowGraphPartitionJavaPtr graph_ptr,
                         DataFlowStateJavaPtr state_ptr);

  // Adding operators.
  NodeIndex AddInputOperator(const std::string &table_name);
  NodeIndex AddUnionOperator(const std::vector<NodeIndex> &parents);
  NodeIndex AddFilterOperator(NodeIndex parent);
  NodeIndex AddProjectOperator(NodeIndex parent);
  NodeIndex AddAggregateOperator(NodeIndex parent,
                                 const std::vector<ColumnID> &group_cols,
                                 AggregateFunctionEnum agg_func,
                                 ColumnID agg_col,
                                 const std::string &agg_col_name);
  NodeIndex AddJoinOperator(NodeIndex left_parent, NodeIndex right_parent,
                            ColumnID left_column, ColumnID right_column,
                            JoinModeEnum mode);

  // Adding output materialized views.
  // Unordered mat view.
  NodeIndex AddMatviewOperator(NodeIndex parent,
                               const std::vector<ColumnID> &key_cols);
  // Potentially sorted (depending on sort_cols).
  NodeIndex AddMatviewOperator(NodeIndex parent,
                               const std::vector<ColumnID> &key_cols,
                               const std::vector<ColumnID> &sort_cols,
                               int limit, size_t offset);

  // Setting properties on existing operators.
  // Filter.
  void AddFilterOperationString(NodeIndex filter, ColumnID column,
                                const std::string &value,
                                FilterOperationEnum op);
  void AddFilterOperationInt(NodeIndex filter, ColumnID column, int64_t value,
                             FilterOperationEnum op);
  void AddFilterOperationNull(NodeIndex filter, ColumnID column,
                              FilterOperationEnum op);
  void AddFilterOperationColumn(NodeIndex filter, ColumnID left, ColumnID right,
                                FilterOperationEnum op);
  // Projection.
  void AddProjectionColumn(NodeIndex project, const std::string &name,
                           ColumnID cid);

  void AddProjectionLiteralInt(NodeIndex project, const std::string &name,
                               int64_t value);
  void AddProjectionLiteralUInt(NodeIndex project, const std::string &name,
                                uint64_t value);
  void AddProjectionLiteralString(NodeIndex project, const std::string &name,
                                  const std::string &value);

  void AddProjectionArithmeticColumns(NodeIndex project,
                                      const std::string &name,
                                      ProjectOperationEnum op, ColumnID left,
                                      ColumnID right);
  void AddProjectionArithmeticLiteralLeft(NodeIndex project,
                                          const std::string &name,
                                          ProjectOperationEnum op, int64_t left,
                                          ColumnID right);
  void AddProjectionArithmeticLiteralRight(NodeIndex project,
                                           const std::string &name,
                                           ProjectOperationEnum op,
                                           ColumnID left, int64_t right);

  // Reading schema.
  std::vector<std::string> GetTables() const;
  size_t GetTableColumnCount(const std::string &table_name) const;
  std::string GetTableColumnName(const std::string &table_name,
                                 ColumnID column) const;
  sqlast::ColumnDefinitionTypeEnum GetTableColumnType(
      const std::string &table_name, ColumnID column) const;

  // Debugging string.
  std::string DebugString() const;

 private:
  DataFlowGraphPartition *graph_;
  DataFlowState *state_;
};

}  // namespace dataflow
}  // namespace k9db

#endif  // K9DB_DATAFLOW_GENERATOR_H_
