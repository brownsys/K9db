// A graph generator is used to build a DataFlowGraphPartition incrementally.
// This is used by the planner (Java with calcite) to build a
// DataFlowGraphPartition using our C++ class structure directly without going
// through intermediates.
#ifndef PELTON_DATAFLOW_GENERATOR_H_
#define PELTON_DATAFLOW_GENERATOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "pelton/dataflow/ops/aggregate_enum.h"
#include "pelton/dataflow/ops/equijoin_enum.h"
#include "pelton/dataflow/ops/filter_enum.h"
#include "pelton/dataflow/ops/project_enum.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast_schema_enums.h"

namespace pelton {
namespace dataflow {

// Forward decleration to avoid include pelton/dataflow/graph.h and
// pelton/dataflow/state.h here.
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
  void AddFilterOperationString(NodeIndex filter_operator,
                                const std::string &value, ColumnID column,
                                FilterOperationEnum fop);
  void AddFilterOperationInt(NodeIndex filter_operator, int64_t value,
                             ColumnID column, FilterOperationEnum fop);
  void AddFilterOperationNull(NodeIndex filter_operator, ColumnID column,
                              FilterOperationEnum fop);
  void AddFilterOperationColumn(NodeIndex filter_operator, ColumnID left_column,
                                ColumnID right_column, FilterOperationEnum fop);
  // Projection.
  void AddProjectionColumn(NodeIndex project_operator,
                           const std::string &column_name, ColumnID cid);
  void AddProjectionLiteralInt(NodeIndex project_operator,
                               const std::string &column_name, int64_t value);

  void AddProjectionArithmeticLiteralLeft(NodeIndex project_operator,
                                          const std::string &column_name,
                                          ColumnID left_column,
                                          int64_t right_literal,
                                          ProjectOperationEnum operation);
  void AddProjectionArithmeticLiteralRight(NodeIndex project_operator,
                                           const std::string &column_name,
                                           int64_t left_literal,
                                           ColumnID right_column,
                                           ProjectOperationEnum operation);
  void AddProjectionArithmeticColumns(NodeIndex project_operator,
                                      const std::string &column_name,
                                      ColumnID left, ColumnID right,
                                      ProjectOperationEnum operation);

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
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GENERATOR_H_
