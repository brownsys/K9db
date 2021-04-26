// A graph generator is used to build a DataFlowGraph incrementally.
// This is used by the planner (Java with calcite) to build a DataFlowGraph
// using our C++ class structure directly without going through intermediates.
#ifndef PELTON_DATAFLOW_GENERATOR_H_
#define PELTON_DATAFLOW_GENERATOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "pelton/dataflow/ops/aggregate_enum.h"
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
class DataFlowGraph;
class DataFlowState;

using DataFlowGraphJavaPtr = uint64_t;
using DataFlowStateJavaPtr = uint64_t;

class DataFlowGraphGenerator {
 public:
  DataFlowGraphGenerator(DataFlowGraphJavaPtr graph_ptr,
                         DataFlowStateJavaPtr state_ptr);

  // Adding operators.
  NodeIndex AddInputOperator(const std::string &table_name);
  NodeIndex AddUnionOperator(const std::vector<NodeIndex> &parents);
  NodeIndex AddFilterOperator(NodeIndex parent);
  NodeIndex AddProjectOperator(NodeIndex parent,
                               const std::vector<ColumnID> keys);
  NodeIndex AddAggregateOperator(NodeIndex parent,
                                 const std::vector<ColumnID> &group_cols,
                                 AggregateFunctionEnum agg_func,
                                 ColumnID agg_col);
  NodeIndex AddEquiJoinOperator(NodeIndex left_parent, NodeIndex right_parent,
                                ColumnID left_column, ColumnID right_column);

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
  void AddFilterOperation(NodeIndex filter_operator, const std::string &value,
                          ColumnID column, FilterOperationEnum fop);
  void AddFilterOperationNull(NodeIndex filter_operator, ColumnID column,
                              FilterOperationEnum fop);
  void AddFilterOperationUnsigned(NodeIndex filter_operator, uint64_t value,
                                  ColumnID column, FilterOperationEnum fop);
  void AddFilterOperationSigned(NodeIndex filter_operator, int64_t value,
                                ColumnID column, FilterOperationEnum fop);
  void AddProjectionColumn(NodeIndex project_operator,
                           const std::string &column_name, ColumnID cid);
  void AddProjectionLiteralSigned(NodeIndex project_operator,
                                  const std::string &column_name, int64_t value,
                                  ProjectMetadataEnum metadata);
  void AddProjectionLiteralUnsigned(NodeIndex project_operator,
                                    const std::string &column_name,
                                    uint64_t value,
                                    ProjectMetadataEnum metadata);
  void AddProjectionArithmeticWithLiteralSigned(NodeIndex project_operator,
                                                const std::string &column_name,
                                                ColumnID left_operand,
                                                ProjectOperationEnum operation,
                                                int64_t right_operand,
                                                ProjectMetadataEnum metadata);
  void AddProjectionArithmeticWithLiteralUnsignedOrColumn(
      NodeIndex project_operator, const std::string &column_name,
      ColumnID left_operand, ProjectOperationEnum operation,
      uint64_t right_operand, ProjectMetadataEnum metadata);

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
  DataFlowGraph *graph_;
  DataFlowState *state_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GENERATOR_H_
