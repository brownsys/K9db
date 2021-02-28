// A graph generator is used to build a DataFlowGraph incrementally.
// This is used by the planner (Java with calcite) to build a DataFlowGraph
// using our C++ class structure directly without going through intermediates.
#ifndef PELTON_DATAFLOW_GENERATOR_H_
#define PELTON_DATAFLOW_GENERATOR_H_

#include <cstdint>
#include <string>
#include <vector>

#include "pelton/dataflow/ops/filter_enum.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast_schema_enums.h"

namespace pelton {
namespace dataflow {

class DataFlowGraphGenerator {
 public:
  DataFlowGraphGenerator(uint64_t graph_ptr, uint64_t state_ptr);

  // Adding operators.
  NodeIndex AddInputOperator(const std::string &table_name);
  NodeIndex AddUnionOperator(const std::vector<NodeIndex> &parents);
  NodeIndex AddFilterOperator(NodeIndex parent);
  NodeIndex AddEquiJoinOperator(NodeIndex left_parent, NodeIndex right_parent,
                                ColumnID left_column, ColumnID right_column);
  NodeIndex AddMatviewOperator(NodeIndex parent);  // Deduce key from parent.
  NodeIndex AddMatviewOperator(NodeIndex parent,
                               const std::vector<ColumnID> &key_cols);

  // Setting properties on existing operators.
  void AddFilterOperation(NodeIndex filter_operator, const std::string &value,
                          ColumnID column, FilterOperationEnum fop);
  void AddFilterOperationUnsigned(NodeIndex filter_operator, uint64_t value,
                                  ColumnID column, FilterOperationEnum fop);
  void AddFilterOperationSigned(NodeIndex filter_operator, int64_t value,
                                ColumnID column, FilterOperationEnum fop);

  // Reading schema.
  std::vector<std::string> GetTables() const;
  size_t GetTableColumnCount(const std::string &table_name) const;
  std::string GetTableColumnName(const std::string &table_name,
                                 ColumnID column) const;
  sqlast::ColumnDefinitionTypeEnum GetTableColumnType(
      const std::string &table_name, ColumnID column) const;

 private:
  uint64_t graph_ptr_;
  uint64_t state_ptr_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GENERATOR_H_
