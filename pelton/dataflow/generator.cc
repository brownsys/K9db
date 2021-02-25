#include "pelton/dataflow/generator.h"

#include <memory>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/union.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"

#define CAST_GRAPH() reinterpret_cast<DataFlowGraph *>(this->graph_ptr_)
#define CAST_STATE() reinterpret_cast<DataflowState *>(this->state_ptr_)

namespace pelton {
namespace dataflow {

DataFlowGraphGenerator::DataFlowGraphGenerator(uint64_t graph_ptr,
                                               uint64_t state_ptr)
    : graph_ptr_(graph_ptr), state_ptr_(state_ptr) {}

// Adding operators.
NodeIndex DataFlowGraphGenerator::AddInputOperator(
    const std::string &table_name) {
  DataFlowGraph *graph = CAST_GRAPH();
  DataflowState *state = CAST_STATE();
  // Create input operator.
  SchemaRef table_schema = state->GetTableSchema(table_name);
  std::shared_ptr<InputOperator> input =
      std::make_shared<InputOperator>(table_name, table_schema);
  // Add the operator to the graph.
  CHECK(graph->AddInputNode(input));
  return input->index();
}
NodeIndex DataFlowGraphGenerator::AddUnionOperator(
    const std::vector<NodeIndex> &parents) {
  DataFlowGraph *graph = CAST_GRAPH();
  // Create union operator.
  std::shared_ptr<UnionOperator> op = std::make_shared<UnionOperator>();
  // Add the operator to the graph.
  std::vector<std::shared_ptr<Operator>> parents_ptrs;
  for (NodeIndex idx : parents) {
    std::shared_ptr<Operator> op = graph->GetNode(idx);
    CHECK(op);
    parents_ptrs.push_back(op);
  }
  CHECK(graph->AddNode(op, parents_ptrs));
  return op->index();
}
NodeIndex DataFlowGraphGenerator::AddFilterOperator(NodeIndex parent) {
  DataFlowGraph *graph = CAST_GRAPH();
  // Create filter operator.
  std::shared_ptr<FilterOperator> op = std::make_shared<FilterOperator>();
  // Add the operator to the graph.
  std::shared_ptr<Operator> parent_ptr = graph->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(graph->AddNode(op, parent_ptr));
  return op->index();
}
NodeIndex DataFlowGraphGenerator::AddEquiJoinOperator(NodeIndex left_parent,
                                                      NodeIndex right_parent,
                                                      ColumnID left_column,
                                                      ColumnID right_column) {
  DataFlowGraph *graph = CAST_GRAPH();
  // Create equijoin operator.
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(left_column, right_column);
  // Add the operator to the graph.
  std::shared_ptr<Operator> left_ptr = graph->GetNode(left_parent);
  std::shared_ptr<Operator> right_ptr = graph->GetNode(right_parent);
  CHECK(left_ptr);
  CHECK(right_ptr);
  CHECK(graph->AddNode(op, {left_ptr, right_ptr}));
  return op->index();
}
NodeIndex DataFlowGraphGenerator::AddMatviewOperator(
    NodeIndex parent, const std::vector<ColumnID> &key_cols) {
  DataFlowGraph *graph = CAST_GRAPH();
  // Create filter operator.
  std::shared_ptr<MatViewOperator> op =
      std::make_shared<MatViewOperator>(key_cols);
  // Add the operator to the graph.
  std::shared_ptr<Operator> parent_ptr = graph->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(graph->AddNode(op, parent_ptr));
  return op->index();
}

// Setting properties on existing operators.
void DataFlowGraphGenerator::AddFilterOperation(NodeIndex filter_operator,
                                                const std::string &value,
                                                ColumnID column,
                                                FilterOperationEnum fop) {
  DataFlowGraph *graph = CAST_GRAPH();
  // Get filter operator.
  std::shared_ptr<Operator> op = graph->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  std::shared_ptr<FilterOperator> filter =
      std::static_pointer_cast<FilterOperator>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}
void DataFlowGraphGenerator::AddFilterOperation(NodeIndex filter_operator,
                                                uint64_t value, ColumnID column,
                                                FilterOperationEnum fop) {
  DataFlowGraph *graph = CAST_GRAPH();
  // Get filter operator.
  std::shared_ptr<Operator> op = graph->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  std::shared_ptr<FilterOperator> filter =
      std::static_pointer_cast<FilterOperator>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}
void DataFlowGraphGenerator::AddFilterOperation(NodeIndex filter_operator,
                                                int64_t value, ColumnID column,
                                                FilterOperationEnum fop) {
  DataFlowGraph *graph = CAST_GRAPH();
  // Get filter operator.
  std::shared_ptr<Operator> op = graph->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  std::shared_ptr<FilterOperator> filter =
      std::static_pointer_cast<FilterOperator>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}

}  // namespace dataflow
}  // namespace pelton
