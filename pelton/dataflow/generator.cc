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

namespace pelton {
namespace dataflow {

DataFlowGraphGenerator::DataFlowGraphGenerator(DataFlowGraphJavaPtr graph_ptr,
                                               DataFlowStateJavaPtr state_ptr) {
  this->graph_ = reinterpret_cast<DataFlowGraph *>(graph_ptr);
  this->state_ = reinterpret_cast<DataFlowState *>(state_ptr);
}

// Adding operators.
NodeIndex DataFlowGraphGenerator::AddInputOperator(
    const std::string &table_name) {
  // Create input operator
  SchemaRef table_schema = this->state_->GetTableSchema(table_name);
  std::shared_ptr<InputOperator> input =
      std::make_shared<InputOperator>(table_name, table_schema);
  // Add the operator to the graph.
  CHECK(this->graph_->AddInputNode(input));
  return input->index();
}
NodeIndex DataFlowGraphGenerator::AddUnionOperator(
    const std::vector<NodeIndex> &parents) {
  // Create union operator.
  std::shared_ptr<UnionOperator> op = std::make_shared<UnionOperator>();
  // Add the operator to the graph.
  std::vector<std::shared_ptr<Operator>> parents_ptrs;
  for (NodeIndex idx : parents) {
    std::shared_ptr<Operator> op = this->graph_->GetNode(idx);
    CHECK(op);
    parents_ptrs.push_back(op);
  }
  CHECK(this->graph_->AddNode(op, parents_ptrs));
  return op->index();
}
NodeIndex DataFlowGraphGenerator::AddFilterOperator(NodeIndex parent) {
  // Create filter operator.
  std::shared_ptr<FilterOperator> op = std::make_shared<FilterOperator>();
  // Add the operator to the graph.
  std::shared_ptr<Operator> parent_ptr = this->graph_->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(this->graph_->AddNode(op, parent_ptr));
  return op->index();
}
NodeIndex DataFlowGraphGenerator::AddEquiJoinOperator(NodeIndex left_parent,
                                                      NodeIndex right_parent,
                                                      ColumnID left_column,
                                                      ColumnID right_column) {
  // Create equijoin operator.
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(left_column, right_column);
  // Add the operator to the graph.
  std::shared_ptr<Operator> left_ptr = this->graph_->GetNode(left_parent);
  std::shared_ptr<Operator> right_ptr = this->graph_->GetNode(right_parent);
  CHECK(left_ptr);
  CHECK(right_ptr);
  CHECK(this->graph_->AddNode(op, {left_ptr, right_ptr}));
  return op->index();
}
NodeIndex DataFlowGraphGenerator::AddMatviewOperator(NodeIndex parent) {
  // Find parent.
  std::shared_ptr<Operator> parent_ptr = this->graph_->GetNode(parent);
  CHECK(parent_ptr);
  // Find the key of the output schema of parent, use as key for this matview.
  const std::vector<ColumnID> &key_cols = parent_ptr->output_schema().keys();
  // TODO(babman): Key.h and Record.h only support single key.
  return this->AddMatviewOperator(
      parent, std::vector<ColumnID>(key_cols.cbegin(), key_cols.cbegin() + 1));
}
NodeIndex DataFlowGraphGenerator::AddMatviewOperator(
    NodeIndex parent, const std::vector<ColumnID> &key_cols) {
  // Create filter operator.
  std::shared_ptr<MatViewOperator> op =
      std::make_shared<MatViewOperator>(key_cols);
  // Add the operator to the graph.
  std::shared_ptr<Operator> parent_ptr = this->graph_->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(this->graph_->AddOutputOperator(op, parent_ptr));
  return op->index();
}

// Setting properties on existing operators.
void DataFlowGraphGenerator::AddFilterOperation(NodeIndex filter_operator,
                                                const std::string &value,
                                                ColumnID column,
                                                FilterOperationEnum fop) {
  // Get filter operator.
  std::shared_ptr<Operator> op = this->graph_->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  std::shared_ptr<FilterOperator> filter =
      std::static_pointer_cast<FilterOperator>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}
void DataFlowGraphGenerator::AddFilterOperationUnsigned(
    NodeIndex filter_operator, uint64_t value, ColumnID column,
    FilterOperationEnum fop) {
  // Get filter operator.
  std::shared_ptr<Operator> op = this->graph_->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  std::shared_ptr<FilterOperator> filter =
      std::static_pointer_cast<FilterOperator>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}
void DataFlowGraphGenerator::AddFilterOperationSigned(NodeIndex filter_operator,
                                                      int64_t value,
                                                      ColumnID column,
                                                      FilterOperationEnum fop) {
  // Get filter operator.
  std::shared_ptr<Operator> op = this->graph_->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  std::shared_ptr<FilterOperator> filter =
      std::static_pointer_cast<FilterOperator>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}

// Reading schema.
std::vector<std::string> DataFlowGraphGenerator::GetTables() const {
  return this->state_->GetTables();
}
size_t DataFlowGraphGenerator::GetTableColumnCount(
    const std::string &table_name) const {
  return this->state_->GetTableSchema(table_name).size();
}
std::string DataFlowGraphGenerator::GetTableColumnName(
    const std::string &table_name, ColumnID column) const {
  return this->state_->GetTableSchema(table_name).NameOf(column);
}
sqlast::ColumnDefinitionTypeEnum DataFlowGraphGenerator::GetTableColumnType(
    const std::string &table_name, ColumnID column) const {
  return this->state_->GetTableSchema(table_name).TypeOf(column);
}

}  // namespace dataflow
}  // namespace pelton
