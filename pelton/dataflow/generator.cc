#include "pelton/dataflow/generator.h"

#include <memory>
#include <utility>

#include "glog/logging.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/ops/union.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/value.h"

namespace pelton {
namespace dataflow {

DataFlowGraphGenerator::DataFlowGraphGenerator(
    DataFlowGraphPartitionJavaPtr graph_ptr, DataFlowStateJavaPtr state_ptr) {
  this->graph_ = reinterpret_cast<DataFlowGraphPartition *>(graph_ptr);
  this->state_ = reinterpret_cast<DataFlowState *>(state_ptr);
}

// Adding operators.
NodeIndex DataFlowGraphGenerator::AddInputOperator(
    const std::string &table_name) {
  // Create input operator
  SchemaRef table_schema = this->state_->GetTableSchema(table_name);
  std::unique_ptr<InputOperator> input =
      std::make_unique<InputOperator>(table_name, table_schema);
  // Add the operator to the graph.
  CHECK(this->graph_->AddInputNode(std::move(input)));
  return this->graph_->LastOperatorIndex();
}
NodeIndex DataFlowGraphGenerator::AddUnionOperator(
    const std::vector<NodeIndex> &parents) {
  // Create union operator.
  std::unique_ptr<UnionOperator> op = std::make_unique<UnionOperator>();
  // Add the operator to the graph.
  std::vector<Operator *> parents_ptrs;
  for (NodeIndex idx : parents) {
    Operator *op = this->graph_->GetNode(idx);
    CHECK(op);
    parents_ptrs.push_back(op);
  }
  CHECK(this->graph_->AddNode(std::move(op), parents_ptrs));
  return this->graph_->LastOperatorIndex();
}
NodeIndex DataFlowGraphGenerator::AddFilterOperator(NodeIndex parent) {
  // Create filter operator.
  std::unique_ptr<FilterOperator> op = std::make_unique<FilterOperator>();
  // Add the operator to the graph.
  Operator *parent_ptr = this->graph_->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(this->graph_->AddNode(std::move(op), parent_ptr));
  return this->graph_->LastOperatorIndex();
}
NodeIndex DataFlowGraphGenerator::AddProjectOperator(NodeIndex parent) {
  // Create project operator.
  std::unique_ptr<ProjectOperator> op = std::make_unique<ProjectOperator>();
  // Add the operator to the graph.
  Operator *parent_ptr = this->graph_->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(this->graph_->AddNode(std::move(op), parent_ptr));
  return this->graph_->LastOperatorIndex();
}
NodeIndex DataFlowGraphGenerator::AddAggregateOperator(
    NodeIndex parent, const std::vector<ColumnID> &group_cols,
    AggregateFunctionEnum agg_func, ColumnID agg_col, const std::string &name) {
  // Create aggregate operator.
  std::unique_ptr<AggregateOperator> op =
      std::make_unique<AggregateOperator>(group_cols, agg_func, agg_col, name);
  // Add the operator to the graph.
  Operator *parent_ptr = this->graph_->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(this->graph_->AddNode(std::move(op), parent_ptr));
  return this->graph_->LastOperatorIndex();
}
NodeIndex DataFlowGraphGenerator::AddJoinOperator(NodeIndex left_parent,
                                                  NodeIndex right_parent,
                                                  ColumnID left_column,
                                                  ColumnID right_column,
                                                  JoinModeEnum mode) {
  // Create equijoin operator.
  std::unique_ptr<EquiJoinOperator> op =
      std::make_unique<EquiJoinOperator>(left_column, right_column, mode);
  // Add the operator to the graph.
  Operator *left_ptr = this->graph_->GetNode(left_parent);
  Operator *right_ptr = this->graph_->GetNode(right_parent);
  CHECK(left_ptr);
  CHECK(right_ptr);
  CHECK(this->graph_->AddNode(std::move(op), {left_ptr, right_ptr}));
  return this->graph_->LastOperatorIndex();
}

// Output materialized views.
NodeIndex DataFlowGraphGenerator::AddMatviewOperator(
    NodeIndex parent, const std::vector<ColumnID> &key_cols) {
  return this->AddMatviewOperator(parent, key_cols, {}, -1, 0);
}
NodeIndex DataFlowGraphGenerator::AddMatviewOperator(
    NodeIndex parent, const std::vector<ColumnID> &key_cols,
    const std::vector<ColumnID> &sort_cols, int limit, size_t offset) {
  std::unique_ptr<MatViewOperator> op;
  // Determine which type of MatView we need.
  if (sort_cols.empty()) {
    op = std::make_unique<UnorderedMatViewOperator>(key_cols, limit, offset);
  } else if (sort_cols == key_cols) {
    op = std::make_unique<KeyOrderedMatViewOperator>(key_cols, limit, offset);
  } else {
    Record::Compare compare{sort_cols};
    op = std::make_unique<RecordOrderedMatViewOperator>(key_cols, compare,
                                                        limit, offset);
  }
  // Add the operator to the graph.
  Operator *parent_ptr = this->graph_->GetNode(parent);
  CHECK(parent_ptr);
  CHECK(this->graph_->AddOutputOperator(std::move(op), parent_ptr));
  return this->graph_->LastOperatorIndex();
}

// Setting properties on existing operators.
// Filter.
void DataFlowGraphGenerator::AddFilterOperationString(NodeIndex filter_operator,
                                                      const std::string &value,
                                                      ColumnID column,
                                                      FilterOperationEnum fop) {
  // Get filter operator.
  Operator *op = this->graph_->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  FilterOperator *filter = static_cast<FilterOperator *>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}
void DataFlowGraphGenerator::AddFilterOperationInt(NodeIndex filter_operator,
                                                   int64_t value,
                                                   ColumnID column,
                                                   FilterOperationEnum fop) {
  // Get filter operator.
  Operator *op = this->graph_->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  FilterOperator *filter = static_cast<FilterOperator *>(op);
  // Add the operation to filter.
  filter->AddOperation(value, column, fop);
}
void DataFlowGraphGenerator::AddFilterOperationNull(NodeIndex filter_operator,
                                                    ColumnID column,
                                                    FilterOperationEnum fop) {
  // Get filter operator.
  Operator *op = this->graph_->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  FilterOperator *filter = static_cast<FilterOperator *>(op);
  // Add the operation to filter.
  filter->AddOperation(column, fop);
}
void DataFlowGraphGenerator::AddFilterOperationColumn(NodeIndex filter_operator,
                                                      ColumnID left_column,
                                                      ColumnID right_column,
                                                      FilterOperationEnum fop) {
  // Get filter operator.
  Operator *op = this->graph_->GetNode(filter_operator);
  CHECK(op->type() == Operator::Type::FILTER);
  FilterOperator *filter = static_cast<FilterOperator *>(op);
  filter->AddOperation(left_column, fop, right_column);
}

// Projection.
void DataFlowGraphGenerator::AddProjectionColumn(NodeIndex project_operator,
                                                 const std::string &column_name,
                                                 ColumnID cid) {
  // Get project operator.
  Operator *op = this->graph_->GetNode(project_operator);
  CHECK(op->type() == Operator::Type::PROJECT);
  ProjectOperator *project = static_cast<ProjectOperator *>(op);
  // Add projection to project
  project->AddColumnProjection(column_name, cid);
}
void DataFlowGraphGenerator::AddProjectionLiteralInt(
    NodeIndex project_operator, const std::string &column_name, int64_t value) {
  // Get project operator.
  Operator *op = this->graph_->GetNode(project_operator);
  CHECK(op->type() == Operator::Type::PROJECT);
  ProjectOperator *project = static_cast<ProjectOperator *>(op);
  // Add projection to project
  project->AddLiteralProjection(column_name, value);
}
void DataFlowGraphGenerator::AddProjectionArithmeticLiteralLeft(
    NodeIndex project_operator, const std::string &column_name,
    ColumnID left_column, int64_t right_literal,
    ProjectOperationEnum operation) {
  // Get project operator.
  Operator *op = this->graph_->GetNode(project_operator);
  CHECK(op->type() == Operator::Type::PROJECT);
  ProjectOperator *project = static_cast<ProjectOperator *>(op);
  // Add projection to project
  project->AddArithmeticLeftProjection(column_name, left_column, right_literal,
                                       operation);
}
void DataFlowGraphGenerator::AddProjectionArithmeticLiteralRight(
    NodeIndex project_operator, const std::string &column_name,
    int64_t left_literal, ColumnID right_column,
    ProjectOperationEnum operation) {
  // Get project operator.
  Operator *op = this->graph_->GetNode(project_operator);
  CHECK(op->type() == Operator::Type::PROJECT);
  ProjectOperator *project = static_cast<ProjectOperator *>(op);
  // Add projection to project
  project->AddArithmeticRightProjection(column_name, left_literal, right_column,
                                        operation);
}
void DataFlowGraphGenerator::AddProjectionArithmeticColumns(
    NodeIndex project_operator, const std::string &column_name, ColumnID left,
    ColumnID right, ProjectOperationEnum operation) {
  // Get project operator.
  Operator *op = this->graph_->GetNode(project_operator);
  CHECK(op->type() == Operator::Type::PROJECT);
  ProjectOperator *project = static_cast<ProjectOperator *>(op);
  // Add projection to project
  project->AddArithmeticColumnsProjection(column_name, left, right, operation);
}

// Reading schema.
std::vector<std::string> DataFlowGraphGenerator::GetTables() const {
  std::vector<std::string> tables;
  std::vector<std::string> views;
  std::vector<std::string> tables_and_views;

  // return names of both tables and views
  tables = this->state_->GetTables();
  views = this->state_->GetFlows();

  // concatenate names
  tables_and_views.reserve(tables.size() + views.size());
  tables_and_views.insert(tables_and_views.end(), tables.begin(), tables.end());
  tables_and_views.insert(tables_and_views.end(), views.begin(), views.end());

  return tables_and_views;
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

// Debugging string.
std::string DataFlowGraphGenerator::DebugString() const {
  return this->graph_->DebugString();
}

}  // namespace dataflow
}  // namespace pelton
