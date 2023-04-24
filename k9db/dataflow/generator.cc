#include "k9db/dataflow/generator.h"

#include <memory>
#include <utility>

#include "glog/logging.h"
#include "k9db/dataflow/graph.h"
#include "k9db/dataflow/operator.h"
#include "k9db/dataflow/ops/aggregate.h"
#include "k9db/dataflow/ops/equijoin.h"
#include "k9db/dataflow/ops/filter.h"
#include "k9db/dataflow/ops/forward_view.h"
#include "k9db/dataflow/ops/input.h"
#include "k9db/dataflow/ops/matview.h"
#include "k9db/dataflow/ops/project.h"
#include "k9db/dataflow/ops/union.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/state.h"

namespace k9db {
namespace dataflow {

DataFlowGraphGenerator::DataFlowGraphGenerator(
    DataFlowGraphPartitionJavaPtr graph_ptr, DataFlowStateJavaPtr state_ptr) {
  this->graph_ = reinterpret_cast<DataFlowGraphPartition *>(graph_ptr);
  this->state_ = reinterpret_cast<DataFlowState *>(state_ptr);
}

// Adding operators.
NodeIndex DataFlowGraphGenerator::AddInputOperator(
    const std::string &table_name) {
  // Check if table_name is a view name.
  if (this->state_->HasFlow(table_name)) {
    // get MAT VIEW operator from dataflow
    const auto partition = this->state_->GetFlow(table_name).GetPartition(0);
    MatViewOperator *matview = partition->outputs().front();
    PCHECK(matview);
    // create an ForwardViewOperator
    std::unique_ptr<ForwardViewOperator> op =
        std::make_unique<ForwardViewOperator>(matview->output_schema(),
                                              table_name, matview->index());
    CHECK(op);
    // Add the ForwardView operator to the graph, where MAT VIEW is a parent
    CHECK(this->graph_->AddForwardOperator(std::move(op), {}));
  } else {
    // Doesn't correspond to view, create input operator as normal
    SchemaRef table_schema = this->state_->GetTableSchema(table_name);
    std::unique_ptr<InputOperator> input =
        std::make_unique<InputOperator>(table_name, table_schema);
    // Add the input operator (table) to the graph.
    CHECK(this->graph_->AddInputNode(std::move(input)));
  }
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
void DataFlowGraphGenerator::AddFilterOperationString(NodeIndex filter,
                                                      ColumnID column,
                                                      const std::string &value,
                                                      FilterOperationEnum op) {
  // Get filter operator.
  Operator *ptr = this->graph_->GetNode(filter);
  CHECK(ptr->type() == Operator::Type::FILTER);
  FilterOperator *fop = static_cast<FilterOperator *>(ptr);
  // Add the operation to filter.
  fop->AddLiteralOperation(column, value, op);
}
void DataFlowGraphGenerator::AddFilterOperationInt(NodeIndex filter,
                                                   ColumnID column,
                                                   int64_t value,
                                                   FilterOperationEnum op) {
  // Get filter operator.
  Operator *ptr = this->graph_->GetNode(filter);
  CHECK(ptr->type() == Operator::Type::FILTER);
  FilterOperator *fop = static_cast<FilterOperator *>(ptr);
  // Add the operation to filter.
  fop->AddLiteralOperation(column, value, op);
}
void DataFlowGraphGenerator::AddFilterOperationNull(NodeIndex filter,
                                                    ColumnID column,
                                                    FilterOperationEnum op) {
  // Get filter operator.
  Operator *ptr = this->graph_->GetNode(filter);
  CHECK(ptr->type() == Operator::Type::FILTER);
  FilterOperator *fop = static_cast<FilterOperator *>(ptr);
  // Add the operation to filter.
  fop->AddNullOperation(column, op);
}
void DataFlowGraphGenerator::AddFilterOperationColumn(NodeIndex filter,
                                                      ColumnID left,
                                                      ColumnID right,
                                                      FilterOperationEnum op) {
  // Get filter operator.
  Operator *ptr = this->graph_->GetNode(filter);
  CHECK(ptr->type() == Operator::Type::FILTER);
  FilterOperator *fop = static_cast<FilterOperator *>(ptr);
  fop->AddColumnOperation(left, right, op);
}

// Projection.
void DataFlowGraphGenerator::AddProjectionColumn(NodeIndex project,
                                                 const std::string &name,
                                                 ColumnID cid) {
  // Get project operator.
  Operator *ptr = this->graph_->GetNode(project);
  CHECK(ptr->type() == Operator::Type::PROJECT);
  ProjectOperator *pop = static_cast<ProjectOperator *>(ptr);
  // Add projection to project
  pop->AddColumnProjection(name, cid);
}
void DataFlowGraphGenerator::AddProjectionLiteralInt(NodeIndex project,
                                                     const std::string &name,
                                                     int64_t value) {
  // Get project operator.
  Operator *ptr = this->graph_->GetNode(project);
  CHECK(ptr->type() == Operator::Type::PROJECT);
  ProjectOperator *pop = static_cast<ProjectOperator *>(ptr);
  // Add projection to project
  pop->AddLiteralProjection(name, value);
}
void DataFlowGraphGenerator::AddProjectionLiteralString(
    NodeIndex project, const std::string &name, const std::string &value) {
  // Get project operator.
  Operator *ptr = this->graph_->GetNode(project);
  CHECK(ptr->type() == Operator::Type::PROJECT);
  ProjectOperator *pop = static_cast<ProjectOperator *>(ptr);
  // Add projection to project
  pop->AddLiteralProjection(name, value);
}
void DataFlowGraphGenerator::AddProjectionArithmeticColumns(
    NodeIndex project, const std::string &name, ProjectOperationEnum op,
    ColumnID left, ColumnID right) {
  // Get project operator.
  Operator *ptr = this->graph_->GetNode(project);
  CHECK(ptr->type() == Operator::Type::PROJECT);
  ProjectOperator *pop = static_cast<ProjectOperator *>(ptr);
  // Add projection to project
  pop->AddArithmeticProjectionColumns(name, op, left, right);
}
void DataFlowGraphGenerator::AddProjectionArithmeticLiteralLeft(
    NodeIndex project, const std::string &name, ProjectOperationEnum op,
    int64_t left, ColumnID right) {
  // Get project operator.
  Operator *ptr = this->graph_->GetNode(project);
  CHECK(ptr->type() == Operator::Type::PROJECT);
  ProjectOperator *pop = static_cast<ProjectOperator *>(ptr);
  // Add projection to project
  pop->AddArithmeticProjectionLiteralLeft(name, op, left, right);
}
void DataFlowGraphGenerator::AddProjectionArithmeticLiteralRight(
    NodeIndex project, const std::string &name, ProjectOperationEnum op,
    ColumnID left, int64_t right) {
  // Get project operator.
  Operator *ptr = this->graph_->GetNode(project);
  CHECK(ptr->type() == Operator::Type::PROJECT);
  ProjectOperator *pop = static_cast<ProjectOperator *>(ptr);
  // Add projection to project
  pop->AddArithmeticProjectionLiteralRight(name, op, left, right);
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
}  // namespace k9db
