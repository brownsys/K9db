#include "pelton/dataflow/graph.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/graph_test_utils.h"

namespace pelton {
namespace dataflow {

// Tests!
TEST(DataFlowGraphTest, TestTrivialGraph) {
  // Schema must survive records.
  SchemaRef schema = MakeLeftSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g = MakeTrivialGraph(0, schema);
  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table", records));
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), records);
}

TEST(DataFlowGraphTest, TestFilterGraph) {
  // Schema must survive records.
  SchemaRef schema = MakeLeftSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g = MakeFilterGraph(0, schema);
  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table", records));
  // Filter records.
  auto op = std::dynamic_pointer_cast<FilterOperator>(g->GetNode(1));
  std::vector<Record> filtered = MakeFilterRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), filtered);
}

TEST(DataFlowGraphTest, TestUnionGraph) {
  // Schema must survive records.
  SchemaRef schema = MakeLeftSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g = MakeUnionGraph(0, schema);
  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  std::vector<Record> first_half;
  first_half.push_back(records.at(0).Copy());
  first_half.push_back(records.at(1).Copy());
  std::vector<Record> second_half;
  second_half.push_back(records.at(2).Copy());
  second_half.push_back(records.at(3).Copy());
  second_half.push_back(records.at(4).Copy());
  // Process records.
  EXPECT_TRUE(g->Process("test-table1", first_half));
  EXPECT_TRUE(g->Process("test-table2", second_half));
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), records);
}

TEST(DataFlowGraphTest, TestEquiJoinGraph) {
  // Schema must survive records.
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g =
      MakeEquiJoinGraph(0, 2, 0, lschema, rschema);
  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table1", left));
  EXPECT_TRUE(g->Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<EquiJoinOperator>(g->GetNode(2));
  std::vector<Record> result = MakeJoinRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), result);
}

TEST(DataFlowGraphTest, TestProjectGraph) {
  // Schema must survive records.
  SchemaRef schema = MakeLeftSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g = MakeProjectGraph(0, schema);
  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table", records));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<ProjectOperator>(g->GetNode(1));
  std::vector<Record> result = MakeProjectRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), result);
}

TEST(DataFlowGraphTest, TestProjectOnFilterGraph) {
  // Schema must survive records.
  SchemaRef schema = MakeLeftSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g = MakeProjectOnFilterGraph(0, schema);
  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table", records));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<ProjectOperator>(g->GetNode(2));
  std::vector<Record> result = MakeProjectOnFilterRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), result);
}

TEST(DataFlowGraphTest, TestProjectOnEquiJoinGraph) {
  // Schema must survive records.
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g =
      MakeProjectOnEquiJoinGraph(0, 2, 0, lschema, rschema);
  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table1", left));
  EXPECT_TRUE(g->Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<ProjectOperator>(g->GetNode(3));
  std::vector<Record> result =
      MakeProjectOnEquiJoinRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), result);
}

TEST(DataFlowGraphTest, TestAggregateGraph) {
  // Schema must survive records.
  SchemaRef schema = MakeLeftSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g = MakeAggregateGraph(0, schema);
  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table", records));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<AggregateOperator>(g->GetNode(1));
  std::vector<Record> result = MakeAggregateRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEqualsIndexed(g->outputs().at(0), result, 0);
}

TEST(DataFlowGraphTest, TestAggregateOnEquiJoinGraph) {
  // Schema must survive records.
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g =
      MakeAggregateOnEquiJoinGraph(0, 2, 0, lschema, rschema);
  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table1", left));
  EXPECT_TRUE(g->Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<AggregateOperator>(g->GetNode(3));
  std::vector<Record> result =
      MakeAggregateOnEquiJoinRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEqualsIndexed(g->outputs().at(0), result, 0);
}

TEST(DataFlowGraphTest, TestDiamondGraph) {
  // Schema must survive records.
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g =
      MakeDiamondGraph(0, 2, 0, lschema, rschema);
  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table1", left));
  EXPECT_TRUE(g->Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<AggregateOperator>(g->GetNode(3));
  std::vector<Record> result = MakeDiamondRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEqualsIndexed(g->outputs().at(0), result, 0);
}

// Similar to TestAggregateOnEquiJoinGraph
TEST(DataFlowGraphTest, CloneTest) {
  // Schema must survive records.
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g =
      MakeAggregateOnEquiJoinGraph(0, 2, 0, lschema, rschema);
  auto g_clone = g->Clone();

  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  // Process records.
  EXPECT_TRUE(g_clone->Process("test-table1", left));
  EXPECT_TRUE(g_clone->Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<AggregateOperator>(g_clone->GetNode(3));
  std::vector<Record> result =
      MakeAggregateOnEquiJoinRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEqualsIndexed(g_clone->outputs().at(0), result, 0);
}

TEST(DataFlowGraphTest, InsertTest) {
  // Schema must survive records.
  SchemaRef schema = MakeLeftSchema();
  // Make graph.
  std::shared_ptr<DataFlowGraph> g = MakeProjectOnFilterGraph(0, schema);
  // Insert an identity operator after the filter operator
  auto identity_op = std::make_shared<IdentityOperator>();
  g->InsertNodeAfter(identity_op, g->GetNode(1));
  EXPECT_EQ(g->GetNode(0)->type(), Operator::Type::INPUT);
  EXPECT_EQ(g->GetNode(1)->type(), Operator::Type::FILTER);
  EXPECT_EQ(g->GetNode(2)->type(), Operator::Type::PROJECT);
  EXPECT_EQ(g->GetNode(3)->type(), Operator::Type::MAT_VIEW);
  EXPECT_EQ(g->GetNode(4)->type(), Operator::Type::IDENTITY);
  // Check children
  EXPECT_EQ(g->GetNode(0)->GetChildren()[0], g->GetNode(1));
  EXPECT_EQ(g->GetNode(1)->GetChildren()[0], g->GetNode(4));
  EXPECT_EQ(g->GetNode(4)->GetChildren()[0], g->GetNode(2));
  EXPECT_EQ(g->GetNode(2)->GetChildren()[0], g->GetNode(3));
  // Check parents
  EXPECT_EQ(g->GetNode(3)->GetParents()[0], g->GetNode(2));
  EXPECT_EQ(g->GetNode(2)->GetParents()[0], g->GetNode(4));
  EXPECT_EQ(g->GetNode(4)->GetParents()[0], g->GetNode(1));
  EXPECT_EQ(g->GetNode(1)->GetParents()[0], g->GetNode(0));

  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  // Process records.
  EXPECT_TRUE(g->Process("test-table", records));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<ProjectOperator>(g->GetNode(2));
  std::vector<Record> result = MakeProjectOnFilterRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g->outputs().at(0), result);
}

#ifndef PELTON_VALGRIND_MODE
TEST(RecordTest, TestDuplicateInputGraph) {
  // Create a schema.
  SchemaRef schema = MakeLeftSchema();

  // Make a graph.
  DataFlowGraph g;

  // Add two input operators to the graph for the same table, expect an error.
  auto in1 = std::make_shared<InputOperator>("test-table", schema);
  auto in2 = std::make_shared<InputOperator>("test-table", schema);
  EXPECT_TRUE(g.AddInputNode(in1));
  ASSERT_DEATH({ g.AddInputNode(in2); }, "input already exists");
}
#endif

}  // namespace dataflow
}  // namespace pelton
