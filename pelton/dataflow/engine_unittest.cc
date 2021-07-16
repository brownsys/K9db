#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/message.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/exchange.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/ops/union.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {
using CType = sqlast::ColumnDefinition::Type;

// Make schemas.
SchemaRef MakeLeftSchema() {
  std::vector<std::string> names = {"ID", "Item", "Category"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

SchemaRef MakeRightSchema() {
  std::vector<std::string> names = {"Category", "Description"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys;
  return SchemaFactory::Create(names, types, keys);
}

std::shared_ptr<DataFlowGraph> MakeTrivialGraph(ColumnID keycol,
                                                const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in = std::make_shared<InputOperator>("test-table", schema);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in));
  EXPECT_TRUE(g->AddOutputOperator(matview, in));
  EXPECT_EQ(g->inputs().size(), 1);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table").get(), in.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeEquiJoinGraph(ColumnID ok, ColumnID lk,
                                                 ColumnID rk,
                                                 const SchemaRef &lschema,
                                                 const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(join, {in1, in2}));
  EXPECT_TRUE(g->AddOutputOperator(matview, join));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeAggregateOnEquiJoinGraph(
    ColumnID ok, ColumnID lk, ColumnID rk, const SchemaRef &lschema,
    const SchemaRef &rschema) {
  std::vector<ColumnID> keys = {ok};
  std::vector<ColumnID> group_columns = {2};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", lschema);
  auto in2 = std::make_shared<InputOperator>("test-table2", rschema);
  auto join = std::make_shared<EquiJoinOperator>(lk, rk);
  auto aggregate = std::make_shared<AggregateOperator>(
      group_columns, AggregateOperator::Function::COUNT, -1);
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(join, {in1, in2}));
  EXPECT_TRUE(g->AddNode(aggregate, join));
  EXPECT_TRUE(g->AddOutputOperator(matview, aggregate));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

std::shared_ptr<DataFlowGraph> MakeUnionGraph(ColumnID keycol,
                                              const SchemaRef &schema) {
  std::vector<ColumnID> keys = {keycol};
  auto g = std::make_shared<DataFlowGraph>();

  auto in1 = std::make_shared<InputOperator>("test-table1", schema);
  auto in2 = std::make_shared<InputOperator>("test-table2", schema);
  auto union_ = std::make_shared<UnionOperator>();
  auto matview = std::make_shared<UnorderedMatViewOperator>(keys);

  EXPECT_TRUE(g->AddInputNode(in1));
  EXPECT_TRUE(g->AddInputNode(in2));
  EXPECT_TRUE(g->AddNode(union_, {in1, in2}));
  EXPECT_TRUE(g->AddOutputOperator(matview, union_));
  EXPECT_EQ(g->inputs().size(), 2);
  EXPECT_EQ(g->outputs().size(), 1);
  EXPECT_EQ(g->inputs().at("test-table1").get(), in1.get());
  EXPECT_EQ(g->inputs().at("test-table2").get(), in2.get());
  EXPECT_EQ(g->outputs().at(0).get(), matview.get());
  return g;
}

TEST(DataFlowEngineTest, TestTrivialGraph) {
  DataFlowState state;
  // Make schema
  SchemaRef schema = MakeLeftSchema();
  state.AddTableSchema("test-table", schema);
  // Make graph
  auto graph = MakeTrivialGraph(2, schema);
  state.AddFlow("trivial", graph);

  auto partitions = state.partitioned_graphs();
  auto input_partitions = state.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at("trivial")) {
    EXPECT_EQ(item.second->GetNode(0)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(1)->type(), Operator::Type::MAT_VIEW);
  }
  // Check if input op's partitioning citeria is correct
  EXPECT_EQ(input_partitions.at("trivial").at("test-table"),
            std::vector<ColumnID>{2});
}

TEST(DataFlowEngineTest, TestEquiJoinGraph) {
  DataFlowState state;
  // Make schemas
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  state.AddTableSchema("test-table1", lschema);
  state.AddTableSchema("test-table2", rschema);
  // Make graph
  auto graph = MakeEquiJoinGraph(0, 2, 0, lschema, rschema);
  state.AddFlow("equijoin", graph);

  auto partitions = state.partitioned_graphs();
  auto input_partitions = state.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at("equijoin")) {
    EXPECT_EQ(item.second->GetNode(0)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(1)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(2)->type(), Operator::Type::EQUIJOIN);
    EXPECT_EQ(item.second->GetNode(3)->type(), Operator::Type::MAT_VIEW);
    EXPECT_EQ(item.second->GetNode(4)->type(), Operator::Type::EXCHANGE);
    // Check if exchange op's partitioning criteria is correct
    auto exchange_op =
        std::dynamic_pointer_cast<ExchangeOperator>(item.second->GetNode(4));
    auto matview_op =
        std::dynamic_pointer_cast<MatViewOperator>(item.second->GetNode(3));
    EXPECT_EQ(exchange_op->partition_key(), matview_op->key_cols());
  }
  // Check if input op's partitioning citeria is correct
  EXPECT_EQ(input_partitions.at("equijoin").at("test-table1"),
            std::vector<ColumnID>{2});
  EXPECT_EQ(input_partitions.at("equijoin").at("test-table2"),
            std::vector<ColumnID>{0});
}

TEST(DataFlowEngineTest, TestAggregateOnEquiJoinGraph) {
  DataFlowState state;
  // Make schemas
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  state.AddTableSchema("test-table1", lschema);
  state.AddTableSchema("test-table2", rschema);
  // Make graph
  auto graph = MakeAggregateOnEquiJoinGraph(0, 2, 0, lschema, rschema);
  state.AddFlow("aggregateOnEquijoin", graph);

  auto partitions = state.partitioned_graphs();
  auto input_partitions = state.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at("aggregateOnEquijoin")) {
    // LOG(INFO) << item.second->DebugString();
    // Based on the graph that is supplied to the engine, it does not require
    // any exchange operators
    EXPECT_EQ(item.second->GetNode(0)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(1)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(2)->type(), Operator::Type::EQUIJOIN);
    EXPECT_EQ(item.second->GetNode(3)->type(), Operator::Type::AGGREGATE);
    EXPECT_EQ(item.second->GetNode(4)->type(), Operator::Type::MAT_VIEW);
  }
  // Check if input op's partitioning citeria is correct
  EXPECT_EQ(input_partitions.at("aggregateOnEquijoin").at("test-table1"),
            std::vector<ColumnID>{2});
  EXPECT_EQ(input_partitions.at("aggregateOnEquijoin").at("test-table2"),
            std::vector<ColumnID>{0});
}

TEST(DataFlowEngineTest, TestUnionGraph) {
  DataFlowState state;
  // Make schemas
  SchemaRef schema = MakeLeftSchema();
  state.AddTableSchema("test-table1", schema);
  state.AddTableSchema("test-table2", schema);
  // Make graph
  auto graph = MakeUnionGraph(0, schema);
  state.AddFlow("union", graph);

  auto partitions = state.partitioned_graphs();
  auto input_partitions = state.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at("union")) {
    EXPECT_EQ(item.second->GetNode(0)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(1)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(2)->type(), Operator::Type::UNION);
    EXPECT_EQ(item.second->GetNode(3)->type(), Operator::Type::MAT_VIEW);
  }
  // Check if input op's partitioning citeria is correct
  EXPECT_EQ(input_partitions.at("union").at("test-table1"),
            std::vector<ColumnID>{0});
  EXPECT_EQ(input_partitions.at("union").at("test-table2"),
            std::vector<ColumnID>{0});
}

}  // namespace dataflow
}  // namespace pelton
