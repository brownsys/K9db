#include "pelton/dataflow/engine.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/channel.h"
#include "pelton/dataflow/graph_test_utils.h"
#include "pelton/dataflow/message.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/exchange.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/ops/union.h"
#include "pelton/dataflow/partition.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

TEST(DataFlowEngineTest, TestTrivialGraph) {
  DataFlowEngine engine;
  // Make schema
  SchemaRef schema = MakeLeftSchema();
  engine.AddTableSchema("test-table", schema);
  // Make graph
  auto graph = MakeTrivialGraph(2, schema);
  std::string flow_name = "trivial";
  engine.AddFlow(flow_name, graph);

  auto partitions = engine.partitioned_graphs();
  auto input_partitions = engine.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at(flow_name)) {
    EXPECT_EQ(item.second->node_count(), 2);
    EXPECT_EQ(item.second->GetNode(0)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(1)->type(), Operator::Type::MAT_VIEW);
  }
  auto matview_key_cols = std::vector<ColumnID>{2};
  // Check if input op's partitioning citeria is correct
  EXPECT_EQ(input_partitions.at(flow_name).at("test-table"), matview_key_cols);

  // Process records through the sharded dataflow
  std::vector<Record> records = MakeLeftRecords(schema);
  std::vector<Record> records_copy = MakeLeftRecords(schema);
  engine.ProcessRecords("test-table", std::move(std::move(records)));
  // Wait for a while for records to get processed
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // Partition the input records so that equality checks are easier to perform
  auto partitioned_records =
      partition::HashPartition(std::move(records_copy), matview_key_cols, 3);
  // Check if records have reached appropriate partition's matviews
  for (const auto &item : partitioned_records) {
    auto partition_matview =
        engine.GetPartitionedFlow(flow_name, item.first)->outputs().at(0);
    EXPECT_EQ_MSET(partition_matview, item.second);
  }
}

TEST(DataFlowEngineTest, TestEquiJoinGraph) {
  DataFlowEngine engine;
  // Make schemas
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  engine.AddTableSchema("test-table1", lschema);
  engine.AddTableSchema("test-table2", rschema);
  // Make graph
  auto graph = MakeEquiJoinGraph(0, 2, 0, lschema, rschema);
  std::string flow_name = "equijion";
  engine.AddFlow(flow_name, graph);

  auto partitions = engine.partitioned_graphs();
  auto input_partitions = engine.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at(flow_name)) {
    EXPECT_EQ(item.second->node_count(), 5);
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
  EXPECT_EQ(input_partitions.at(flow_name).at("test-table1"),
            std::vector<ColumnID>{2});
  EXPECT_EQ(input_partitions.at(flow_name).at("test-table2"),
            std::vector<ColumnID>{0});

  // Process records through the sharded dataflow
  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  engine.ProcessRecords("test-table1", std::move(left));
  engine.ProcessRecords("test-table2", std::move(right));
  auto join_op = std::dynamic_pointer_cast<EquiJoinOperator>(graph->GetNode(2));
  std::vector<Record> expected_records =
      MakeJoinRecords(join_op->output_schema());
  // Wait for a while for records to get processed
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // Partition the input records so that equality checks are easier to perform
  auto matview_op =
      std::dynamic_pointer_cast<MatViewOperator>(graph->GetNode(3));
  auto partitioned_records = partition::HashPartition(
      std::move(expected_records), matview_op->key_cols(), 3);
  // Check if records have reached appropriate partition's matviews
  for (const auto &item : partitioned_records) {
    auto partition_matview =
        engine.GetPartitionedFlow(flow_name, item.first)->outputs().at(0);
    EXPECT_EQ_MSET(partition_matview, item.second);
  }
}

TEST(DataFlowEngineTest, TestAggregateOnEquiJoinGraph) {
  DataFlowEngine engine;
  // Make schemas
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  engine.AddTableSchema("test-table1", lschema);
  engine.AddTableSchema("test-table2", rschema);
  // Make graph
  auto graph = MakeAggregateOnEquiJoinGraph(0, 2, 0, lschema, rschema);
  std::string flow_name = "aggregateOnEquijoin";
  engine.AddFlow(flow_name, graph);

  auto partitions = engine.partitioned_graphs();
  auto input_partitions = engine.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at(flow_name)) {
    // This graph should not require any exchange operators
    EXPECT_EQ(item.second->node_count(), 5);
    EXPECT_EQ(item.second->GetNode(0)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(1)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(2)->type(), Operator::Type::EQUIJOIN);
    EXPECT_EQ(item.second->GetNode(3)->type(), Operator::Type::AGGREGATE);
    EXPECT_EQ(item.second->GetNode(4)->type(), Operator::Type::MAT_VIEW);
  }
  // Check if input op's partitioning citeria is correct
  EXPECT_EQ(input_partitions.at(flow_name).at("test-table1"),
            std::vector<ColumnID>{2});
  EXPECT_EQ(input_partitions.at(flow_name).at("test-table2"),
            std::vector<ColumnID>{0});

  // Process records through the sharded dataflow
  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  engine.ProcessRecords("test-table1", std::move(left));
  engine.ProcessRecords("test-table2", std::move(right));
  auto aggregate_op =
      std::dynamic_pointer_cast<AggregateOperator>(graph->GetNode(3));
  std::vector<Record> expected_records =
      MakeAggregateOnEquiJoinRecords(aggregate_op->output_schema());
  // Wait for a while for records to get processed
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // Partition the input records so that equality checks are easier to perform
  auto matview_op =
      std::dynamic_pointer_cast<MatViewOperator>(graph->GetNode(4));
  auto partitioned_records = partition::HashPartition(
      std::move(expected_records), matview_op->key_cols(), 3);
  // Check if records have reached appropriate partition's matviews
  for (const auto &item : partitioned_records) {
    auto partition_matview =
        engine.GetPartitionedFlow(flow_name, item.first)->outputs().at(0);
    EXPECT_EQ_MSET(partition_matview, item.second);
  }
}

TEST(DataFlowEngineTest, TestUnionGraph) {
  DataFlowEngine engine;
  // Make schemas
  SchemaRef schema = MakeLeftSchema();
  engine.AddTableSchema("test-table1", schema);
  engine.AddTableSchema("test-table2", schema);
  // Make graph
  auto graph = MakeUnionGraph(0, schema);
  std::string flow_name = "union";
  engine.AddFlow("union", graph);

  auto partitions = engine.partitioned_graphs();
  auto input_partitions = engine.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at("union")) {
    EXPECT_EQ(item.second->node_count(), 4);
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

  // Process records through the sharded dataflow
  // Make records.
  std::vector<Record> records = MakeLeftRecords(schema);
  std::vector<Record> first_half;
  first_half.push_back(records.at(0).Copy());
  first_half.push_back(records.at(1).Copy());
  std::vector<Record> second_half;
  second_half.push_back(records.at(2).Copy());
  second_half.push_back(records.at(3).Copy());
  second_half.push_back(records.at(4).Copy());
  engine.ProcessRecords("test-table1", std::move(first_half));
  engine.ProcessRecords("test-table2", std::move(second_half));
  // Wait for a while for records to get processed
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // Partition the input records so that equality checks are easier to perform
  auto matview_op =
      std::dynamic_pointer_cast<MatViewOperator>(graph->GetNode(3));
  auto partitioned_records =
      partition::HashPartition(std::move(records), matview_op->key_cols(), 3);
  // Check if records have reached appropriate partition's matviews
  for (const auto &item : partitioned_records) {
    auto partition_matview =
        engine.GetPartitionedFlow(flow_name, item.first)->outputs().at(0);
    EXPECT_EQ_MSET(partition_matview, item.second);
  }
}

TEST(DataFlowEngineTest, TestDiamondGraph) {
  DataFlowEngine engine;
  // Make schemas
  SchemaRef lschema = MakeLeftSchema();
  SchemaRef rschema = MakeRightSchema();
  engine.AddTableSchema("test-table1", lschema);
  engine.AddTableSchema("test-table2", rschema);
  // Make graph
  auto graph = MakeDiamondGraph(0, 2, 0, lschema, rschema);
  std::string flow_name = "diamond";
  engine.AddFlow(flow_name, graph);

  auto partitions = engine.partitioned_graphs();
  auto input_partitions = engine.input_partitioned_by();
  // Check if partitions are as expected.
  for (auto item : partitions.at(flow_name)) {
    // This graph should not require any exchange operators
    EXPECT_EQ(item.second->node_count(), 10);
    EXPECT_EQ(item.second->GetNode(0)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(1)->type(), Operator::Type::INPUT);
    EXPECT_EQ(item.second->GetNode(2)->type(), Operator::Type::EQUIJOIN);
    EXPECT_EQ(item.second->GetNode(3)->type(), Operator::Type::AGGREGATE);
    EXPECT_EQ(item.second->GetNode(4)->type(), Operator::Type::PROJECT);
    EXPECT_EQ(item.second->GetNode(5)->type(), Operator::Type::PROJECT);
    EXPECT_EQ(item.second->GetNode(6)->type(), Operator::Type::FILTER);
    EXPECT_EQ(item.second->GetNode(7)->type(), Operator::Type::FILTER);
    EXPECT_EQ(item.second->GetNode(8)->type(), Operator::Type::UNION);
    EXPECT_EQ(item.second->GetNode(9)->type(), Operator::Type::MAT_VIEW);
  }
  // Check if input op's partitioning citeria is correct
  EXPECT_EQ(input_partitions.at(flow_name).at("test-table1"),
            std::vector<ColumnID>{2});
  EXPECT_EQ(input_partitions.at(flow_name).at("test-table2"),
            std::vector<ColumnID>{0});

  // Process records through the sharded dataflow
  // Make records.
  std::vector<Record> left = MakeLeftRecords(lschema);
  std::vector<Record> right = MakeRightRecords(rschema);
  engine.ProcessRecords("test-table1", std::move(left));
  engine.ProcessRecords("test-table2", std::move(right));
  auto aggregate_op =
      std::dynamic_pointer_cast<AggregateOperator>(graph->GetNode(3));
  std::vector<Record> expected_records =
      MakeDiamondRecords(aggregate_op->output_schema());
  // Wait for a while for records to get processed
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  // Partition the input records so that equality checks are easier to perform
  auto matview_op =
      std::dynamic_pointer_cast<MatViewOperator>(graph->GetNode(9));
  auto partitioned_records = partition::HashPartition(
      std::move(expected_records), matview_op->key_cols(), 3);
  // Check if records have reached appropriate partition's matviews
  for (const auto &item : partitioned_records) {
    auto partition_matview =
        engine.GetPartitionedFlow(flow_name, item.first)->outputs().at(0);
    EXPECT_EQ_MSET(partition_matview, item.second);
  }
}

}  // namespace dataflow
}  // namespace pelton
