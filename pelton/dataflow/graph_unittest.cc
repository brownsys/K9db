#include "pelton/dataflow/graph.h"

#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/graph_partition.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/exchange.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/types.h"
#include "pelton/planner/planner.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// Schemas.
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

// Make records.
inline std::vector<Record> MakeLeftRecords(const SchemaRef &schema) {
  // Allocate some unique_ptrs.
  std::unique_ptr<std::string> si1 = std::make_unique<std::string>("item0");
  std::unique_ptr<std::string> si2 = std::make_unique<std::string>("item1");
  std::unique_ptr<std::string> si3 = std::make_unique<std::string>("item2");
  std::unique_ptr<std::string> si4 = std::make_unique<std::string>("item3");
  std::unique_ptr<std::string> si5 = std::make_unique<std::string>("item4");
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  // Record 1.
  records.at(0).SetUInt(0UL, 0);
  records.at(0).SetString(std::move(si1), 1);
  records.at(0).SetInt(1L, 2);
  // Record 2.
  records.at(1).SetUInt(4UL, 0);
  records.at(1).SetString(std::move(si2), 1);
  records.at(1).SetInt(3L, 2);
  // Record 3.
  records.at(2).SetUInt(5UL, 0);
  records.at(2).SetString(std::move(si3), 1);
  records.at(2).SetInt(5L, 2);
  // Record 4.
  records.at(3).SetUInt(7UL, 0);
  records.at(3).SetString(std::move(si4), 1);
  records.at(3).SetInt(1L, 2);
  // Record 5.
  records.at(4).SetUInt(2UL, 0);
  records.at(4).SetString(std::move(si5), 1);
  records.at(4).SetInt(1L, 2);
  return records;
}
inline std::vector<Record> MakeRightRecords(const SchemaRef &schema) {
  // Allocate some unique_ptrs.
  std::unique_ptr<std::string> sd1 = std::make_unique<std::string>("descrp0");
  std::unique_ptr<std::string> sd2 = std::make_unique<std::string>("descrp1");
  std::unique_ptr<std::string> sd3 = std::make_unique<std::string>("descrp2");
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.emplace_back(schema);
  records.emplace_back(schema);
  // Record 1.
  records.at(0).SetInt(5L, 0);
  records.at(0).SetString(std::move(sd1), 1);
  // Record 2.
  records.at(1).SetInt(1L, 0);
  records.at(1).SetString(std::move(sd2), 1);
  // Record 3.
  records.at(2).SetInt(-2L, 0);
  records.at(2).SetString(std::move(sd3), 1);
  return records;
}
inline std::vector<Record> MakeOutputRecords() {
  // Make output schema.
  std::vector<std::string> names = {"Category", "Count"};
  std::vector<CType> types = {CType::INT, CType::UINT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);
  // Make records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 1L, (uint64_t)3ULL);
  records.emplace_back(schema, true, 5L, (uint64_t)1ULL);
  return records;
}

// Transform query into a flow using planner.
std::unique_ptr<DataFlowGraphPartition> CreateFlow(const std::string &query,
                                                   DataFlowState *state) {
  return planner::PlanGraph(state, query);
}

// Find exchanges.
std::vector<ExchangeOperator *> Exchanges(DataFlowGraphPartition *partition) {
  std::vector<ExchangeOperator *> result;
  for (NodeIndex i = 0; i < partition->Size(); i++) {
    Operator *op = partition->GetNode(i);
    if (op->type() == Operator::Type::EXCHANGE) {
      result.push_back(static_cast<ExchangeOperator *>(op));
    }
  }
  return result;
}

// Compare exchanges to expected.
using ExchangeInfo = std::tuple<PartitionKey, NodeIndex, NodeIndex>;
void ExpectedExchanges(const std::vector<ExchangeOperator *> &exchanges,
                       std::set<ExchangeInfo> &&expected) {
  for (auto exchange : exchanges) {
    EXPECT_EQ(exchange->parents().size(), 1);
    EXPECT_EQ(exchange->children().size(), 1);
    ExchangeInfo info = std::make_tuple(exchange->outkey(),
                                        exchange->parents().front()->index(),
                                        exchange->children().front()->index());
    auto it = expected.find(info);
    EXPECT_NE(it, expected.end());
    expected.erase(it);
  }
  EXPECT_EQ(expected.size(), 0);
}

// Compare expected vector result to matview contents.
inline void MatViewContentsEquals(MatViewOperator *matview,
                                  const std::vector<Record> &records,
                                  ColumnID key) {
  EXPECT_EQ(matview->count(), records.size());
  for (const Record &record : records) {
    EXPECT_EQ(record, matview->Lookup(record.GetValues({key})).front());
  }
}

// Test that partitioned flows work fine.
TEST(DataFlowGraphTest, JoinAggregateFunctionality) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, COUNT(ID) FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE input1.Category = ? "
      "GROUP BY input1.Category";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // Make records and process records.
  state.ProcessRecords("input1", MakeLeftRecords(MakeLeftSchema()));
  state.ProcessRecords("input2", MakeRightRecords(MakeRightSchema()));
  // Compute expected result.
  std::vector<Record> result = MakeOutputRecords();
  std::unordered_map<PartitionIndex, std::vector<Record>> partitioned;
  for (Record &record : result) {
    PartitionIndex partition = record.Hash({0}) % partitions;
    partitioned[partition].push_back(std::move(record));
  }

  // Outputs must be equal per partition.
  for (PartitionIndex i = 0; i < partitions; i++) {
    auto view = graph.partitions_[i]->outputs().at(0);
    MatViewContentsEquals(view, partitioned[i], 0);
  }

  state.Shutdown();
}

// Test that partitioned flows work fine.
TEST(DataFlowGraphTest, JoinAggregateExchangeFunctionality) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, COUNT(ID) FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category "
      "GROUP BY input1.Category "
      "HAVING COUNT(ID) = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // Make records and process records.
  state.ProcessRecords("input1", MakeLeftRecords(MakeLeftSchema()));
  state.ProcessRecords("input2", MakeRightRecords(MakeRightSchema()));
  // Compute expected result.
  std::vector<Record> result = MakeOutputRecords();
  std::unordered_map<PartitionIndex, std::vector<Record>> partitioned;
  for (Record &record : result) {
    PartitionIndex partition = record.Hash({1}) % partitions;
    partitioned[partition].push_back(std::move(record));
  }

  // Outputs must be equal per partition.
  for (PartitionIndex i = 0; i < partitions; i++) {
    auto view = graph.partitions_[i]->outputs().at(0);
    MatViewContentsEquals(view, partitioned[i], 1);
  }

  state.Shutdown();
}

// Test that exchanges were added in the correct locations, and that
// input and output partitioning keys are correct.
// Trivial flows.
TEST(DataFlowGraphTest, TrivialGraphNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query = "SELECT * FROM input1";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}

TEST(DataFlowGraphTest, TrivialGraphWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query = "SELECT * FROM input1 WHERE ID = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}

// Filters.
TEST(DataFlowGraphTest, TrivialFilterGraph) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query = "SELECT * FROM input1 WHERE ID = ? AND Category > 10";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}
TEST(DataFlowGraphTest, TrivialUnionGraphWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 WHERE (((Item = 'it2' OR Item = 'it1') AND "
      "(Category = 1 OR Category = 2)) OR ID > 5) AND ID = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}
TEST(DataFlowGraphTest, TrivialUnionGraphWithNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 WHERE (((Item = 'it2' OR Item = 'it1') AND "
      "(Category = 1 OR Category = 2)) OR ID > 5)";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}

// Aggregate.
TEST(DataFlowGraphTest, AggregateGraphWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT Category, COUNT(ID) FROM input1 GROUP BY Category HAVING "
      "Count(ID) = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({1}, 2, 3)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.outkey_, PartitionKey{1});

  state.Shutdown();
}
TEST(DataFlowGraphTest, AggregateGraphSameKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT Category, COUNT(ID) FROM input1 GROUP BY Category HAVING "
      "Category = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}
TEST(DataFlowGraphTest, AggregateGraphNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT Category, COUNT(ID) FROM input1 GROUP BY Category";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}

// Join.
TEST(DataFlowGraphTest, JoinGraphWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 JOIN input2 ON input1.Category = input2.Category "
      "WHERE ID = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({0}, 2, 3)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}

TEST(DataFlowGraphTest, JoinGraphSameKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 JOIN input2 ON input1.Category = input2.Category "
      "WHERE Description = Item AND input1.Category = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{2});

  state.Shutdown();
}
TEST(DataFlowGraphTest, JoinGraphNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 JOIN input2 ON input1.Category = input2.Category "
      "WHERE Description = Item";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{2});

  state.Shutdown();
}

// Projections.
TEST(DataFlowGraphTest, ReorderingProjectionWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query = "SELECT Item, Category, ID FROM input1 WHERE ID = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{2});

  state.Shutdown();
}

TEST(DataFlowGraphTest, ReorderingProjectionNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query = "SELECT Item, Category, ID FROM input1";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{2});

  state.Shutdown();
}
TEST(DataFlowGraphTest, ChangingProjectionWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query = "SELECT ID, ID + 1 AS calc FROM input1 WHERE ID = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}

TEST(DataFlowGraphTest, ChangingProjectionNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query = "SELECT Item, ID + 1 AS calc FROM input1";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({0, 1}, 1, 2)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, (PartitionKey{0, 1}));

  state.Shutdown();
}

// Join and projection.
TEST(DataFlowGraphTest, JoinReorderProjectWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item AND ID = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({1}, 4, 5)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{1});

  state.Shutdown();
}
TEST(DataFlowGraphTest, JoinReorderProjectNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}
TEST(DataFlowGraphTest, JoinProjectDroppedKeyWithKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input2.Description, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item AND "
      "input1.ID = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({1}, 4, 5)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{1});

  state.Shutdown();
}
TEST(DataFlowGraphTest, JoinProjectDroppedKeyNoKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input2.Description, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({1}, 4, 5)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{1});

  state.Shutdown();
}

// Complex cases.
TEST(DataFlowGraphTest, JoinAggregateKey) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, COUNT(ID) FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item GROUP BY "
      "input1.Category HAVING COUNT(ID) = ?";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({1}, 6, 7)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{1});

  state.Shutdown();
}
TEST(DataFlowGraphTest, JoinAggregateUnion) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, COUNT(ID) FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item GROUP BY "
      "input1.Category HAVING COUNT(ID) > 10 OR input1.Category = 0";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}
TEST(DataFlowGraphTest, UnionJoinAggregateUnionReorderProject) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT COUNT(ID), input1.Category FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE (ID = 1 OR input1.Category = 0) "
      "AND (ID = 3 OR input1.Category = 5) AND Description = Item GROUP BY "
      "input1.Category HAVING COUNT(ID) > 10 OR input1.Category = 0";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  EXPECT_EQ(Exchanges(partition).size(), 0);

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{1});

  state.Shutdown();
}
TEST(DataFlowGraphTest, UnionJoinAggregateUnionDroppingProject) {
  PartitionIndex partitions = 3;

  // Create schemas.
  DataFlowState state(partitions, true);
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT COUNT(ID) FROM input1 JOIN input2 ON input1.Category = "
      "input2.Category WHERE (ID = 1 OR input1.Category = 0) AND (ID = 3 OR "
      "input1.Category = 5) AND Description = Item GROUP BY input1.Category "
      "HAVING COUNT(ID) > 10 OR input1.Category = 0";

  // Create a graph and perform partition key discovery and traversal.
  state.AddFlow("testflow", CreateFlow(query, &state));
  const DataFlowGraph &graph = state.GetFlow("testflow");

  // No exchanges.
  DataFlowGraphPartition *partition = graph.partitions_.front().get();
  ExpectedExchanges(Exchanges(partition), {ExchangeInfo({0}, 16, 17)});

  // Assert input and output partition keys are correct.
  EXPECT_EQ(graph.inkeys_.at("input1"), PartitionKey{2});
  EXPECT_EQ(graph.inkeys_.at("input2"), PartitionKey{0});
  EXPECT_EQ(graph.outkey_, PartitionKey{0});

  state.Shutdown();
}

}  // namespace dataflow
}  // namespace pelton
