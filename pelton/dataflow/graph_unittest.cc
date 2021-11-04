#include "pelton/dataflow/graph.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "pelton/dataflow/graph_partition.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/types.h"
#include "pelton/planner/planner.h"
#include "pelton/sqlast/ast.h"
/*#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
*/

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

// Transform query into a flow using planner.
std::unique_ptr<DataFlowGraphPartition> CreateFlow(
    const std::string &query, dataflow::DataFlowState *state) {
  return planner::PlanGraph(state, query);
}

// Trivial flows.
TEST(DataFlowGraphTest, TrivialGraphNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query = "SELECT * FROM input1";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, TrivialGraphWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query = "SELECT * FROM input1 WHERE ID = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

// Filters.
TEST(DataFlowGraphTest, TrivialFilterGraph) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query = "SELECT * FROM input1 WHERE ID = ? AND Category > 10";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, TrivialUnionGraphWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 WHERE (((Item = 'it2' OR Item = 'it1') AND "
      "(Category = 1 OR Category = 2)) OR ID > 5) AND ID = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, TrivialUnionGraphWithNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 WHERE (((Item = 'it2' OR Item = 'it1') AND "
      "(Category = 1 OR Category = 2)) OR ID > 5)";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

// Aggregate.
TEST(DataFlowGraphTest, AggregateGraphWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT Category, COUNT(ID) FROM input1 GROUP BY Category HAVING "
      "Count(ID) = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, AggregateGraphSameKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT Category, COUNT(ID) FROM input1 GROUP BY Category HAVING "
      "Category = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, AggregateGraphNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT Category, COUNT(ID) FROM input1 GROUP BY Category";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

// Join.
TEST(DataFlowGraphTest, JoinGraphWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 JOIN input2 ON input1.Category = input2.Category "
      "WHERE ID = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

TEST(DataFlowGraphTest, JoinGraphSameKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 JOIN input2 ON input1.Category = input2.Category "
      "WHERE Description = Item AND input1.Category = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, JoinGraphNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT * FROM input1 JOIN input2 ON input1.Category = input2.Category "
      "WHERE Description = Item";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

// Projections.
TEST(DataFlowGraphTest, ReorderingProjectionWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query = "SELECT Item, Category, ID FROM input1 WHERE ID = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, ReorderingProjectionNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query = "SELECT Item, Category, ID FROM input1";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, ChangingProjectionWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query = "SELECT ID, ID + 1 AS calc FROM input1 WHERE ID = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, ChangingProjectionNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query = "SELECT Item, ID + 1 AS calc FROM input1";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

// Join and projection.
TEST(DataFlowGraphTest, JoinReorderProjectWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item AND ID = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, JoinReorderProjectNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, JoinProjectDroppedKeyWithKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input2.Description, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item AND "
      "input1.ID = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, JoinProjectDroppedKeyNoKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input2.Description, input1.ID FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

// Complex cases.
TEST(DataFlowGraphTest, JoinAggregateKey) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, COUNT(ID) FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item GROUP BY "
      "input1.Category HAVING COUNT(ID) = ?";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, JoinAggregateUnion) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT input1.Category, COUNT(ID) FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE Description = Item GROUP BY "
      "input1.Category HAVING COUNT(ID) > 10 OR input1.Category = 0";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, UnionJoinAggregateUnionReorderProject) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT COUNT(ID), input1.Category FROM input1 JOIN input2 ON "
      "input1.Category = input2.Category WHERE (ID = 1 OR input1.Category = 0) "
      "AND (ID = 3 OR input1.Category = 5) AND Description = Item GROUP BY "
      "input1.Category HAVING COUNT(ID) > 10 OR input1.Category = 0";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}
TEST(DataFlowGraphTest, UnionJoinAggregateUnionDroppingProject) {
  // Create schemas.
  DataFlowState state;
  state.AddTableSchema("input1", MakeLeftSchema());
  state.AddTableSchema("input2", MakeRightSchema());

  // Turn query into a flow.
  std::string query =
      "SELECT COUNT(ID) FROM input1 JOIN input2 ON input1.Category = "
      "input2.Category WHERE (ID = 1 OR input1.Category = 0) AND (ID = 3 OR "
      "input1.Category = 5) AND Description = Item GROUP BY input1.Category "
      "HAVING COUNT(ID) > 10 OR input1.Category = 0";
  std::unique_ptr<DataFlowGraphPartition> partition = CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;

  // Create a graph and perform partition key discovery and traversal.
  DataFlowGraph graph;
  graph.Initialize(std::move(partition), 3);
}

}  // namespace dataflow
}  // namespace pelton
