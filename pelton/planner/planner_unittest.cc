#include "pelton/planner/planner.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace planner {

using CType = sqlast::ColumnDefinition::Type;

TEST(PlannerTest, SimpleFilter) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaOwner schema{names, types, keys};
  dataflow::SchemaRef schema_ref{schema};

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE Col2 = 'hello!'";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", std::move(schema));

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);
  ShutdownPlanner();

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema_ref, true, 10L, std::move(str1), 20L);
  records.emplace_back(schema_ref, true, 2L, std::move(str2), 50L);
  graph.Process("test_table", records);

  // Look at flow output.
  std::shared_ptr<dataflow::MatViewOperator> output = graph.outputs().at(0);
  EXPECT_EQ(output->count(), 1);
  for (const dataflow::Record &record : *output) {
    EXPECT_EQ(record, records.at(0));
  }
}

}  // namespace planner
}  // namespace pelton
