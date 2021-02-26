#include "pelton/planner/planner.h"

#include <iostream>
#include <memory>
#include <string>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"

int main(int argc, char **argv) {
  // Make a dummy query.
  std::string dummy_query = "";

  // Create a dummy state.
  pelton::dataflow::DataflowState state;
  state.AddTableSchema("submissions",
                       pelton::dataflow::SchemaOwner({}, {}, {}));

  // Plan the graph via calcite.
  pelton::dataflow::DataFlowGraph graph =
      pelton::planner::PlanGraph(&state, dummy_query);

  // Check that the graph is what we expect!
  std::shared_ptr<pelton::dataflow::Operator> op = graph.GetNode(0);
  std::shared_ptr<pelton::dataflow::InputOperator> input =
      graph.inputs().at("submissions");
  std::cout << input->input_name() << std::endl;
  CHECK(op.get() == input.get()) << "Operator and InputOperator are different";

  // All good.
  return 0;
}

/*
TEST(DataFlowGraphTest, TestEquiJoinGraph) {
  // Schema must survive records.
  SchemaOwner lschema = MakeLeftSchema();
  SchemaOwner rschema = MakeRightSchema();
  // Make graph.
  DataFlowGraph g =
      MakeEquiJoinGraph(0, 2, 0, SchemaRef(lschema), SchemaRef(rschema));
  // Make records.
  std::vector<Record> left = MakeLeftRecords(SchemaRef(lschema));
  std::vector<Record> right = MakeRightRecords(SchemaRef(rschema));
  // Process records.
  EXPECT_TRUE(g.Process("test-table1", left));
  EXPECT_TRUE(g.Process("test-table2", right));
  // Compute expected result.
  auto op = std::dynamic_pointer_cast<EquiJoinOperator>(g.GetNode(2));
  std::vector<Record> result = MakeJoinRecords(op->output_schema());
  // Outputs must be equal.
  MatViewContentsEquals(g.outputs().at(0), result);
}
*/
