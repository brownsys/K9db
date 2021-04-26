#include "pelton/planner/planner.h"

#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "glog/logging.h"
#include "gtest/gtest.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace planner {

using CType = sqlast::ColumnDefinition::Type;

// Expects that a matview and vector are equal (as multi-sets).
inline void EXPECT_EQ_MSET(std::shared_ptr<dataflow::MatViewOperator> output,
                           const std::vector<dataflow::Record> &r) {
  std::vector<dataflow::Record> tmp;
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      tmp.push_back(record.Copy());
    }
  }

  for (const auto &v : r) {
    auto it = std::find(tmp.begin(), tmp.end(), v);
    // v must be found in tmp.
    EXPECT_NE(it, tmp.end());
    // Erase v from tmp, ensures that if an equal record is encountered in the
    // future, it will match a different record in r (multiset equality).
    tmp.erase(it);
  }
  EXPECT_TRUE(tmp.empty());
}

TEST(PlannerTest, SimpleFilter) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<dataflow::ColumnID> cols = {0, 1, 2};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE Col2 = 'hello!'";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph.Process("test_table", records);

  // Look at flow output.
  std::shared_ptr<dataflow::MatViewOperator> output = graph.outputs().at(0);
  EXPECT_EQ(output->count(), 1);
  EXPECT_EQ(output->output_schema(), schema);
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      EXPECT_EQ(record, records.at(0));
    }
  }
}

TEST(PlannerTest, SimpleProject) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT Col3 FROM test_table WHERE Col2 = 'hello!'";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph.GetNode(3)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp =
      std::dynamic_pointer_cast<dataflow::ProjectOperator>(graph.GetNode(2));
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(projectOp->output_schema(), true, 20_s);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph.Process("test_table", records);

  // Look at flow output.
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, SimpleProjectLiteral) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "select 1 as `one` from  test_table";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp =
      std::dynamic_pointer_cast<dataflow::ProjectOperator>(graph.GetNode(1));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph.Process("test_table", records);

  // Look at flow output.
  std::shared_ptr<dataflow::MatViewOperator> output = graph.outputs().at(0);
  EXPECT_EQ(projectOp->output_schema().column_names(),
            std::vector<std::string>{"one"});
  EXPECT_EQ(projectOp->output_schema().column_types(),
            std::vector<CType>{CType::INT});
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      EXPECT_EQ(record.GetInt(0), 1);
    }
  }
}

TEST(PlannerTest, ProjectArithmeticRightLiteral) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT Col1, Col3 - 5 as Delta5 FROM test_table";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp =
      std::dynamic_pointer_cast<dataflow::ProjectOperator>(graph.GetNode(1));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph.Process("test_table", records);

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(projectOp->output_schema(), true, 10_s, 15_s);
  expected_records.emplace_back(projectOp->output_schema(), true, 2_s, 45_s);
  std::vector<std::string> expected_col_names = {"Col1", "Delta5"};
  std::vector<CType> expected_col_types = {CType::INT, CType::INT};

  // Look at flow output.
  EXPECT_EQ(projectOp->output_schema().column_names(), expected_col_names);
  EXPECT_EQ(projectOp->output_schema().column_types(), expected_col_types);
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, ProjectArithmeticRightColumn) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT Col3 - Col1 as DeltaCol FROM test_table";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp =
      std::dynamic_pointer_cast<dataflow::ProjectOperator>(graph.GetNode(1));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph.Process("test_table", records);

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(projectOp->output_schema(), true, 10_s);
  expected_records.emplace_back(projectOp->output_schema(), true, 48_s);
  std::vector<std::string> expected_col_names = {"DeltaCol"};
  std::vector<CType> expected_col_types = {CType::INT};

  // // Look at flow output.
  EXPECT_EQ(projectOp->output_schema().column_names(), expected_col_names);
  EXPECT_EQ(projectOp->output_schema().column_types(), expected_col_types);
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, SimpleAggregate) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query =
      "SELECT Col3, COUNT(*) FROM test_table GROUP BY Col3 ORDER BY Col3";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::AGGREGATE);
  EXPECT_EQ(graph.GetNode(3)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto aggregateOp =
      std::dynamic_pointer_cast<dataflow::AggregateOperator>(graph.GetNode(2));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph.Process("test_table", records);

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(aggregateOp->output_schema(), true, 20_s, 2_u);
  expected_records.emplace_back(aggregateOp->output_schema(), true, 50_s, 1_u);
  std::vector<std::string> expected_col_names = {"Col3", "Count"};
  std::vector<CType> expected_col_types = {CType::INT, CType::UINT};

  // Look at flow output.
  EXPECT_EQ(aggregateOp->output_schema().column_names(), expected_col_names);
  EXPECT_EQ(aggregateOp->output_schema().column_types(), expected_col_types);
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, SingleConditionFilter) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE Col3=20";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph.Process("test_table", records);

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(0).Copy());
  expected_records.push_back(records.at(1).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, FilterSingleORCondition) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query =
      "SELECT * FROM test_table WHERE Col3=20 OR Col3=50 ORDER BY Col3";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(3)->type(), dataflow::Operator::Type::UNION);
  EXPECT_EQ(graph.GetNode(4)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph.Process("test_table", records);

  // Look at flow output.
  std::shared_ptr<dataflow::MatViewOperator> output = graph.outputs().at(0);
  int counter = 0;
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      EXPECT_EQ(record, records.at(counter));
      counter++;
    }
  }
}

TEST(PlannerTest, FilterSingleANDCondition) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE Col3=20 AND Col1>5";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph.Process("test_table", records);

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(0).Copy());
  expected_records.push_back(records.at(1).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, FilterNestedORCondition) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query =
      "SELECT * FROM test_table WHERE Col3=20 AND (Col1<12 OR Col1>=20)";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(3)->type(), dataflow::Operator::Type::UNION);
  EXPECT_EQ(graph.GetNode(4)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(5)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph.Process("test_table", records);

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(0).Copy());
  expected_records.push_back(records.at(1).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, FilterNestedANDCondition) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query =
      "SELECT * FROM test_table WHERE Col3=50 OR (Col3=20 AND Col1=20)";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph.GetNode(3)->type(), dataflow::Operator::Type::UNION);
  EXPECT_EQ(graph.GetNode(4)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph.Process("test_table", records);

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(1).Copy());
  expected_records.push_back(records.at(2).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph.outputs().at(0), expected_records);
}

TEST(PlannerTest, UniqueSecondaryIndexFlow) {
  // Create a schema.
  std::vector<std::string> names = {"IndexCol", "ShardByCol", "DataCol"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT IndexCol, ShardByCol FROM test_table";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);
  EXPECT_EQ(graph.GetNode(2).get(), graph.outputs().at(0).get());

  // Materialized View.
  std::shared_ptr<dataflow::MatViewOperator> matview = graph.outputs().at(0);
  EXPECT_EQ(matview->output_schema().column_names(),
            (std::vector<std::string>{"IndexCol", "ShardByCol"}));
  EXPECT_EQ(matview->output_schema().column_types(),
            (std::vector<CType>{CType::INT, CType::TEXT}));
  EXPECT_EQ(matview->output_schema().keys(),
            std::vector<dataflow::ColumnID>{0});

  // Try to process some records through flow.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s,
                       std::make_unique<std::string>("shard1"), 20_s);
  records.emplace_back(schema, true, 20_s,
                       std::make_unique<std::string>("shard2"), 20_s);
  records.emplace_back(schema, true, 30_s,
                       std::make_unique<std::string>("shard3"), 50_s);
  graph.Process("test_table", records);

  // Expected records.
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"));
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"));
  expected_records.emplace_back(matview->output_schema(), true, 30_s,
                                std::make_unique<std::string>("shard3"));

  // Look at flow output.
  EXPECT_EQ_MSET(matview, expected_records);

  // Delete some records see if everything is fine.
  std::vector<dataflow::Record> records2;
  records2.emplace_back(schema, false, 10_s,
                        std::make_unique<std::string>("shard1"), 20_s);
  records2.emplace_back(schema, false, 20_s,
                        std::make_unique<std::string>("shard2"), 20_s);
  records2.emplace_back(schema, true, 100_s,
                        std::make_unique<std::string>("shard5"), 50_s);
  graph.Process("test_table", records2);

  // Expected records.
  std::vector<dataflow::Record> expected_records2;
  expected_records2.emplace_back(expected_records.back().Copy());
  expected_records2.emplace_back(matview->output_schema(), true, 100_s,
                                 std::make_unique<std::string>("shard5"));

  EXPECT_EQ_MSET(matview, expected_records2);
}

TEST(PlannerTest, DuplicatesSecondaryIndexFlow) {
  // Create a schema.
  std::vector<std::string> names = {"PK", "IndexCol", "ShardByCol", "DataCol"};
  std::vector<CType> types = {CType::INT, CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query =
      "SELECT IndexCol as I, ShardByCol as S, COUNT(*) "
      "FROM test_table GROUP BY IndexCol, ShardByCol";

  // Create a dummy state.
  dataflow::DataFlowState state;
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  dataflow::DataFlowGraph graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph.inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph.GetNode(0).get(), graph.inputs().at("test_table").get());
  EXPECT_EQ(graph.GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph.GetNode(2)->type(), dataflow::Operator::Type::AGGREGATE);
  EXPECT_EQ(graph.GetNode(3)->type(), dataflow::Operator::Type::MAT_VIEW);
  EXPECT_EQ(graph.GetNode(3).get(), graph.outputs().at(0).get());

  // Materialized View.
  std::shared_ptr<dataflow::MatViewOperator> matview = graph.outputs().at(0);
  EXPECT_EQ(matview->output_schema().column_names(),
            (std::vector<std::string>{"I", "S", "Count"}));
  EXPECT_EQ(matview->output_schema().column_types(),
            (std::vector<CType>{CType::INT, CType::TEXT, CType::UINT}));
  EXPECT_EQ(matview->output_schema().keys(),
            std::vector<dataflow::ColumnID>{0});

  // Try to process some records through flow.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 0_s, 10_s,
                       std::make_unique<std::string>("shard1"), 20_s);
  records.emplace_back(schema, true, 1_s, 10_s,
                       std::make_unique<std::string>("shard1"), 20_s);
  graph.Process("test_table", records);

  // Check that processing was correct.
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 2_u);
  EXPECT_EQ_MSET(matview, expected_records);

  // Another round of processing and checking.
  records.emplace_back(schema, true, 2_s, 10_s,
                       std::make_unique<std::string>("shard1"), 50_s);
  records.emplace_back(schema, true, 3_s, 20_s,
                       std::make_unique<std::string>("shard2"), 150_s);
  records.emplace_back(schema, true, 2_s, 30_s,
                       std::make_unique<std::string>("shard1"), 200_s);
  graph.Process("test_table", records);

  expected_records.clear();
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 5_u);
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"), 1_u);
  expected_records.emplace_back(matview->output_schema(), true, 30_s,
                                std::make_unique<std::string>("shard1"), 1_u);
  EXPECT_EQ_MSET(matview, expected_records);

  // Another round of processing and checking.
  std::vector<dataflow::Record> records2;
  records2.emplace_back(schema, false, 0_s, 10_s,
                        std::make_unique<std::string>("shard1"), 20_s);
  records2.emplace_back(schema, false, 1_s, 10_s,
                        std::make_unique<std::string>("shard1"), 20_s);
  graph.Process("test_table", records2);

  expected_records.clear();
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 3_u);
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"), 1_u);
  expected_records.emplace_back(matview->output_schema(), true, 30_s,
                                std::make_unique<std::string>("shard1"), 1_u);
  EXPECT_EQ_MSET(matview, expected_records);

  // A last round of processing and checking.
  records2.pop_back();
  records2.emplace_back(schema, false, 2_s, 10_s,
                        std::make_unique<std::string>("shard1"), 50_s);
  records2.emplace_back(schema, false, 2_s, 30_s,
                        std::make_unique<std::string>("shard1"), 200_s);
  graph.Process("test_table", records2);

  expected_records.clear();
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 1_u);
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"), 1_u);
  EXPECT_EQ_MSET(matview, expected_records);
}

}  // namespace planner
}  // namespace pelton
