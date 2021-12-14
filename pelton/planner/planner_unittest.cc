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
#include "pelton/dataflow/ops/matview.h"
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

// Expects that an iterable and vector have the same elements in the same order.
template <typename T>
inline void EXPECT_EQ_ORDER(const std::vector<T> &l, const std::vector<T> &r) {
  EXPECT_EQ(l.size(), r.size());
  for (size_t i = 0; i < l.size(); i++) {
    EXPECT_EQ(l.at(i), r.at(i));
  }
}

// Expects that two vector are equal (as multi-sets).
template <typename T>
inline void EXPECT_EQ_MSET(std::vector<T> &&l, const std::vector<T> &r) {
  for (const T &v : r) {
    auto it = std::find(l.begin(), l.end(), v);
    // v must be found in tmp.
    EXPECT_NE(it, l.end());
    // Erase v from tmp, ensures that if an equal record is encountered in the
    // future, it will match a different record in r (multiset equality).
    l.erase(it);
  }
  EXPECT_TRUE(l.empty());
}

std::vector<dataflow::Record> CopyVec(
    const std::vector<dataflow::Record> &records) {
  std::vector<dataflow::Record> copy;
  copy.reserve(records.size());
  for (const dataflow::Record &r : records) {
    copy.push_back(r.Copy());
  }
  return copy;
}

// Project.
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp = static_cast<dataflow::ProjectOperator *>(graph->GetNode(2));
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(projectOp->output_schema(), true, 20_s);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph->_Process("test_table", std::move(records));

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp = static_cast<dataflow::ProjectOperator *>(graph->GetNode(1));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph->_Process("test_table", std::move(records));

  // Look at flow output.
  dataflow::MatViewOperator *output = graph->outputs().at(0);
  EXPECT_EQ(projectOp->output_schema().column_names(),
            std::vector<std::string>{"one"});
  EXPECT_EQ(projectOp->output_schema().column_types(),
            std::vector<CType>{CType::INT});
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      EXPECT_EQ(record.GetInt(0), 1);
    }
  }

  state.Shutdown();
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp = static_cast<dataflow::ProjectOperator *>(graph->GetNode(1));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph->_Process("test_table", std::move(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(projectOp->output_schema(), true, 10_s, 15_s);
  expected_records.emplace_back(projectOp->output_schema(), true, 2_s, 45_s);
  std::vector<std::string> expected_col_names = {"Col1", "Delta5"};
  std::vector<CType> expected_col_types = {CType::INT, CType::INT};

  // Look at flow output.
  EXPECT_EQ(projectOp->output_schema().column_names(), expected_col_names);
  EXPECT_EQ(projectOp->output_schema().column_types(), expected_col_types);
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp = static_cast<dataflow::ProjectOperator *>(graph->GetNode(1));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph->_Process("test_table", std::move(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(projectOp->output_schema(), true, 10_s);
  expected_records.emplace_back(projectOp->output_schema(), true, 48_s);
  std::vector<std::string> expected_col_names = {"DeltaCol"};
  std::vector<CType> expected_col_types = {CType::INT};

  // // Look at flow output.
  EXPECT_EQ(projectOp->output_schema().column_names(), expected_col_names);
  EXPECT_EQ(projectOp->output_schema().column_types(), expected_col_types);
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
}

// Aggregate.
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::AGGREGATE);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto aggregateOp =
      static_cast<dataflow::AggregateOperator *>(graph->GetNode(2));

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph->_Process("test_table", std::move(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(aggregateOp->output_schema(), true, 20_s, 2_u);
  expected_records.emplace_back(aggregateOp->output_schema(), true, 50_s, 1_u);
  std::vector<std::string> expected_col_names = {"Col3", "Count"};
  std::vector<CType> expected_col_types = {CType::INT, CType::UINT};

  // Look at flow output.
  EXPECT_EQ(aggregateOp->output_schema().column_names(), expected_col_names);
  EXPECT_EQ(aggregateOp->output_schema().column_types(), expected_col_types);
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
}

// Filter.
TEST(PlannerTest, SimpleFilter) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<dataflow::ColumnID> cols = {0, 1, 2};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE 'hello!' = Col2";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph->_Process("test_table", CopyVec(records));

  // Look at flow output.
  dataflow::MatViewOperator *output = graph->outputs().at(0);
  EXPECT_EQ(output->count(), 1);
  EXPECT_EQ(output->output_schema(), schema);
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      EXPECT_EQ(record, records.at(0));
    }
  }

  state.Shutdown();
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph->_Process("test_table", CopyVec(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(0).Copy());
  expected_records.push_back(records.at(1).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::UNION);
  EXPECT_EQ(graph->GetNode(4)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph->_Process("test_table", CopyVec(records));

  // Look at flow output.
  dataflow::MatViewOperator *output = graph->outputs().at(0);
  int counter = 0;
  for (const auto &key : output->Keys()) {
    for (const auto &record : output->Lookup(key)) {
      EXPECT_EQ(record, records.at(counter));
      counter++;
    }
  }

  state.Shutdown();
}

TEST(PlannerTest, FilterSingleANDCondition) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE Col3=20 AND 5<Col1";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph->_Process("test_table", CopyVec(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(0).Copy());
  expected_records.push_back(records.at(1).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
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
      "SELECT * FROM test_table WHERE Col3=20 AND (Col1 < 12 OR Col1 >= 20) "
      " AND (Col1 + 2 < 16 OR Col1 = 20)";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(4)->type(), dataflow::Operator::Type::UNION);
  EXPECT_EQ(graph->GetNode(5)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(6)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(7)->type(), dataflow::Operator::Type::UNION);
  EXPECT_EQ(graph->GetNode(8)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(9)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(10)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str0 = std::make_unique<std::string>("notin!");
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::unique_ptr<std::string> str4 = std::make_unique<std::string>("nono");
  std::unique_ptr<std::string> str5 = std::make_unique<std::string>("bad");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 12_s, std::move(str0), 20_s);
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 21_s, std::move(str3), 20_s);
  records.emplace_back(schema, true, 14_s, std::move(str4), 30_s);
  records.emplace_back(schema, true, 10_s, std::move(str5), 30_s);
  graph->_Process("test_table", std::move(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  dataflow::SchemaRef output_schema = graph->outputs().at(0)->output_schema();
  expected_records.emplace_back(output_schema, true, 10_s,
                                std::make_unique<std::string>("hello!"), 20_s);
  expected_records.emplace_back(output_schema, true, 20_s,
                                std::make_unique<std::string>("bye!"), 20_s);

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::UNION);
  EXPECT_EQ(graph->GetNode(4)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph->_Process("test_table", CopyVec(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(1).Copy());
  expected_records.push_back(records.at(2).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
}

TEST(PlannerTest, FilterColumnComparison) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE Col1 >= Col3";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 20_s, std::move(str2), 20_s);
  records.emplace_back(schema, true, 30_s, std::move(str3), 50_s);
  graph->_Process("test_table", CopyVec(records));

  // Expected records
  std::vector<dataflow::Record> expected_records;
  expected_records.push_back(records.at(1).Copy());

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected_records);

  state.Shutdown();
}

TEST(PlannerTest, FilterArithmeticCondition) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::UINT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query = "SELECT * FROM test_table WHERE 100 > 30 + Col3";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(4)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Output schema should not include any tmp columns.
  dataflow::SchemaRef output_schema = graph->outputs().at(0)->output_schema();
  EXPECT_EQ(output_schema.column_names(), schema.column_names());
  EXPECT_EQ(output_schema.column_types(), schema.column_types());
  EXPECT_EQ(output_schema.keys(), schema.keys());
  EXPECT_EQ(graph->outputs().at(0)->key_cols(),
            std::vector<dataflow::ColumnID>{});

  // Get project operators added by filter and perform deep schema checks.
  auto projectOpBefore =
      static_cast<dataflow::ProjectOperator *>(graph->GetNode(1));
  EXPECT_EQ(
      projectOpBefore->output_schema().column_names(),
      (std::vector<std::string>{"Col1", "Col2", "Col3", "_PELTON_TMP_4"}));
  EXPECT_EQ(
      projectOpBefore->output_schema().column_types(),
      (std::vector<CType>{CType::INT, CType::TEXT, CType::UINT, CType::UINT}));
  EXPECT_EQ(projectOpBefore->output_schema().keys(),
            std::vector<dataflow::ColumnID>{0});

  auto projectOpAfter =
      static_cast<dataflow::ProjectOperator *>(graph->GetNode(3));
  EXPECT_EQ(projectOpAfter->output_schema().column_names(),
            schema.column_names());
  EXPECT_EQ(projectOpAfter->output_schema().column_types(),
            schema.column_types());
  EXPECT_EQ(projectOpAfter->output_schema().keys(), schema.keys());

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 50_u);
  records.emplace_back(schema, true, 20_s, std::move(str2), 70_u);
  records.emplace_back(schema, true, 30_s, std::move(str3), 30_u);
  graph->_Process("test_table", std::move(records));

  // Expected records
  std::unique_ptr<std::string> str4 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str5 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> expected;
  expected.emplace_back(output_schema, true, 10_s, std::move(str4), 50_u);
  expected.emplace_back(output_schema, true, 30_s, std::move(str5), 30_u);

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(), expected);

  state.Shutdown();
}

TEST(PlannerTest, FilterArithmeticConditionTwoColumns) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::UINT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query =
      "SELECT * FROM test_table WHERE Col3 - Col1 < 10 ORDER BY Col1";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::FILTER);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(4)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Output schema should not include any tmp columns.
  dataflow::SchemaRef output_schema = graph->outputs().at(0)->output_schema();
  EXPECT_EQ(output_schema.column_names(), schema.column_names());
  EXPECT_EQ(output_schema.column_types(), schema.column_types());
  EXPECT_EQ(output_schema.keys(), schema.keys());
  EXPECT_EQ(graph->outputs().at(0)->key_cols(),
            std::vector<dataflow::ColumnID>{});

  // Get project operators added by filter and perform deep schema checks.
  auto projectOpBefore =
      static_cast<dataflow::ProjectOperator *>(graph->GetNode(1));
  EXPECT_EQ(
      projectOpBefore->output_schema().column_names(),
      (std::vector<std::string>{"Col1", "Col2", "Col3", "_PELTON_TMP_4"}));
  EXPECT_EQ(
      projectOpBefore->output_schema().column_types(),
      (std::vector<CType>{CType::UINT, CType::TEXT, CType::UINT, CType::INT}));
  EXPECT_EQ(projectOpBefore->output_schema().keys(),
            std::vector<dataflow::ColumnID>{0});

  auto projectOpAfter =
      static_cast<dataflow::ProjectOperator *>(graph->GetNode(3));
  EXPECT_EQ(projectOpAfter->output_schema().column_names(),
            schema.column_names());
  EXPECT_EQ(projectOpAfter->output_schema().column_types(),
            schema.column_types());
  EXPECT_EQ(projectOpAfter->output_schema().keys(), schema.keys());

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str3 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 20_u, std::move(str2), 20_u);
  records.emplace_back(schema, true, 10_u, std::move(str1), 20_u);
  records.emplace_back(schema, true, 30_u, std::move(str3), 25_u);
  graph->_Process("test_table", std::move(records));

  // Expected records
  std::unique_ptr<std::string> str4 = std::make_unique<std::string>("bye!");
  std::unique_ptr<std::string> str5 = std::make_unique<std::string>("nope");
  std::vector<dataflow::Record> expected;
  expected.emplace_back(output_schema, true, 20_u, std::move(str4), 20_u);
  expected.emplace_back(output_schema, true, 30_u, std::move(str5), 25_u);

  // Look at flow output.
  dataflow::MatViewOperator *matview = graph->outputs().at(0);
  EXPECT_EQ_ORDER(matview->Lookup(dataflow::Key(0)), expected);

  state.Shutdown();
}

// Secondary index.
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);
  EXPECT_EQ(graph->GetNode(2), graph->outputs().at(0));

  // Materialized View.
  dataflow::MatViewOperator *matview = graph->outputs().at(0);
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
  graph->_Process("test_table", std::move(records));

  // Expected records.
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"));
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"));
  expected_records.emplace_back(matview->output_schema(), true, 30_s,
                                std::make_unique<std::string>("shard3"));

  // Look at flow output.
  EXPECT_EQ_MSET(matview->All(), expected_records);

  // Delete some records see if everything is fine.
  std::vector<dataflow::Record> records2;
  records2.emplace_back(schema, false, 10_s,
                        std::make_unique<std::string>("shard1"), 20_s);
  records2.emplace_back(schema, false, 20_s,
                        std::make_unique<std::string>("shard2"), 20_s);
  records2.emplace_back(schema, true, 100_s,
                        std::make_unique<std::string>("shard5"), 50_s);
  graph->_Process("test_table", std::move(records2));

  // Expected records.
  std::vector<dataflow::Record> expected_records2;
  expected_records2.emplace_back(expected_records.back().Copy());
  expected_records2.emplace_back(matview->output_schema(), true, 100_s,
                                 std::make_unique<std::string>("shard5"));

  EXPECT_EQ_MSET(matview->All(), expected_records2);

  state.Shutdown();
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
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::AGGREGATE);
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::MAT_VIEW);
  EXPECT_EQ(graph->GetNode(3), graph->outputs().at(0));

  // Materialized View.
  dataflow::MatViewOperator *matview = graph->outputs().at(0);
  EXPECT_EQ(matview->output_schema().column_names(),
            (std::vector<std::string>{"I", "S", "Count"}));
  EXPECT_EQ(matview->output_schema().column_types(),
            (std::vector<CType>{CType::INT, CType::TEXT, CType::UINT}));
  EXPECT_EQ(matview->output_schema().keys(),
            (std::vector<dataflow::ColumnID>{0, 1}));

  // Try to process some records through flow.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 0_s, 10_s,
                       std::make_unique<std::string>("shard1"), 20_s);
  records.emplace_back(schema, true, 1_s, 10_s,
                       std::make_unique<std::string>("shard1"), 20_s);
  graph->_Process("test_table", CopyVec(records));

  // Check that processing was correct.
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 2_u);
  EXPECT_EQ_MSET(matview->All(), expected_records);

  // Another round of processing and checking.
  records.emplace_back(schema, true, 2_s, 10_s,
                       std::make_unique<std::string>("shard1"), 50_s);
  records.emplace_back(schema, true, 3_s, 20_s,
                       std::make_unique<std::string>("shard2"), 150_s);
  records.emplace_back(schema, true, 2_s, 30_s,
                       std::make_unique<std::string>("shard1"), 200_s);
  graph->_Process("test_table", CopyVec(records));

  expected_records.clear();
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 5_u);
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"), 1_u);
  expected_records.emplace_back(matview->output_schema(), true, 30_s,
                                std::make_unique<std::string>("shard1"), 1_u);
  EXPECT_EQ_MSET(matview->All(), expected_records);

  // Another round of processing and checking.
  std::vector<dataflow::Record> records2;
  records2.emplace_back(schema, false, 0_s, 10_s,
                        std::make_unique<std::string>("shard1"), 20_s);
  records2.emplace_back(schema, false, 1_s, 10_s,
                        std::make_unique<std::string>("shard1"), 20_s);
  graph->_Process("test_table", CopyVec(records2));

  expected_records.clear();
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 3_u);
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"), 1_u);
  expected_records.emplace_back(matview->output_schema(), true, 30_s,
                                std::make_unique<std::string>("shard1"), 1_u);
  EXPECT_EQ_MSET(matview->All(), expected_records);

  // A last round of processing and checking.
  records2.pop_back();
  records2.emplace_back(schema, false, 2_s, 10_s,
                        std::make_unique<std::string>("shard1"), 50_s);
  records2.emplace_back(schema, false, 2_s, 30_s,
                        std::make_unique<std::string>("shard1"), 200_s);
  graph->_Process("test_table", CopyVec(records2));

  expected_records.clear();
  expected_records.emplace_back(matview->output_schema(), true, 10_s,
                                std::make_unique<std::string>("shard1"), 1_u);
  expected_records.emplace_back(matview->output_schema(), true, 20_s,
                                std::make_unique<std::string>("shard2"), 1_u);
  EXPECT_EQ_MSET(matview->All(), expected_records);

  state.Shutdown();
}

// End-to-end complex query.
TEST(PlannerTest, ComplexQueryWithKeys) {
  // Create a schema.
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      {"ID", "NAME"}, {CType::INT, CType::TEXT}, {0});
  dataflow::SchemaRef schema2 = dataflow::SchemaFactory::Create(
      {"ID", "ASSIGNMENT"}, {CType::INT, CType::TEXT}, {0});
  dataflow::SchemaRef schema3 = dataflow::SchemaFactory::Create(
      {"ID", "STUDENT_ID", "ASSIGNMENT_ID", "TS"},
      {CType::INT, CType::INT, CType::INT, CType::INT}, {0});

  // Make a dummy query.
  std::string query =
      "SELECT assignments.ID AS aid, students.NAME, students.ID AS sid, "
      "       COUNT(*) "
      "FROM submissions "
      "  JOIN students ON submissions.STUDENT_ID = students.ID "
      "  JOIN assignments ON submissions.ASSIGNMENT_ID = assignments.ID "
      "WHERE students.ID = ? "
      "GROUP BY assignments.ID, students.NAME, students.ID "
      "HAVING assignments.ID = ?";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("students", schema1);
  state.AddTableSchema("assignments", schema2);
  state.AddTableSchema("submissions", schema3);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("students")->input_name(), "students");
  EXPECT_EQ(graph->inputs().at("assignments")->input_name(), "assignments");
  EXPECT_EQ(graph->inputs().at("submissions")->input_name(), "submissions");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("submissions"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2), graph->inputs().at("students"));
  EXPECT_EQ(graph->GetNode(3)->type(), dataflow::Operator::Type::EQUIJOIN);
  EXPECT_EQ(graph->GetNode(4)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(5), graph->inputs().at("assignments"));
  EXPECT_EQ(graph->GetNode(6)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(7)->type(), dataflow::Operator::Type::EQUIJOIN);
  EXPECT_EQ(graph->GetNode(8)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(9)->type(), dataflow::Operator::Type::AGGREGATE);
  EXPECT_EQ(graph->GetNode(10), graph->outputs().at(0));

  // Materialized View.
  dataflow::MatViewOperator *matview = graph->outputs().at(0);
  EXPECT_EQ(matview->output_schema().column_names(),
            (std::vector<std::string>{"aid", "NAME", "sid", "Count"}));
  EXPECT_EQ(
      matview->output_schema().column_types(),
      (std::vector<CType>{CType::INT, CType::TEXT, CType::INT, CType::UINT}));
  EXPECT_EQ(matview->key_cols(), (std::vector<dataflow::ColumnID>{2, 0}));
  EXPECT_EQ(matview->output_schema().keys(),
            (std::vector<dataflow::ColumnID>{0, 1, 2}));

  // Try to process some records through flow.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema1, true, 0_s, std::make_unique<std::string>("s1"));
  records.emplace_back(schema1, true, 1_s, std::make_unique<std::string>("s2"));
  graph->_Process("students", std::move(records));

  records.clear();
  records.emplace_back(schema2, true, 10_s,
                       std::make_unique<std::string>("a1"));
  records.emplace_back(schema2, true, 20_s,
                       std::make_unique<std::string>("a2"));
  graph->_Process("assignments", std::move(records));

  records.clear();
  records.emplace_back(schema3, true, 0_s, 0_s, 10_s, 100_s);
  records.emplace_back(schema3, true, 1_s, 0_s, 20_s, 200_s);
  records.emplace_back(schema3, true, 3_s, 1_s, 10_s, 320_s);
  records.emplace_back(schema3, true, 4_s, 1_s, 10_s, 440_s);
  records.emplace_back(schema3, true, 5_s, 1_s, 10_s, 465_s);
  records.emplace_back(schema3, true, 6_s, 0_s, 10_s, 721_s);
  graph->_Process("submissions", std::move(records));

  // Check that processing was correct.
  dataflow::SchemaRef schema4 = matview->output_schema();

  // All keys.
  std::vector<dataflow::Key> keys;
  keys.emplace_back(2);
  keys.back().AddValue(0_s);
  keys.back().AddValue(10_s);
  keys.emplace_back(2);
  keys.back().AddValue(0_s);
  keys.back().AddValue(20_s);
  keys.emplace_back(2);
  keys.back().AddValue(1_s);
  keys.back().AddValue(10_s);
  keys.emplace_back(2);
  keys.back().AddValue(1_s);
  keys.back().AddValue(20_s);

  // Expected record per key.
  std::vector<std::vector<dataflow::Record>> expected{4};
  expected[0].emplace_back(schema4, true, 10_s,
                           std::make_unique<std::string>("s1"), 0_s, 2_u);
  expected[1].emplace_back(schema4, true, 20_s,
                           std::make_unique<std::string>("s1"), 0_s, 1_u);
  expected[2].emplace_back(schema4, true, 10_s,
                           std::make_unique<std::string>("s2"), 1_s, 3_u);

  // Compare to check everything is good!
  for (size_t i = 0; i < keys.size(); i++) {
    const dataflow::Key &key = keys.at(i);
    EXPECT_EQ_MSET(matview->Lookup(key), expected.at(i));
  }

  state.Shutdown();
}

TEST(PlannerTest, BasicLeftJoin) {
  // Create a schema.
  dataflow::SchemaRef schema1 = dataflow::SchemaFactory::Create(
      {"ID", "NAME"}, {CType::INT, CType::TEXT}, {0});
  dataflow::SchemaRef schema3 = dataflow::SchemaFactory::Create(
      {"ID", "STUDENT_ID", "ASSIGNMENT_ID", "TS"},
      {CType::INT, CType::INT, CType::INT, CType::INT}, {0});

  // Make a dummy query.
  std::string query =
      "SELECT * FROM submissions LEFT JOIN students ON submissions.STUDENT_ID "
      "= students.ID";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("students", schema1);
  state.AddTableSchema("submissions", schema3);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("students")->input_name(), "students");
  EXPECT_EQ(graph->inputs().at("submissions")->input_name(), "submissions");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("submissions"));
  EXPECT_EQ(graph->GetNode(1), graph->inputs().at("students"));
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::EQUIJOIN);
  EXPECT_EQ(graph->GetNode(3), graph->outputs().at(0));

  // Materialized View.
  dataflow::MatViewOperator *matview = graph->outputs().at(0);
  EXPECT_EQ(matview->output_schema().column_names(),
            (std::vector<std::string>{"ID", "STUDENT_ID", "ASSIGNMENT_ID", "TS",
                                      "NAME"}));
  EXPECT_EQ(matview->output_schema().column_types(),
            (std::vector<CType>{CType::INT, CType::INT, CType::INT, CType::INT,
                                CType::TEXT}));

  // Try to process some records through flow.
  std::vector<dataflow::Record> records;
  records.emplace_back(schema3, true, 0_s, 0_s, 10_s, 100_s);
  records.emplace_back(schema3, true, 1_s, 0_s, 20_s, 200_s);
  records.emplace_back(schema3, true, 3_s, 1_s, 10_s, 320_s);
  records.emplace_back(schema3, true, 4_s, 1_s, 10_s, 440_s);
  records.emplace_back(schema3, true, 5_s, 1_s, 10_s, 465_s);
  records.emplace_back(schema3, true, 6_s, 0_s, 10_s, 721_s);
  graph->_Process("submissions", std::move(records));

  // Check for appropriate records with null values for left join
  std::vector<dataflow::Record> expected_records;
  dataflow::SchemaRef schema4 = matview->output_schema();
  expected_records.emplace_back(schema4, true, 0_s, 0_s, 10_s, 100_s,
                                dataflow::NullValue());
  expected_records.emplace_back(schema4, true, 1_s, 0_s, 20_s, 200_s,
                                dataflow::NullValue());
  expected_records.emplace_back(schema4, true, 3_s, 1_s, 10_s, 320_s,
                                dataflow::NullValue());
  expected_records.emplace_back(schema4, true, 4_s, 1_s, 10_s, 440_s,
                                dataflow::NullValue());
  expected_records.emplace_back(schema4, true, 5_s, 1_s, 10_s, 465_s,
                                dataflow::NullValue());
  expected_records.emplace_back(schema4, true, 6_s, 0_s, 10_s, 721_s,
                                dataflow::NullValue());
  EXPECT_EQ_MSET(matview->All(), expected_records);

  records.clear();
  records.emplace_back(schema1, true, 0_s, std::make_unique<std::string>("s1"));
  records.emplace_back(schema1, true, 1_s, std::make_unique<std::string>("s2"));
  graph->_Process("students", std::move(records));

  // Check if matview gets updated as a consequence of negative records
  expected_records.clear();
  expected_records.emplace_back(schema4, true, 0_s, 0_s, 10_s, 100_s,
                                std::make_unique<std::string>("s1"));
  expected_records.emplace_back(schema4, true, 1_s, 0_s, 20_s, 200_s,
                                std::make_unique<std::string>("s1"));
  expected_records.emplace_back(schema4, true, 3_s, 1_s, 10_s, 320_s,
                                std::make_unique<std::string>("s2"));
  expected_records.emplace_back(schema4, true, 4_s, 1_s, 10_s, 440_s,
                                std::make_unique<std::string>("s2"));
  expected_records.emplace_back(schema4, true, 5_s, 1_s, 10_s, 465_s,
                                std::make_unique<std::string>("s2"));
  expected_records.emplace_back(schema4, true, 6_s, 0_s, 10_s, 721_s,
                                std::make_unique<std::string>("s1"));
  EXPECT_EQ_MSET(matview->All(), expected_records);

  state.Shutdown();
}

// Test case with limit and offset.
TEST(PlannerTest, SimpleLimitOffset) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  // Make a dummy query.
  std::string query =
      "SELECT Col3 FROM test_table WHERE Col2 = ? ORDER BY Col3 LIMIT 1";

  // Create a dummy state.
  dataflow::DataFlowState state(0, false);
  state.AddTableSchema("test_table", schema);

  // Plan the graph via calcite.
  auto graph = PlanGraph(&state, query);

  // Check that the graph is what we expect!
  EXPECT_EQ(graph->inputs().at("test_table")->input_name(), "test_table");
  EXPECT_EQ(graph->GetNode(0), graph->inputs().at("test_table"));
  EXPECT_EQ(graph->GetNode(1)->type(), dataflow::Operator::Type::PROJECT);
  EXPECT_EQ(graph->GetNode(2)->type(), dataflow::Operator::Type::MAT_VIEW);

  // Get project operator for performing deep schema checks
  auto projectOp = static_cast<dataflow::ProjectOperator *>(graph->GetNode(1));
  std::vector<dataflow::Record> expected_records;
  expected_records.emplace_back(projectOp->output_schema(), true, 20_s);
  std::vector<dataflow::Record> expected_records2;
  expected_records2.emplace_back(projectOp->output_schema(), true, 20_s);
  expected_records2.emplace_back(projectOp->output_schema(), true, 50_s);

  // Try to process some records through flow.
  std::unique_ptr<std::string> str1 = std::make_unique<std::string>("hello!");
  std::unique_ptr<std::string> str2 = std::make_unique<std::string>("bye!");
  std::vector<dataflow::Record> records;
  records.emplace_back(schema, true, 10_s, std::move(str1), 20_s);
  records.emplace_back(schema, true, 2_s, std::move(str2), 50_s);
  graph->_Process("test_table", std::move(records));

  // Look at flow output.
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(1), expected_records);
  EXPECT_EQ_MSET(graph->outputs().at(0)->All(2), expected_records2);

  state.Shutdown();
}

}  // namespace planner
}  // namespace pelton
