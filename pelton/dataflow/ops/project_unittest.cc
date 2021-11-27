#include "pelton/dataflow/ops/project.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/dataflow/value.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaRef CreateSchemaPrimaryKey() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

inline SchemaRef CreateSchemaCompositeKey() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3", "Col4", "Col5"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT, CType::INT,
                              CType::INT};
  std::vector<ColumnID> keys = {1, 4};
  return SchemaFactory::Create(names, types, keys);
}

TEST(ProjectOperatorTest, BatchTestColumn) {
  SchemaRef schema = CreateSchemaPrimaryKey();
  // create project operator..
  ProjectOperator project = ProjectOperator();
  project.input_schemas_.push_back(schema);
  project.AddColumnProjection(schema.NameOf(0), 0);
  project.AddColumnProjection(schema.NameOf(1), 1);
  project.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u,
                       std::make_unique<std::string>("Hello!"), -5_s);
  records.emplace_back(schema, true, 5_u, std::make_unique<std::string>("Bye!"),
                       7_s);
  records.emplace_back(schema, true, 6_u,
                       std::make_unique<std::string>("hello!"), 10_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true, 0_u,
                                std::make_unique<std::string>("Hello!"));
  expected_records.emplace_back(project.output_schema_, true, 5_u,
                                std::make_unique<std::string>("Bye!"));
  expected_records.emplace_back(project.output_schema_, true, 6_u,
                                std::make_unique<std::string>("hello!"));

  // Feed records and test
  std::vector<Record> outputs =
      project.Process(UNDEFINED_NODE_INDEX, std::move(records));
  EXPECT_EQ(outputs, expected_records);
}

TEST(ProjectOperatorTest, BatchTestLiteral) {
  SchemaRef schema = CreateSchemaPrimaryKey();
  // create project operator..
  ProjectOperator project = ProjectOperator();
  project.input_schemas_.push_back(schema);
  project.AddColumnProjection(schema.NameOf(0), 0);
  project.AddLiteralProjection("One", 1_u);
  project.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u,
                       std::make_unique<std::string>("Hello!"), -5_s);
  records.emplace_back(schema, true, 5_u, std::make_unique<std::string>("Bye!"),
                       7_s);
  records.emplace_back(schema, true, 6_u,
                       std::make_unique<std::string>("hello!"), 10_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true, 0_u, 1_u);
  expected_records.emplace_back(project.output_schema_, true, 5_u, 1_u);
  expected_records.emplace_back(project.output_schema_, true, 6_u, 1_u);

  // Feed records and test
  std::vector<Record> outputs =
      project.Process(UNDEFINED_NODE_INDEX, std::move(records));
  EXPECT_EQ(outputs, expected_records);
}

TEST(ProjectOperatorTest, BatchTestOperationRightColumn) {
  SchemaRef schema = CreateSchemaCompositeKey();
  // create project operator..
  ProjectOperator project = ProjectOperator();
  project.input_schemas_.push_back(schema);
  project.AddColumnProjection(schema.NameOf(0), 0);
  project.AddArithmeticRightProjection("Delta", 8_s, 4,
                                       ProjectOperator::Operation::MINUS);
  project.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u,
                       std::make_unique<std::string>("Hello!"), -5_s, 5_s,
                       10_s);
  records.emplace_back(schema, true, 5_u, std::make_unique<std::string>("Bye!"),
                       7_s, 4_s, 8_s);
  records.emplace_back(schema, true, 6_u,
                       std::make_unique<std::string>("hello!"), 10_s, 3_s, 6_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true, 0_u, -2_s);
  expected_records.emplace_back(project.output_schema_, true, 5_u, 0_s);
  expected_records.emplace_back(project.output_schema_, true, 6_u, 2_s);

  // Feed records and test
  std::vector<Record> outputs =
      project.Process(UNDEFINED_NODE_INDEX, std::move(records));
  EXPECT_EQ(outputs, expected_records);
}

TEST(ProjectOperatorTest, BatchTestOperationRightLiteral) {
  SchemaRef schema = CreateSchemaCompositeKey();
  // create project operator..
  ProjectOperator project = ProjectOperator();
  project.input_schemas_.push_back(schema);
  project.AddColumnProjection(schema.NameOf(0), 0);
  project.AddArithmeticLeftProjection("Delta", 0, 5_u,
                                      ProjectOperator::Operation::PLUS);
  project.ComputeOutputSchema();
  // uint MINUS uint would result in int
  EXPECT_EQ(project.output_schema_.TypeOf(1),
            sqlast::ColumnDefinition::Type::UINT);

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u,
                       std::make_unique<std::string>("Hello!"), -5_s, 5_s,
                       10_s);
  records.emplace_back(schema, true, 5_u, std::make_unique<std::string>("Bye!"),
                       7_s, 4_s, 8_s);
  records.emplace_back(schema, true, 6_u,
                       std::make_unique<std::string>("hello!"), 10_s, 3_s, 6_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true, 0_u, 5_u);
  expected_records.emplace_back(project.output_schema_, true, 5_u, 10_u);
  expected_records.emplace_back(project.output_schema_, true, 6_u, 11_u);

  // Feed records and test
  std::vector<Record> outputs =
      project.Process(UNDEFINED_NODE_INDEX, std::move(records));
  EXPECT_EQ(outputs, expected_records);
}

TEST(ProjectOperatorTest, OutputSchemaPrimaryKeyTest) {
  // TEST 1: Primary keyed schema's keycolumn included in projected schema
  SchemaRef schema = CreateSchemaPrimaryKey();
  // create project operator..
  ProjectOperator project1 = ProjectOperator();
  project1.input_schemas_.push_back(schema);
  project1.AddColumnProjection(schema.NameOf(0), 0);
  project1.AddColumnProjection(schema.NameOf(1), 1);
  project1.ComputeOutputSchema();

  // expected data in output schema
  std::vector<std::string> expected_names = {"Col1", "Col2"};
  std::vector<CType> expected_types = {CType::UINT, CType::TEXT};
  std::vector<ColumnID> expected_keys = {0};
  // test schema data
  EXPECT_EQ(project1.output_schema_.column_names(), expected_names);
  EXPECT_EQ(project1.output_schema_.column_types(), expected_types);
  EXPECT_EQ(project1.output_schema_.keys(), expected_keys);

  // TEST 2: Primary keyed schema's keycolumn not included in projected schema
  ProjectOperator project2 = ProjectOperator();
  project2.input_schemas_.push_back(schema);
  project2.AddColumnProjection(schema.NameOf(1), 1);
  project2.ComputeOutputSchema();
  // expected data in output schema
  expected_names = {"Col2"};
  expected_types = {CType::TEXT};
  expected_keys = {};
  EXPECT_EQ(project2.output_schema_.column_names(), expected_names);
  EXPECT_EQ(project2.output_schema_.column_types(), expected_types);
  EXPECT_EQ(project2.output_schema_.keys(), expected_keys);
}

TEST(ProjectOperatorTest, OutputSchemaCompositeKeyTest) {
  // TEST 1: Composite keyed schema's keycolumns included in projected schema
  SchemaRef schema = CreateSchemaCompositeKey();
  // create project operator..
  ProjectOperator project1 = ProjectOperator();
  project1.input_schemas_.push_back(schema);
  project1.AddColumnProjection(schema.NameOf(1), 1);
  project1.AddColumnProjection(schema.NameOf(3), 3);
  project1.AddColumnProjection(schema.NameOf(4), 4);
  project1.ComputeOutputSchema();
  // expected data in output schema
  std::vector<std::string> expected_names = {"Col2", "Col4", "Col5"};
  std::vector<CType> expected_types = {CType::TEXT, CType::INT, CType::INT};
  std::vector<ColumnID> expected_keys = {0, 2};
  // test schema data
  EXPECT_EQ(project1.output_schema_.column_names(), expected_names);
  EXPECT_EQ(project1.output_schema_.column_types(), expected_types);
  EXPECT_EQ(project1.output_schema_.keys(), expected_keys);

  // TEST 2: Subset of composite keyed schema's keycolumns included in projected
  // schema
  ProjectOperator project2 = ProjectOperator();
  project2.input_schemas_.push_back(schema);
  project2.AddColumnProjection(schema.NameOf(0), 0);
  project2.AddColumnProjection(schema.NameOf(1), 1);
  project2.AddColumnProjection(schema.NameOf(2), 2);
  project2.ComputeOutputSchema();
  // expected data in output schema
  expected_names = {"Col1", "Col2", "Col3"};
  expected_types = {CType::UINT, CType::TEXT, CType::INT};
  expected_keys = {};
  EXPECT_EQ(project2.output_schema_.column_names(), expected_names);
  EXPECT_EQ(project2.output_schema_.column_types(), expected_types);
  EXPECT_EQ(project2.output_schema_.keys(), expected_keys);
}

TEST(ProjectOperatorTest, NullValueTest) {
  SchemaRef schema = CreateSchemaPrimaryKey();
  // create project operator..
  ProjectOperator project = ProjectOperator();
  project.AddColumnProjection(schema.NameOf(0), 0);
  project.AddColumnProjection(schema.NameOf(1), 1);
  project.input_schemas_.push_back(schema);
  project.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u,
                       std::make_unique<std::string>("Hello!"), NullValue());
  records.emplace_back(schema, true, 5_u, NullValue(), 7_s);
  records.emplace_back(schema, true, NullValue(),
                       std::make_unique<std::string>("hello!"), 10_s);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true, 0_u,
                                std::make_unique<std::string>("Hello!"));
  expected_records.emplace_back(project.output_schema_, true, 5_u, NullValue());
  expected_records.emplace_back(project.output_schema_, true, NullValue(),
                                std::make_unique<std::string>("hello!"));

  // Feed records and test
  std::vector<Record> outputs =
      project.Process(UNDEFINED_NODE_INDEX, std::move(records));
  EXPECT_EQ(outputs, expected_records);
}

TEST(ProjectOperatorTest, ArithmeticAndNullValueTest) {
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::UINT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // create project operator..
  ProjectOperator project = ProjectOperator();
  project.AddColumnProjection(schema.NameOf(1), 1);
  project.AddColumnProjection(schema.NameOf(0), 0);
  project.AddArithmeticLeftProjection("Ten", 0, 10_u,
                                      ProjectOperator::Operation::MINUS);
  project.AddArithmeticColumnsProjection("Delta", 0, 2,
                                         ProjectOperator::Operation::MINUS);
  project.input_schemas_.push_back(schema);
  project.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u,
                       std::make_unique<std::string>("Hello!"), NullValue());
  records.emplace_back(schema, true, 5_u, NullValue(), 7_u);
  records.emplace_back(schema, true, NullValue(),
                       std::make_unique<std::string>("hello!"), 10_u);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true,
                                std::make_unique<std::string>("Hello!"), 0_u,
                                -10_s, 0_s);
  expected_records.emplace_back(project.output_schema_, true, NullValue(), 5_u,
                                -5_s, -2_s);
  expected_records.emplace_back(project.output_schema_, true,
                                std::make_unique<std::string>("hello!"),
                                NullValue(), -10_s, -10_s);

  // Feed records and test
  std::vector<Record> outputs =
      project.Process(UNDEFINED_NODE_INDEX, std::move(records));
  EXPECT_EQ(outputs, expected_records);
}

}  // namespace dataflow
}  // namespace pelton
