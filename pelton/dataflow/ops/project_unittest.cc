#include "pelton/dataflow/ops/project.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaOwner CreateSchemaPrimaryKey() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

inline SchemaOwner CreateSchemaCompositeKey() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3", "Col4", "Col5"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT, CType::INT,
                              CType::INT};
  std::vector<ColumnID> keys = {1, 4};
  return SchemaOwner{names, types, keys};
}

TEST(ProjectOperatorTest, BatchTest) {
  SchemaOwner schema = CreateSchemaPrimaryKey();
  std::vector<ColumnID> cids = {0, 1};
  // create project operator..
  ProjectOperator project = ProjectOperator(cids);
  project.input_schemas_.push_back(SchemaRef(schema));
  project.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, (uint64_t)0ULL,
                       std::make_unique<std::string>("Hello!"), -5L);
  records.emplace_back(SchemaRef(schema), true, (uint64_t)5ULL,
                       std::make_unique<std::string>("Bye!"), 7L);
  records.emplace_back(SchemaRef(schema), true, (uint64_t)6ULL,
                       std::make_unique<std::string>("hello!"), 10L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true, (uint64_t)0ULL,
                                std::make_unique<std::string>("Hello!"));
  expected_records.emplace_back(project.output_schema_, true, (uint64_t)5ULL,
                                std::make_unique<std::string>("Bye!"));
  expected_records.emplace_back(project.output_schema_, true, (uint64_t)6ULL,
                                std::make_unique<std::string>("hello!"));

  // Feed records and test
  std::vector<Record> outputs;
  EXPECT_TRUE(project.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  EXPECT_EQ(outputs, expected_records);
}

TEST(ProjectOperatorTest, OutputSchemaPrimaryKeyTest) {
  // TEST 1: Primary keyed schema's keycolumn included in projected schema
  SchemaOwner schema = CreateSchemaPrimaryKey();
  std::vector<ColumnID> cids = {0, 1};
  // create project operator..
  ProjectOperator project1 = ProjectOperator(cids);
  project1.input_schemas_.push_back(SchemaRef(schema));
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
  cids = {1};
  ProjectOperator project2 = ProjectOperator(cids);
  project2.input_schemas_.push_back(SchemaRef(schema));
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
  SchemaOwner schema = CreateSchemaCompositeKey();
  std::vector<ColumnID> cids = {1, 3, 4};
  // create project operator..
  ProjectOperator project1 = ProjectOperator(cids);
  project1.input_schemas_.push_back(SchemaRef(schema));
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
  cids = {0, 1, 2};
  ProjectOperator project2 = ProjectOperator(cids);
  project2.input_schemas_.push_back(SchemaRef(schema));
  project2.ComputeOutputSchema();
  // expected data in output schema
  expected_names = {"Col1", "Col2", "Col3"};
  expected_types = {CType::UINT, CType::TEXT, CType::INT};
  expected_keys = {};
  EXPECT_EQ(project2.output_schema_.column_names(), expected_names);
  EXPECT_EQ(project2.output_schema_.column_types(), expected_types);
  EXPECT_EQ(project2.output_schema_.keys(), expected_keys);
}

}  // namespace dataflow
}  // namespace pelton
