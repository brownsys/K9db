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

inline SchemaOwner CreateSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

TEST(ProjectOperatorTest, BatchTest) {
  SchemaOwner schema = CreateSchema();
  std::vector<ColumnID> cids = {0,1};
  // create project operator..
  ProjectOperator project = ProjectOperator(cids);
  project.input_schemas_.push_back(SchemaRef(schema));
  project.ComputeOutputSchema();

  // Records to be fed
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, (uint64_t)0ULL, std::make_unique<std::string>("Hello!"), -5L);
  records.emplace_back(SchemaRef(schema), true, (uint64_t)5ULL, std::make_unique<std::string>("Bye!"), 7L);
  records.emplace_back(SchemaRef(schema), true, (uint64_t)6ULL, std::make_unique<std::string>("hello!"), 10L);

  // expected output
  std::vector<Record> expected_records;
  expected_records.emplace_back(project.output_schema_, true, (uint64_t)0ULL, std::make_unique<std::string>("Hello!"));
  expected_records.emplace_back(project.output_schema_, true, (uint64_t)5ULL, std::make_unique<std::string>("Bye!"));
  expected_records.emplace_back(project.output_schema_, true, (uint64_t)6ULL, std::make_unique<std::string>("hello!"));

  // Feed records and test
  std::vector<Record> outputs;
  EXPECT_TRUE(project.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  EXPECT_EQ(outputs, expected_records);
}

}  // namespace dataflow
}  // namespace pelton
