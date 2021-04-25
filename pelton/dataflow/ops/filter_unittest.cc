#include "pelton/dataflow/ops/filter.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaRef CreateSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

TEST(FilterOperatorTest, SingleAccept) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter1;
  FilterOperator filter2;
  FilterOperator filter3;
  filter1.AddOperation(5_u, 0, FilterOperator::Operation::LESS_THAN);
  filter2.AddOperation("Hello!", 1, FilterOperator::Operation::EQUAL);
  filter3.AddOperation(7_s, 2,
                       FilterOperator::Operation::GREATER_THAN_OR_EQUAL);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.at(0).SetUInt(0, 0);
  records.at(0).SetString(std::make_unique<std::string>("Hello!"), 1);
  records.at(0).SetInt(-5, 2);
  records.emplace_back(schema);
  records.at(1).SetUInt(5, 0);
  records.at(1).SetString(std::make_unique<std::string>("Bye!"), 1);
  records.at(1).SetInt(7, 2);
  records.emplace_back(schema);
  records.at(2).SetUInt(6, 0);
  records.at(2).SetString(std::make_unique<std::string>("hello!"), 1);
  records.at(2).SetInt(10, 2);

  // Test filtering out records.
  EXPECT_TRUE(filter1.Accept(records.at(0)));
  EXPECT_TRUE(filter2.Accept(records.at(0)));
  EXPECT_FALSE(filter3.Accept(records.at(0)));
  EXPECT_FALSE(filter1.Accept(records.at(1)));
  EXPECT_FALSE(filter2.Accept(records.at(1)));
  EXPECT_TRUE(filter3.Accept(records.at(1)));
  EXPECT_FALSE(filter1.Accept(records.at(2)));
  EXPECT_FALSE(filter2.Accept(records.at(2)));
  EXPECT_TRUE(filter3.Accept(records.at(2)));
}

TEST(FilterOperatorTest, AndAccept) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter;
  filter.AddOperation(5_u, 0, FilterOperator::Operation::LESS_THAN);
  filter.AddOperation("Hello!", 1, FilterOperator::Operation::EQUAL);
  filter.AddOperation(7_s, 2, FilterOperator::Operation::GREATER_THAN_OR_EQUAL);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.at(0).SetUInt(0, 0);
  records.at(0).SetString(std::make_unique<std::string>("Hello!"), 1);
  records.at(0).SetInt(7, 2);
  records.emplace_back(schema);
  records.at(1).SetUInt(4, 0);
  records.at(1).SetString(std::make_unique<std::string>("Bye!"), 1);
  records.at(1).SetInt(7, 2);
  records.emplace_back(schema);
  records.at(2).SetUInt(6, 0);
  records.at(2).SetString(std::make_unique<std::string>("Hello!"), 1);
  records.at(2).SetInt(10, 2);

  // Test filtering out records.
  EXPECT_TRUE(filter.Accept(records.at(0)));
  EXPECT_FALSE(filter.Accept(records.at(1)));
  EXPECT_FALSE(filter.Accept(records.at(2)));
}

TEST(FilterOperatorTest, SeveralOpsPerColumn) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter;
  filter.AddOperation("Hello!", 1, FilterOperator::Operation::EQUAL);
  filter.AddOperation(7_s, 2, FilterOperator::Operation::GREATER_THAN_OR_EQUAL);
  filter.AddOperation(10_s, 2, FilterOperator::Operation::LESS_THAN_OR_EQUAL);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.at(0).SetUInt(0, 0);
  records.at(0).SetString(std::make_unique<std::string>("Hello!"), 1);
  records.at(0).SetInt(7, 2);
  records.emplace_back(schema);
  records.at(1).SetUInt(4, 0);
  records.at(1).SetString(std::make_unique<std::string>("Bye!"), 1);
  records.at(1).SetInt(7, 2);
  records.emplace_back(schema);
  records.at(2).SetUInt(6, 0);
  records.at(2).SetString(std::make_unique<std::string>("Hello!"), 1);
  records.at(2).SetInt(15, 2);

  // Test filtering out records.
  EXPECT_TRUE(filter.Accept(records.at(0)));
  EXPECT_FALSE(filter.Accept(records.at(1)));
  EXPECT_FALSE(filter.Accept(records.at(2)));
}

TEST(FilterOperatorTest, BatchTest) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter;
  filter.AddOperation("Hello!", 1, FilterOperator::Operation::NOT_EQUAL);
  filter.AddOperation(5_s, 2, FilterOperator::Operation::GREATER_THAN);
  filter.AddOperation(10_s, 2, FilterOperator::Operation::LESS_THAN);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.at(0).SetUInt(0, 0);
  records.at(0).SetString(std::make_unique<std::string>("Hello!"), 1);
  records.at(0).SetInt(7, 2);
  records.emplace_back(schema);
  records.at(1).SetUInt(4, 0);
  records.at(1).SetString(std::make_unique<std::string>("Bye!"), 1);
  records.at(1).SetInt(7, 2);
  records.emplace_back(schema);
  records.at(2).SetUInt(6, 0);
  records.at(2).SetString(std::make_unique<std::string>("Bye!"), 1);
  records.at(2).SetInt(9, 2);
  records.emplace_back(schema);
  records.at(3).SetUInt(6, 0);
  records.at(3).SetString(std::make_unique<std::string>("Bye!"), 1);
  records.at(3).SetInt(15, 2);

  // Test filtering out records.
  std::vector<Record> outputs;
  EXPECT_TRUE(filter.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  EXPECT_EQ(outputs.size(), 2);
  EXPECT_EQ(outputs.at(0), records.at(1));
  EXPECT_EQ(outputs.at(1), records.at(2));
}

#ifndef PELTON_VALGRIND_MODE
TEST(FilterOperatorTest, TypeMistmatch) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter1;
  FilterOperator filter2;
  FilterOperator filter3;
  filter1.AddOperation(5_u, 2, FilterOperator::Operation::LESS_THAN);
  filter2.AddOperation("Hello!", 0, FilterOperator::Operation::EQUAL);
  filter3.AddOperation(7_s, 1,
                       FilterOperator::Operation::GREATER_THAN_OR_EQUAL);

  // Create some records.
  Record record{schema};
  record.SetUInt(0, 0);
  record.SetString(std::make_unique<std::string>("Hello!"), 1);
  record.SetInt(-5, 2);

  // Test filtering out records.
  ASSERT_DEATH({ filter1.Accept(record); }, "Type mistmatch");
  ASSERT_DEATH({ filter2.Accept(record); }, "Type mistmatch");
  ASSERT_DEATH({ filter3.Accept(record); }, "Type mistmatch");
}
#endif

TEST(FilterOperatorTest, IsNullAccept) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter1;
  FilterOperator filter2;
  filter1.AddOperation(0, FilterOperator::Operation::IS_NULL);
  filter2.AddOperation(1, FilterOperator::Operation::IS_NOT_NULL);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema);
  records.at(0).SetUInt(0, 0);
  records.at(0).SetString(std::make_unique<std::string>("Hello!"), 1);
  records.emplace_back(schema);
  records.at(1).SetNull(true, 0);
  records.at(1).SetString(std::make_unique<std::string>("Bye!"), 1);
  records.emplace_back(schema);
  records.at(2).SetUInt(6, 0);
  records.at(2).SetNull(true, 1);

  // Test filtering out records.
  EXPECT_FALSE(filter1.Accept(records.at(0)));
  EXPECT_TRUE(filter2.Accept(records.at(0)));
  EXPECT_TRUE(filter1.Accept(records.at(1)));
  EXPECT_TRUE(filter2.Accept(records.at(1)));
  EXPECT_FALSE(filter1.Accept(records.at(2)));
  EXPECT_FALSE(filter2.Accept(records.at(2)));
}

}  // namespace dataflow
}  // namespace pelton
