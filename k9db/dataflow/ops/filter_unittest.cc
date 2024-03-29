#include "k9db/dataflow/ops/filter.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/ints.h"

namespace k9db {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaRef CreateSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

std::vector<Record> CopyVec(const std::vector<Record> &records) {
  std::vector<Record> copy;
  copy.reserve(records.size());
  for (const Record &r : records) {
    copy.push_back(r.Copy());
  }
  return copy;
}

TEST(FilterOperatorTest, SingleAccept) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter1;
  FilterOperator filter2;
  FilterOperator filter3;
  filter1.input_schemas_.push_back(schema);
  filter2.input_schemas_.push_back(schema);
  filter3.input_schemas_.push_back(schema);
  filter1.AddLiteralOperation(0, 5_u, FilterOperator::Operation::LESS_THAN);
  filter2.AddLiteralOperation(1, "Hello!", FilterOperator::Operation::EQUAL);
  filter3.AddLiteralOperation(2, 7_s,
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
  filter.input_schemas_.push_back(schema);
  filter.AddLiteralOperation(0, 5_u, FilterOperator::Operation::LESS_THAN);
  filter.AddLiteralOperation(1, "Hello!", FilterOperator::Operation::EQUAL);
  filter.AddLiteralOperation(2, 7_s,
                             FilterOperator::Operation::GREATER_THAN_OR_EQUAL);

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
  filter.input_schemas_.push_back(schema);
  filter.AddLiteralOperation(1, "Hello!", FilterOperator::Operation::EQUAL);
  filter.AddLiteralOperation(2, 7_s,
                             FilterOperator::Operation::GREATER_THAN_OR_EQUAL);
  filter.AddLiteralOperation(2, 10_s,
                             FilterOperator::Operation::LESS_THAN_OR_EQUAL);

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
  filter.input_schemas_.push_back(schema);
  filter.AddLiteralOperation(1, "Hello!", FilterOperator::Operation::NOT_EQUAL);
  filter.AddLiteralOperation(2, 5_s, FilterOperator::Operation::GREATER_THAN);
  filter.AddLiteralOperation(2, 10_s, FilterOperator::Operation::LESS_THAN);

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
  std::vector<Record> outputs =
      filter.Process(UNDEFINED_NODE_INDEX, CopyVec(records), Promise::None);
  EXPECT_EQ(outputs.size(), 2);
  EXPECT_EQ(outputs.at(0), records.at(1));
  EXPECT_EQ(outputs.at(1), records.at(2));
}

TEST(FilterOperatorTest, ColOps) {
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Create some filter operator.
  FilterOperator filter;
  filter.input_schemas_.push_back(schema);
  filter.AddColumnOperation(0_u, 2_u, FilterOperator::Operation::GREATER_THAN);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_s,
                       std::make_unique<std::string>("Hello!"), 7_s);
  records.emplace_back(schema, true, 4_s,
                       std::make_unique<std::string>("Hello!"), 7_s);
  records.emplace_back(schema, true, 6_s, std::make_unique<std::string>("Bye!"),
                       9_s);
  records.emplace_back(schema, true, 20_s,
                       std::make_unique<std::string>("Bye!"), 15_s);

  // Test filtering out records.
  std::vector<Record> outputs =
      filter.Process(UNDEFINED_NODE_INDEX, CopyVec(records), Promise::None);
  EXPECT_EQ(outputs.size(), 1);
  EXPECT_EQ(outputs.at(0), records.at(3));
}

#ifndef K9DB_VALGRIND_MODE
TEST(FilterOperatorTest, TypeMistmatch) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter1;
  FilterOperator filter2;
  FilterOperator filter3;
  filter1.input_schemas_.push_back(schema);
  filter2.input_schemas_.push_back(schema);
  filter3.input_schemas_.push_back(schema);
  filter1.AddLiteralOperation(2, 5_u, FilterOperator::Operation::LESS_THAN);
  filter2.AddLiteralOperation(0, "Hello!", FilterOperator::Operation::EQUAL);
  filter3.AddLiteralOperation(1, 7_s,
                              FilterOperator::Operation::GREATER_THAN_OR_EQUAL);

  // Create some records.
  Record record{schema};
  record.SetUInt(0, 0);
  record.SetString(std::make_unique<std::string>("Hello!"), 1);
  record.SetInt(-5, 2);

  // Test filtering out records.
  ASSERT_DEATH({ filter1.Accept(record); }, "Type mismatch");
  ASSERT_DEATH({ filter2.Accept(record); }, "Type mismatch");
  ASSERT_DEATH({ filter3.Accept(record); }, "Type mismatch");
}
#endif

TEST(FilterOperatorTest, ImplicitTypeConversion) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter1;
  FilterOperator filter2;
  FilterOperator filter3;
  filter1.input_schemas_.push_back(schema);
  filter2.input_schemas_.push_back(schema);
  filter3.input_schemas_.push_back(schema);
  filter1.AddLiteralOperation(0, 5_s, FilterOperator::Operation::LESS_THAN);
  filter2.AddLiteralOperation(0, 5_s, FilterOperator::Operation::EQUAL);
  filter3.AddLiteralOperation(0, 5_s,
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
  EXPECT_FALSE(filter2.Accept(records.at(0)));
  EXPECT_FALSE(filter3.Accept(records.at(0)));
  EXPECT_FALSE(filter1.Accept(records.at(1)));
  EXPECT_TRUE(filter2.Accept(records.at(1)));
  EXPECT_TRUE(filter3.Accept(records.at(1)));
  EXPECT_FALSE(filter1.Accept(records.at(2)));
  EXPECT_FALSE(filter2.Accept(records.at(2)));
  EXPECT_TRUE(filter3.Accept(records.at(2)));
}

TEST(FilterOperatorTest, IsNullAccept) {
  SchemaRef schema = CreateSchema();

  // Create some filter operators.
  FilterOperator filter1;
  FilterOperator filter2;
  filter1.input_schemas_.push_back(schema);
  filter2.input_schemas_.push_back(schema);
  filter1.AddNullOperation(0, FilterOperator::Operation::IS_NULL);
  filter2.AddNullOperation(1, FilterOperator::Operation::IS_NOT_NULL);

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
}  // namespace k9db
