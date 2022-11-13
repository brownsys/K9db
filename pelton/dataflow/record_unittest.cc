#include "pelton/dataflow/record.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// Test record size.
TEST(RecordTest, Size) {
  // should be 40 bytes.
  EXPECT_EQ(sizeof(Record), 32);
}

// Tests setting and getting data from record.
TEST(RecordTest, DataRep) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> ptr = std::make_unique<std::string>("hello");
  std::string *v1 = ptr.get();  // Does not release ownership.
  int64_t v2 = -20;

  // Make the record and test.
  Record record{schema};
  record.SetUInt(v0, 0);
  record.SetString(std::move(ptr), 1);
  record.SetInt(v2, 2);

  EXPECT_EQ(record.GetUInt(0), v0);
  EXPECT_EQ(&record.GetString(1), v1);  // pointer/address equality.
  EXPECT_EQ(record.GetString(1), *v1);  // deep equality
  EXPECT_EQ(record.GetInt(2), v2);

  // Using GetValue()...
  EXPECT_EQ(record.GetValue(0).type(), sqlast::Value::Type::UINT);
  EXPECT_EQ(record.GetValue(0).GetUInt(), v0);
  EXPECT_EQ(record.GetValue(1).type(), sqlast::Value::Type::TEXT);
  EXPECT_EQ(record.GetValue(1).GetString(), *v1);
  EXPECT_EQ(record.GetValue(2).type(), sqlast::Value::Type::INT);
  EXPECT_EQ(record.GetValue(2).GetInt(), v2);
}

// Tests setting and getting data from record using variadic parameter pack.
TEST(RecordTest, VariadicDataRep) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> ptr = std::make_unique<std::string>("hello");
  std::string *v1 = ptr.get();  // Does not release ownership.
  int64_t v2 = -20;

  // Make the record and test.
  Record record{schema};
  record.SetData(v0, std::move(ptr), v2);

  EXPECT_EQ(record.GetUInt(0), v0);
  EXPECT_EQ(&record.GetString(1), v1);  // pointer/address equality.
  EXPECT_EQ(record.GetString(1), *v1);  // deep equality
  EXPECT_EQ(record.GetInt(2), v2);
}

// Tests setting and getting data from record using variadic constructor.
TEST(RecordTest, VariadicConstructor) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> ptr = std::make_unique<std::string>("hello");
  std::string *v1 = ptr.get();  // Does not release ownership.
  int64_t v2 = -20;

  // Make the record and test.
  Record record{schema, false, v0, std::move(ptr), v2};
  EXPECT_EQ(record.GetUInt(0), v0);
  EXPECT_EQ(&record.GetString(1), v1);  // pointer/address equality.
  EXPECT_EQ(record.GetString(1), *v1);  // deep equality
  EXPECT_EQ(record.GetInt(2), v2);
  EXPECT_EQ(record.IsPositive(), false);
}

TEST(RecordTest, NulLValues) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> ptr = std::make_unique<std::string>("hello");
  std::string *v1 = ptr.get();  // Does not release ownership.
  int64_t v2 = -20;

  // Make the record and test.
  Record record{schema};
  record.SetUInt(v0, 0);
  record.SetString(std::move(ptr), 1);
  record.SetInt(v2, 2);
  record.SetNull(true, 0);
  record.SetNull(false, 1);
  record.SetNull(true, 2);

  EXPECT_TRUE(record.IsNull(0));
  EXPECT_FALSE(record.IsNull(1));
  EXPECT_EQ(&record.GetString(1), v1);  // pointer/address equality.
  EXPECT_EQ(record.GetString(1), *v1);  // deep equality
  EXPECT_TRUE(record.IsNull(2));

  EXPECT_TRUE(record.GetValue(0).IsNull());
  EXPECT_EQ(record.GetValue(0).type(), sqlast::Value::Type::_NULL);
  EXPECT_EQ(record.GetValue(1).GetString(), "hello");
  EXPECT_EQ(record.GetValue(1).type(), sqlast::Value::Type::TEXT);
  EXPECT_TRUE(record.GetValue(2).IsNull());
  EXPECT_EQ(record.GetValue(2).type(), sqlast::Value::Type::_NULL);
}

// Tests setting and getting data from record using variadic constructor.
#ifndef PELTON_VALGRIND_MODE
TEST(RecordTest, VariadicConstructorTypeMistmatch) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> ptr = std::make_unique<std::string>("hello");
  int64_t v2 = -20;

  // Make the record with bad data types and make sure we die.
  ASSERT_DEATH({ Record record(schema, false, v0, v2, std::move(ptr)); },
               "Type mismatch");
  ASSERT_DEATH({ Record record(schema, false, v0, std::move(ptr), v2, v2); },
               "too many");
  ASSERT_DEATH({ Record record(schema, false, v0); }, "too few");
}
#endif

// Tests null values using variadic constructor.
#ifndef PELTON_VALGRIND_MODE
TEST(RecordTest, VariadicConstructorNullValue) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> ptr = std::make_unique<std::string>("hello");
  NullValue n;

  Record r1(schema, false, n, std::move(ptr));
  Record r2(schema, false, v0, n);

  EXPECT_TRUE(r1.IsNull(0));
  EXPECT_FALSE(r1.IsNull(1));
  EXPECT_TRUE(r2.IsNull(1));
  EXPECT_FALSE(r2.IsNull(0));
}
#endif

// Tests record equality.
TEST(RecordTest, Comparisons) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  uint64_t v1 = 42;
  uint64_t v2 = 43;
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("hello");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("bye");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("bye");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("hello");

  Record r1{schema};
  r1.SetUInt(v1, 0);
  r1.SetString(std::move(s1), 1);

  Record r2{schema};
  r2.SetUInt(v2, 0);
  r2.SetString(std::move(s2), 1);

  Record r3{schema};
  r3.SetUInt(v1, 0);
  r3.SetString(std::move(s3), 1);

  Record r4{schema};
  r4.SetUInt(v1, 0);  // string content left empty.

  Record r5{schema};
  r5.SetUInt(v1, 0);
  r5.SetString(std::move(s4), 1);  // Value equal but not pointer equal.

  // Record equality is reflexive.
  EXPECT_EQ(r1, r1);
  EXPECT_EQ(r2, r2);
  EXPECT_EQ(r3, r3);
  EXPECT_EQ(r4, r4);
  EXPECT_EQ(r5, r5);
  // Records that are unequal appear unequal.
  EXPECT_NE(r1, r2);
  EXPECT_NE(r1, r3);
  EXPECT_NE(r1, r4);
  EXPECT_NE(r2, r1);
  EXPECT_NE(r2, r3);
  EXPECT_NE(r2, r4);
  EXPECT_NE(r2, r5);
  EXPECT_NE(r3, r1);
  EXPECT_NE(r3, r2);
  EXPECT_NE(r3, r4);
  EXPECT_NE(r3, r5);
  EXPECT_NE(r4, r1);
  EXPECT_NE(r4, r2);
  EXPECT_NE(r4, r3);
  EXPECT_NE(r4, r5);
  EXPECT_NE(r5, r2);
  EXPECT_NE(r5, r3);
  EXPECT_NE(r5, r4);
  // Equal records are equal.
  EXPECT_EQ(r1, r5);
  EXPECT_EQ(r5, r1);
}

// Test getting data with wrong types from record.
TEST(RecordTest, GetTypeMismatch) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> v1 = std::make_unique<std::string>("hello");

  Record record{schema};
  record.SetUInt(v0, 0);
  record.SetString(std::move(v1), 1);

#ifndef PELTON_VALGRIND_MODE
  ASSERT_DEATH({ record.GetString(0); }, "Type mismatch");
  ASSERT_DEATH({ record.GetUInt(1); }, "Type mismatch");
  ASSERT_DEATH({ record.GetInt(1); }, "Type mismatch");
#endif
}

// Test setting data with wrong types from record.
#ifndef PELTON_VALGRIND_MODE
TEST(RecordTest, SetTypeMismatch) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> v1 = std::make_unique<std::string>("hello");

  Record record{schema};
  ASSERT_DEATH({ record.SetUInt(v0, 1); }, "Type mismatch");
  ASSERT_DEATH({ record.SetString(std::move(v1), 0); }, "Type mismatch");
}
#endif

// Test negative integers.
TEST(RecordTest, NegativeIntegers) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  Record r{schema};
  r.SetInt(-7, 0);
  r.SetInt(-1000, 1);
  EXPECT_EQ(r.GetInt(0), -7);
  EXPECT_EQ(r.GetInt(1), -1000);
}

// Test getting the key data from record.
TEST(RecordTest, GetKey) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys1 = {1, 0};
  std::vector<ColumnID> keys2 = {1};
  SchemaRef schema1 = SchemaFactory::Create(names, types, keys1);
  SchemaRef schema2 = SchemaFactory::Create(names, types, keys2);

  // Some values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("hello");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("bye");
  // Some keys.
  int64_t k12 = -10;
  std::string k11 = *s1;
  std::string k2 = *s2;

  // Create two records.
  Record r1{schema1};
  r1.SetInt(k12, 0);
  r1.SetString(std::move(s1), 1);
  Record r2{schema2};
  r2.SetInt(500, 0);
  r2.SetString(std::move(s2), 1);
  Record r3{schema2};
  r3.SetInt(500, 0);

  // Test key types.
  EXPECT_EQ(r1.GetKey().value(0).type(), sqlast::Value::Type::TEXT);
  EXPECT_EQ(r1.GetKey().value(1).type(), sqlast::Value::Type::INT);
  EXPECT_EQ(r2.GetKey().value(0).type(), sqlast::Value::Type::TEXT);
  EXPECT_EQ(r3.GetKey().value(0).type(), sqlast::Value::Type::TEXT);

  // Test keys.
  EXPECT_EQ(r1.GetKey().value(0).GetString(), k11);
  EXPECT_EQ(r1.GetKey().value(1).GetInt(), k12);
  EXPECT_EQ(r2.GetKey().value(0).GetString(), k2);
  EXPECT_EQ(r3.GetKey().value(0).GetString(), "");
}

// Test getting values by columns from record.
TEST(RecordTest, GetValues) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Create a record.
  std::unique_ptr<std::string> s = std::make_unique<std::string>("hello");
  Record record{schema, true, 0_u, std::move(s), -10_s};

  // Get values.
  Key vals1 = record.GetValues({0});
  Key vals2 = record.GetValues({0, 2});
  Key vals3 = record.GetValues({2, 1});

  // Check values are what we expect.
  EXPECT_EQ(vals1.size(), 1);
  EXPECT_EQ(vals1.value(0).GetUInt(), 0_u);

  EXPECT_EQ(vals2.size(), 2);
  EXPECT_EQ(vals2.value(0).GetUInt(), 0_u);
  EXPECT_EQ(vals2.value(1).GetInt(), -10_s);

  EXPECT_EQ(vals3.size(), 2);
  EXPECT_EQ(vals3.value(0).GetInt(), -10_s);
  EXPECT_EQ(vals3.value(1).GetString(), "hello");
}

// Test move and copy.
TEST(RecordTest, RecordMoveConstructor) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Values
  int64_t v0 = 5;
  std::unique_ptr<std::string> v1 = std::make_unique<std::string>("hello");
  std::string *str = v1.get();

  // Create a record.
  Record record{schema};
  record.SetInt(v0, 0);
  record.SetString(std::move(v1), 1);

  // Move record, must get correct values.
  Record record2{std::move(record)};
  EXPECT_EQ(record2.GetInt(0), v0);
  EXPECT_EQ(record2.GetString(1), *str);
  EXPECT_EQ(&record2.GetString(1), str);

#ifndef PELTON_VALGRIND_MODE
  // Accessing moved record must cause error.
  ASSERT_DEATH({ record.GetInt(0); }, "");
  ASSERT_DEATH({ record.GetString(1); }, "");
#endif

  // This also calls the move constructor.
  Record record3 = std::move(record2);
  EXPECT_EQ(record3.GetInt(0), v0);
  EXPECT_EQ(record3.GetString(1), *str);
  EXPECT_EQ(&record3.GetString(1), str);

#ifndef PELTON_VALGRIND_MODE
  // Accessing moved record must cause error.
  ASSERT_DEATH({ record2.GetInt(0); }, "");
  ASSERT_DEATH({ record2.GetString(1); }, "");
#endif
}

TEST(RecordTest, RecordMoveAssignment) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  std::vector<ColumnID> keys2 = {1};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);
  SchemaRef schema2 = SchemaFactory::Create(names, types, keys2);

  // Values
  int64_t v0 = 5;
  std::unique_ptr<std::string> v1 = std::make_unique<std::string>("hello");
  std::unique_ptr<std::string> v2 = std::make_unique<std::string>("bye");
  std::string *str = v1.get();

  // Create a record.
  Record record{schema};
  record.SetInt(v0, 0);
  record.SetString(std::move(v1), 1);

  // Create another record.
  Record record2{schema};
  record2.SetInt(v0, 0);
  record2.SetString(std::move(v2), 1);

  // Move into record2.
  record2 = std::move(record);
  EXPECT_EQ(record2.GetInt(0), v0);
  EXPECT_EQ(record2.GetString(1), *str);
  EXPECT_EQ(&record2.GetString(1), str);

#ifndef PELTON_VALGRIND_MODE
  // Accessing moved record must cause error.
  ASSERT_DEATH({ record.GetInt(0); }, "");
  ASSERT_DEATH({ record.GetString(1); }, "");
  // With valgrind, we can check that v2 is freed properly.

  // Move into record with different schema should fail.
  Record record3{schema2};
  ASSERT_DEATH({ record3 = std::move(record2); },
               "Bad move assign record schema");
#endif
}

TEST(RecordTest, ExplicitCopy) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<ColumnID> keys = {0};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Values
  int64_t v0 = 5;
  std::unique_ptr<std::string> v1 = std::make_unique<std::string>("hello");
  std::string *str = v1.get();

  // Create a record.
  Record record{schema, false};
  record.SetInt(v0, 0);
  record.SetString(std::move(v1), 1);

  // Create another record.
  Record record2 = record.Copy();

  // All values are equal, but strings are different copies.
  EXPECT_EQ(record.GetInt(0), v0);
  EXPECT_EQ(record.GetString(1), *str);
  EXPECT_EQ(&record.GetString(1), str);
  EXPECT_EQ(record2.GetInt(0), v0);
  EXPECT_EQ(record2.GetString(1), *str);
  EXPECT_NE(&record2.GetString(1), str);
  // Metadata is equal.
  EXPECT_EQ(record.schema(), record2.schema());
  EXPECT_EQ(record.GetTimestamp(), record2.GetTimestamp());
  EXPECT_EQ(record.IsPositive(), record2.IsPositive());
}

}  // namespace dataflow
}  // namespace pelton
