#include "pelton/dataflow/record.h"

#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <utility>

#include <iostream>

#include "pelton/dataflow/key.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"
#include "gtest/gtest.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

TEST(RecordTest, Size) {
  // should be 24 bytes.
  EXPECT_EQ(sizeof(Record), 24);
}

// Tests internal storage format via raw void* APIs
TEST(RecordTest, DataRep) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<size_t> keys = {0};
  Schema schema{names, types, keys};

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
}

// Tests record comparisons
TEST(RecordTest, Comparisons) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::TEXT};
  std::vector<size_t> keys = {0};
  Schema schema{names, types, keys};

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

TEST(RecordTest, GetTypeMismatch) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::TEXT};
  std::vector<size_t> keys = {0};
  Schema schema{names, types, keys};

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> v1 = std::make_unique<std::string>("hello");

  Record record{schema};
  record.SetUInt(v0, 0);
  record.SetString(std::move(v1), 1);

  ASSERT_DEATH({ record.GetString(0); }, "Type mismatch");
  ASSERT_DEATH({ record.GetUInt(1); }, "Type mismatch");
  ASSERT_DEATH({ record.GetInt(1); }, "Type mismatch");
}

TEST(RecordTest, SetTypeMismatch) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::TEXT};
  std::vector<size_t> keys = {0};
  Schema schema{names, types, keys};

  // Make some values.
  uint64_t v0 = 42;
  std::unique_ptr<std::string> v1 = std::make_unique<std::string>("hello");

  Record record{schema};
  ASSERT_DEATH({ record.SetUInt(v0, 1); }, "Type mismatch");
  ASSERT_DEATH({ record.SetString(std::move(v1), 0); }, "Type mismatch");
}

TEST(RecordTest, NegativeIntegers) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::INT};
  std::vector<size_t> keys = {0};
  Schema schema{names, types, keys};

  Record r{schema};
  r.SetInt(-7, 0);
  r.SetInt(-1000, 1);
  EXPECT_EQ(r.GetInt(0), -7);
  EXPECT_EQ(r.GetInt(1), -1000);
}

TEST(RecordTest, Key) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<size_t> keys1 = {0};
  std::vector<size_t> keys2 = {1};
  Schema schema1{names, types, keys1};
  Schema schema2{names, types, keys2};

  // Some values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("hello");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("bye");
  // Some keys.
  int64_t k1 = -10;
  std::string k2 = *s2;

  // Create two records.
  Record r1{schema1};
  r1.SetInt(k1, 0);
  r1.SetString(std::move(s1), 1);
  Record r2{schema2};
  r2.SetInt(500, 0);
  r2.SetString(std::move(s2), 1);
  Record r3{schema2};
  r3.SetInt(500, 0);

  // Test keys.
  EXPECT_EQ(r1.GetKey().GetInt(), k1);
  EXPECT_EQ(r2.GetKey().GetString(), k2);
  EXPECT_EQ(r3.GetKey().GetString(), "");
}

TEST(RecordTest, KeyTypeMistmatch) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::INT, CType::TEXT};
  std::vector<size_t> keys1 = {0};
  std::vector<size_t> keys2 = {1};
  Schema schema1{names, types, keys1};
  Schema schema2{names, types, keys2};

  // Some values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("hello");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("bye");
  // Some keys.
  int64_t k1 = -10;
  std::string k2 = *s2;

  // Create two records.
  Record r1{schema1};
  r1.SetInt(k1, 0);
  r1.SetString(std::move(s1), 1);
  Record r2{schema2};
  r2.SetInt(500, 0);
  r2.SetString(std::move(s2), 1);

  // Test keys.
  ASSERT_DEATH({ r1.GetKey().GetUInt(); }, "Type mismatch");
  ASSERT_DEATH({ r1.GetKey().GetString(); }, "Type mismatch");
  ASSERT_DEATH({ r2.GetKey().GetUInt(); }, "Type mismatch");
  ASSERT_DEATH({ r2.GetKey().GetInt(); }, "Type mismatch");
}

}  // namespace dataflow
}  // namespace pelton
