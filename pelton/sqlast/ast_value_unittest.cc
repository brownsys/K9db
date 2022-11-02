#include "pelton/sqlast/ast_value.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace sqlast {

using CType = Value::Type;

// Test value size.
TEST(ValueTest, Size) {
  // should be 40 bytes, unless std::string has a different size on the machine.
  EXPECT_EQ(sizeof(Value), sizeof(std::string) + sizeof(size_t));
}

// Test constructing and retrieving data from value.
TEST(ValueTest, DataRep) {
  // Make some values.
  int64_t v0 = 10;
  uint64_t v1 = 5;
  std::string v2 = "hello";

  // Make values.
  Value k0(v0);
  Value k1(v1);
  Value k2(v2);
  Value k3;

  // Retrieve data and type.
  EXPECT_EQ(k0.GetInt(), v0);
  EXPECT_EQ(k1.GetUInt(), v1);
  EXPECT_EQ(k2.GetString(), v2);
  EXPECT_TRUE(k3.IsNull());

  EXPECT_EQ(k0.type(), CType::INT);
  EXPECT_EQ(k1.type(), CType::UINT);
  EXPECT_EQ(k2.type(), CType::TEXT);
  EXPECT_EQ(k3.type(), CType::_NULL);
}

// Tests value equality check.
TEST(ValueTest, Comparisons) {
  // Make values.
  Value uvalues[3] = {Value(5_u), Value(6_u), Value(5_u)};
  Value ivalues[3] = {Value(-1_s), Value(5_s), Value(-1_s)};
  Value svalues[3] = {Value("hello"), Value("bye"), Value("hello")};

  for (size_t i = 0; i < 3; i++) {
    // Reflexive.
    EXPECT_EQ(uvalues[i], uvalues[i]);
    EXPECT_EQ(ivalues[i], ivalues[i]);
    EXPECT_EQ(svalues[i], svalues[i]);

    // Unequal values mean unequal values.
    if (i > 0) {
      EXPECT_NE(uvalues[i - 1], uvalues[i]);
      EXPECT_NE(ivalues[i - 1], ivalues[i]);
      EXPECT_NE(svalues[i - 1], svalues[i]);
    }
  }

  // Equal values and types mean equal values.
  EXPECT_EQ(uvalues[0], uvalues[2]);
  EXPECT_EQ(ivalues[0], ivalues[2]);
  EXPECT_EQ(svalues[0], svalues[2]);

  // Checking values of different types fails.
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 3; j++) {
      EXPECT_NE(uvalues[i], ivalues[j]);
      EXPECT_NE(uvalues[i], svalues[j]);
      EXPECT_NE(ivalues[i], uvalues[j]);
      EXPECT_NE(ivalues[i], svalues[j]);
      EXPECT_NE(svalues[i], uvalues[j]);
      EXPECT_NE(svalues[i], ivalues[j]);
    }
  }
}

// Test getting data with wrong types from value.
#ifndef PELTON_VALGRIND_MODE
TEST(ValueTest, TypeMismatch) {
  Value value1(1_s);
  Value value2("hello");

  ASSERT_DEATH({ value1.GetString(); }, "Type mismatch");
  ASSERT_DEATH({ value1.GetUInt(); }, "Type mismatch");
  ASSERT_DEATH({ value2.GetUInt(); }, "Type mismatch");
  ASSERT_DEATH({ value2.GetInt(); }, "Type mismatch");
}
#endif

// Test value with negative integers.
TEST(ValueTest, NegativeIntegers) {
  Value value1(-7_s);
  Value value2(-100_s);

  EXPECT_EQ(value1.GetInt(), -7);
  EXPECT_EQ(value2.GetInt(), -100);
}

// Test move constructor and move assignment.
TEST(ValueTest, ValueMoveConstructor) {
  // Create some values.
  Value value1(10_s);
  Value value2("hello and welcome to this long string!");

  // Address of "hello..." in value2.
  const char *ptr = &value2.GetString()[0];

  // Move values.
  Value value1m(std::move(value1));
  Value value2m(std::move(value2));

  // Must get correct values.
  EXPECT_EQ(value1m.GetInt(), 10);
  EXPECT_EQ(value2m.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&value2m.GetString()[0], ptr);  // same pointer.
  EXPECT_EQ(value1.GetInt(), 10);
  EXPECT_EQ(value2.GetString(), "");
  EXPECT_EQ(value1.type(), value1m.type());
  EXPECT_EQ(value2.type(), value2m.type());
}
TEST(ValueTest, ValueMoveAssignment) {
  // Create some values.
  Value value1(-10_s);
  Value value2("hello and welcome to this long string!");

  // Address of "hello..." in value2.
  const char *ptr = &value2.GetString()[0];

  // Values to move to.
  Value value1m(-1000_s);
  Value value2m(100_u);
  Value value3m("hello from second big and long string!");
  Value value4m("hello from third big and long string!");

  // Move int value and check moved things are equal.
  value1m = std::move(value1);
  EXPECT_EQ(value1m.GetInt(), -10);
  EXPECT_EQ(value1.GetInt(), -10);
  EXPECT_EQ(value1.type(), value1m.type());

  // Move string value and check moved things are equal.
  value3m = std::move(value2);
  EXPECT_EQ(value3m.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&value3m.GetString()[0], ptr);  // same pointer.
  EXPECT_EQ(value2.GetString(), "");        // String has been moved.
  // With valgrind we can check that the string initially in value3m was freed
  // during move!

  // Move values into values of other types.
  value2m = std::move(value3m);
  value4m = std::move(value1m);
  EXPECT_EQ(value2m.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&value2m.GetString()[0], ptr);  // same pointer.
  EXPECT_EQ(value4m.GetInt(), -10);
  EXPECT_EQ(value4m.GetInt(), -10);
  EXPECT_EQ(value4m.type(), value1m.type());
}

// Test copy constructor and assignments.
TEST(ValueTest, ValueCopyConstructor) {
  // Create some values.
  Value value1(10_s);
  Value value2("hello and welcome to this long string!");

  // Address of "hello..." in value2.
  const char *ptr = &value2.GetString()[0];

  // Copy values.
  Value value1c(value1);
  Value value2c = value2;

  // Must get correct values.
  EXPECT_EQ(value1.GetInt(), 10);
  EXPECT_EQ(value1c.GetInt(), 10);
  EXPECT_EQ(value1.type(), value1c.type());
  EXPECT_EQ(value2.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(value2c.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&value2.GetString()[0], ptr);   // Original string unchanged.
  EXPECT_NE(&value2c.GetString()[0], ptr);  // Target string is a copy.
  EXPECT_EQ(value2.type(), value2c.type());
}
TEST(ValueTest, ValueCopyAssignment) {
  // Create some values.
  Value value1(-10_s);
  Value value2("hello and welcome to this long string!");

  // Address of "hello..." in value2.
  const char *ptr = &value2.GetString()[0];

  // Values to move to.
  Value value1c(-1000_s);
  Value value2c(100_u);
  Value value3c("hello from second big and long string!");
  Value value4c("hello from third big and long string!");

  // Copy int value and check copied things are equal.
  value1c = value1;
  EXPECT_EQ(value1.GetInt(), -10);
  EXPECT_EQ(value1c.GetInt(), -10);
  EXPECT_EQ(value1.type(), value1c.type());

  // Copy string value and check copied things are equal.
  value3c = value2;
  EXPECT_EQ(value2.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(value3c.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&value2.GetString()[0], ptr);   // same pointer.
  EXPECT_NE(&value3c.GetString()[0], ptr);  // different pointer.
  EXPECT_EQ(value2.type(), value3c.type());
  // With valgrind we can check that the initial string and copy are both freed!

  // Move values into values of other types.
  value2c = value2;
  value4c = value1;

  EXPECT_EQ(value2.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(value2c.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(value3c.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&value2.GetString()[0], ptr);   // same pointer.
  EXPECT_NE(&value2c.GetString()[0], ptr);  // different pointers.
  EXPECT_NE(&value3c.GetString()[0], ptr);
  EXPECT_NE(&value2c.GetString()[0], &value3c.GetString()[0]);
  EXPECT_EQ(value2.type(), value3c.type());
  EXPECT_EQ(value2.type(), value2c.type());

  EXPECT_EQ(value1.GetInt(), -10);
  EXPECT_EQ(value1c.GetInt(), -10);
  EXPECT_EQ(value4c.GetInt(), -10);
  EXPECT_EQ(value1.type(), value1c.type());
  EXPECT_EQ(value1.type(), value4c.type());
}

}  // namespace sqlast
}  // namespace pelton
