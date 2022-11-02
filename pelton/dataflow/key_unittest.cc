#include "pelton/dataflow/key.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// Test key size.
TEST(KeyTest, Size) {
  EXPECT_EQ(sizeof(Key), sizeof(std::vector<sqlast::Value>));
}

// Test constructing and retrieving data from key.
TEST(KeyTest, DataRep) {
  // Make some keys.
  Key k1(1);
  Key k2(2);
  Key k3(3);

  // Add values to keys.
  std::string str = "Hello from a long movable string!";
  char *ptr = &str[0];
  k1.AddValue(0_u);
  k2.AddValue(1_u);
  k2.AddValue(str);
  k3.AddValue(2_u);
  k3.AddValue(std::move(str));
  k3.AddValue(5_s);

  // Values are what we expect.
  EXPECT_EQ(str, "");
  EXPECT_EQ(k1.value(0).GetUInt(), 0_u);
  EXPECT_EQ(k2.value(0).GetUInt(), 1_u);
  EXPECT_EQ(k2.value(1).GetString(), "Hello from a long movable string!");
  EXPECT_EQ(k3.value(0).GetUInt(), 2_u);
  EXPECT_EQ(k3.value(1).GetString(), "Hello from a long movable string!");
  EXPECT_EQ(k3.value(2).GetInt(), 5_s);
  EXPECT_EQ(&k3.value(1).GetString()[0], ptr);
}

#ifndef PELTON_VALGRIND_MODE
// Test attempting to go over capacity.
TEST(KeyTest, Capacity) {
  Key k{2};
  k.AddValue("hi");
  k.AddValue("bye");
  ASSERT_DEATH({ k.AddValue(-5_s); }, "Key is already full");
}
#endif

// Tests key equality check.
TEST(KeyTest, Comparisons) {
  // Make keys.
  Key key1{3};
  Key key2{3};
  Key key3{3};
  Key key4{2};

  // Add values.
  key1.AddValue(0_u);
  key1.AddValue("Hello!");
  key1.AddValue(-5_s);

  key2.AddValue(0_u);
  key2.AddValue("Hello!");
  key2.AddValue(-5_s);

  key3.AddValue(0_u);
  key3.AddValue("Bye!");
  key3.AddValue(-5_s);

  key4.AddValue(-5_s);
  key4.AddValue("Hello!");

  // Equal values mean equal keys.
  EXPECT_EQ(key1, key2);

  // Different sizes mean unequal.
  EXPECT_NE(key1, key4);
  EXPECT_NE(key2, key4);
  EXPECT_NE(key3, key4);

  // Different values mean unequal.
  EXPECT_NE(key1, key3);
  EXPECT_NE(key2, key3);
}

}  // namespace dataflow
}  // namespace pelton
