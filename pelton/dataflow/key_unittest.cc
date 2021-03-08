#include "pelton/dataflow/key.h"

#include <cstdint>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// Test key size.
TEST(KeyTest, Size) {
  // should be 40 bytes, unless std::string has a different size on the machine.
  if (sizeof(Key) != 40) {
    size_t max = sizeof(std::string) > 8 ? sizeof(std::string) : 8;
    size_t machine_size = sizeof(CType) + max;
    // Align with 8 bytes for x64.
    while (machine_size % 8 != 0) {
      machine_size++;
    }
    EXPECT_EQ(sizeof(Key), machine_size);
  }
}

// Test constructing and retrieving data from key.
TEST(KeyTest, DataRep) {
  // Make some values.
  int64_t v0 = 10;
  uint64_t v1 = 5;
  std::string v2 = "hello";

  // Make keys.
  Key k0(v0);
  Key k1(v1);
  Key k2(v2);

  // Retrieve data and type.
  EXPECT_EQ(k0.GetInt(), v0);
  EXPECT_EQ(k1.GetUInt(), v1);
  EXPECT_EQ(k2.GetString(), v2);

  EXPECT_EQ(k0.type(), CType::INT);
  EXPECT_EQ(k1.type(), CType::UINT);
  EXPECT_EQ(k2.type(), CType::TEXT);
}

// Tests key equality check.
TEST(KeyTest, Comparisons) {
  // Make keys.
  Key ukeys[3] = {Key(5ULL), Key(6ULL), Key(5ULL)};
  Key ikeys[3] = {Key(-1LL), Key(5LL), Key(-1LL)};
  Key skeys[3] = {Key("hello"), Key("bye"), Key("hello")};

  for (size_t i = 0; i < 3; i++) {
    // Reflexive.
    EXPECT_EQ(ukeys[i], ukeys[i]);
    EXPECT_EQ(ikeys[i], ikeys[i]);
    EXPECT_EQ(skeys[i], skeys[i]);

    // Unequal values mean unequal keys.
    if (i > 0) {
      EXPECT_NE(ukeys[i - 1], ukeys[i]);
      EXPECT_NE(ikeys[i - 1], ikeys[i]);
      EXPECT_NE(skeys[i - 1], skeys[i]);
    }
  }

  // Equal values and types mean equal keys.
  EXPECT_EQ(ukeys[0], ukeys[2]);
  EXPECT_EQ(ikeys[0], ikeys[2]);
  EXPECT_EQ(skeys[0], skeys[2]);

#ifndef PELTON_VALGRIND_MODE
  // Checking keys of different types fails.
  for (size_t i = 0; i < 3; i++) {
    for (size_t j = 0; j < 3; j++) {
      ASSERT_DEATH({ ukeys[i] == ikeys[j]; }, "Type mismatch");
      ASSERT_DEATH({ ukeys[i] == skeys[j]; }, "Type mismatch");
      ASSERT_DEATH({ ikeys[i] == ukeys[j]; }, "Type mismatch");
      ASSERT_DEATH({ ikeys[i] == skeys[j]; }, "Type mismatch");
      ASSERT_DEATH({ skeys[i] == ukeys[j]; }, "Type mismatch");
      ASSERT_DEATH({ skeys[i] == ikeys[j]; }, "Type mismatch");
    }
  }
#endif
}

// Test getting data with wrong types from key.
#ifndef PELTON_VALGRIND_MODE
TEST(KeyTest, TypeMismatch) {
  Key key1(1LL);
  Key key2("hello");

  ASSERT_DEATH({ key1.GetString(); }, "Type mismatch");
  ASSERT_DEATH({ key1.GetUInt(); }, "Type mismatch");
  ASSERT_DEATH({ key2.GetUInt(); }, "Type mismatch");
  ASSERT_DEATH({ key2.GetInt(); }, "Type mismatch");
}
#endif

// Test key with negative integers.
TEST(KeyTest, NegativeIntegers) {
  Key key1(-7LL);
  Key key2(-100LL);

  EXPECT_EQ(key1.GetInt(), -7);
  EXPECT_EQ(key2.GetInt(), -100);
}

// Test move constructor and move assignment.
TEST(KeyTest, KeyMoveConstructor) {
  // Create some keys.
  Key key1(10LL);
  Key key2("hello and welcome to this long string!");

  // Address of "hello..." in key2.
  const char *ptr = &key2.GetString()[0];

  // Move keys.
  Key key1m(std::move(key1));
  Key key2m(std::move(key2));

  // Must get correct values.
  EXPECT_EQ(key1m.GetInt(), 10);
  EXPECT_EQ(key2m.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&key2m.GetString()[0], ptr);  // same pointer.
  EXPECT_EQ(key1.GetInt(), 10);
  EXPECT_EQ(key2.GetString(), "");
  EXPECT_EQ(key1.type(), key1m.type());
  EXPECT_EQ(key2.type(), key2m.type());
}
TEST(KeyTest, KeyMoveAssignment) {
  // Create some keys.
  Key key1(-10LL);
  Key key2("hello and welcome to this long string!");

  // Address of "hello..." in key2.
  const char *ptr = &key2.GetString()[0];

  // Keys to move to.
  Key key1m(-1000LL);
  Key key2m(100ULL);
  Key key3m("hello from second big and long string!");
  Key key4m("hello from third big and long string!");

  // Move int key and check moved things are equal.
  key1m = std::move(key1);
  EXPECT_EQ(key1m.GetInt(), -10);
  EXPECT_EQ(key1.GetInt(), -10);
  EXPECT_EQ(key1.type(), key1m.type());

  // Move string key and check moved things are equal.
  key3m = std::move(key2);
  EXPECT_EQ(key3m.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&key3m.GetString()[0], ptr);  // same pointer.
  EXPECT_EQ(key2.GetString(), "");        // String has been moved.
  // With valgrind we can check that the string initially in key3m was freed
  // during move!

#ifndef PELTON_VALGRIND_MODE
  // Test illegal moves.
  ASSERT_DEATH({ key2m = std::move(key3m); }, "Bad move assign key type");
  ASSERT_DEATH({ key4m = std::move(key1m); }, "Bad move assign key type");
#endif
}

// Test copy constructor and assignments.
TEST(KeyTest, KeyCopyConstructor) {
  // Create some keys.
  Key key1(10LL);
  Key key2("hello and welcome to this long string!");

  // Address of "hello..." in key2.
  const char *ptr = &key2.GetString()[0];

  // Copy keys.
  Key key1c(key1);
  Key key2c = key2;

  // Must get correct values.
  EXPECT_EQ(key1.GetInt(), 10);
  EXPECT_EQ(key1c.GetInt(), 10);
  EXPECT_EQ(key1.type(), key1c.type());
  EXPECT_EQ(key2.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(key2c.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&key2.GetString()[0], ptr);   // Original string unchanged.
  EXPECT_NE(&key2c.GetString()[0], ptr);  // Target string is a copy.
  EXPECT_EQ(key2.type(), key2c.type());
}
TEST(KeyTest, KeyCopyAssignment) {
  // Create some keys.
  Key key1(-10LL);
  Key key2("hello and welcome to this long string!");

  // Address of "hello..." in key2.
  const char *ptr = &key2.GetString()[0];

  // Keys to move to.
  Key key1c(-1000LL);
  Key key2c(100ULL);
  Key key3c("hello from second big and long string!");
  Key key4c("hello from third big and long string!");

  // Copy int key and check copied things are equal.
  key1c = key1;
  EXPECT_EQ(key1.GetInt(), -10);
  EXPECT_EQ(key1c.GetInt(), -10);
  EXPECT_EQ(key1.type(), key1c.type());

  // Copy string key and check copied things are equal.
  key3c = key2;
  EXPECT_EQ(key2.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(key3c.GetString(), "hello and welcome to this long string!");
  EXPECT_EQ(&key2.GetString()[0], ptr);   // same pointer.
  EXPECT_NE(&key3c.GetString()[0], ptr);  // different pointer.
  EXPECT_EQ(key2.type(), key3c.type());
  // With valgrind we can check that the string initially in key3c was freed
  // during copy!

#ifndef PELTON_VALGRIND_MODE
  // Test illegal copies.
  ASSERT_DEATH({ key2c = key2; }, "Bad copy assign key type");
  ASSERT_DEATH({ key4c = key1; }, "Bad copy assign key type");
#endif
}

}  // namespace dataflow
}  // namespace pelton
