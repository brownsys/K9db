#include "dataflow/record.h"

#include <cstdint>

#include "gtest/gtest.h"

namespace dataflow {

TEST(RecordDataTest, Size) {
  // should be 16 bytes
  EXPECT_EQ(sizeof(RecordData), 16);
}

TEST(RecordTest, Size) {
  // should be 32 bytes
  EXPECT_EQ(sizeof(Record), 32);
}

// Tests internal storage format
TEST(RecordTest, DataRep) {
  uint64_t v = 42;
  std::string* p = new std::string("hello");

  std::vector<DataType> st = {kUInt, kText, kInt};
  Schema s(st);
  Record r(s);
  *static_cast<uint64_t*>(r[0]) = v;
  std::string** sp = static_cast<std::string**>(r[1]);
  *sp = p;
  *static_cast<int64_t*>(r[2]) = v + 1;

  EXPECT_EQ(*static_cast<uint64_t*>(r[0]), v);
  EXPECT_EQ(*static_cast<std::string**>(r[1]), p);
  EXPECT_EQ(*static_cast<int64_t*>(r[2]), v + 1);
}

// Tests record comparisons
TEST(RecordTest, Comparisons) {
  uint64_t v1 = 42;
  uint64_t v2 = 43;
  std::string* s1 = new std::string("hello");
  std::string* s2 = new std::string("bye");

  Schema s({kUInt, kText});

  Record r1(s);
  *static_cast<uint64_t*>(r1[0]) = v1;
  *static_cast<std::string**>(r1[1]) = s1;

  Record r2(s);
  *static_cast<uint64_t*>(r2[0]) = v2;
  *static_cast<std::string**>(r2[1]) = s2;

  Record r3(s);
  *static_cast<uint64_t*>(r3[0]) = v1;
  *static_cast<std::string**>(r3[1]) = new std::string(*s2);

  EXPECT_EQ(r1, r1);
  EXPECT_EQ(r2, r2);
  EXPECT_NE(r1, r2);
  EXPECT_NE(r2, r1);
  EXPECT_NE(r1, r3);
  EXPECT_NE(r3, r1);
}

}  // namespace dataflow
