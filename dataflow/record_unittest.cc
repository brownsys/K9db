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

// Tests internal storage format via raw void* APIs
TEST(RecordTest, DataRepRaw) {
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

// Tests internal storage format via typed APIs
TEST(RecordTest, DataRep) {
  uint64_t v = 42;
  std::string* p = new std::string("hello");

  std::vector<DataType> st = {kUInt, kText, kInt};
  Schema s(st);
  Record r(s);
  r.set_uint(0, v);
  r.set_string(1, p);
  r.set_int(2, v + 1);

  EXPECT_EQ(r.as_uint(0), v);
  EXPECT_EQ(r.as_string(1), p);
  EXPECT_EQ(r.as_int(2), v + 1);
}

// Tests record comparisons
TEST(RecordTest, Comparisons) {
  uint64_t v1 = 42;
  uint64_t v2 = 43;
  std::string* s1 = new std::string("hello");
  std::string* s2 = new std::string("bye");

  Schema s({kUInt, kText});

  Record r1(s);
  r1.set_uint(0, v1);
  r1.set_string(1, s1);

  Record r2(s);
  r2.set_uint(0, v2);
  r2.set_string(1, s2);

  Record r3(s);
  r3.set_uint(0, v1);
  r3.set_string(1, new std::string(*s2));

  EXPECT_EQ(r1, r1);
  EXPECT_EQ(r2, r2);
  EXPECT_NE(r1, r2);
  EXPECT_NE(r2, r1);
  EXPECT_NE(r1, r3);
  EXPECT_NE(r3, r1);
}

TEST(RecordTest, TypeMismatch) {
  uint64_t v1 = 42;
  std::string* s1 = new std::string("hello");

  Schema s({kUInt, kText});

  Record r1(s);
  r1.set_uint(0, v1);
  r1.set_string(1, s1);

  ASSERT_DEATH({ r1.as_string(0); }, "Type mismatch");
  ASSERT_DEATH({ r1.as_uint(1); }, "Type mismatch");
  ASSERT_DEATH({ r1.as_int(1); }, "Type mismatch");
}

TEST(RecordTest, DataRepSizes) {
  uint64_t i = 42;
  std::string* s = new std::string("hello");

  Schema sch({kUInt, kText});

  Record r(sch);
  r.set_uint(0, i);
  r.set_string(1, s);

  EXPECT_EQ(r.size_at(0), sizeof(uint64_t));
  EXPECT_EQ(r.size_at(1), s->size());
}

}  // namespace dataflow
