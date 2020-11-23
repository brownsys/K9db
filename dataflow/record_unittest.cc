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

  std::vector<DataType> st = {UINT, TEXT, UINT};
  Schema s(st);
  Record r(s);
  *static_cast<uint64_t*>(r[0]) = v;
  // std::string* sp = static_cast<std::string*>(r[1]);
  // sp = p;
  *static_cast<uint64_t*>(r[2]) = v + 1;

  EXPECT_EQ(*static_cast<uint64_t*>(r[0]), v);
  // EXPECT_EQ(*static_cast<std::string*>(r[1]), std::string("hello"));
  EXPECT_EQ(*static_cast<uint64_t*>(r[2]), v + 1);

  delete p;
}

}  // namespace dataflow
