#include "dataflow/record.h"

#include <cstdint>

#include "gtest/gtest.h"

namespace dataflow {

TEST(RecordDataTest, Size) {
  // should be 8 bytes
  EXPECT_EQ(sizeof(RecordData), 8);
}

// Tests internal storage format
TEST(RecordTest, DataRep) {
  uint64_t v = 42;
  uint64_t* p = new uint64_t(v);
  std::unique_ptr<uint64_t> up(p);
  RecordData d_ptr(std::move(up));

  // should return the plain pointer
  EXPECT_EQ(p, (uint64_t*)d_ptr.data_);
  EXPECT_EQ(p, (uint64_t*)d_ptr.as_ptr());

  RecordData d_val(v);

  // should return value stored inline
  EXPECT_EQ((uint64_t)d_val.data_, v);
  EXPECT_EQ((uint64_t)d_val.as_val(), v);

  uint64_t bitmap = 0x2;  // set bit 1, for d_ptr
  std::vector<RecordData> d_init{d_ptr, d_val};
  Record r(true, d_init, bitmap);

  // should get data out either way when wrapped into Record
  EXPECT_EQ((uint64_t*)r[0], p);
  EXPECT_EQ(*(uint64_t*)r[0], v);
  // inline val gets copied on construction, so addr != &d_init.data_
  EXPECT_EQ((uint64_t*)r[1], (uint64_t*)&(r.data_[1].data_));
  EXPECT_NE((uint64_t*)r[1], (uint64_t*)&d_init[1].data_);
  EXPECT_EQ(*(uint64_t*)r[1], v);

  delete p;
}

TEST(RecordTest, Size) {
  // should be 40 bytes
  EXPECT_EQ(sizeof(Record), 40);
}

}  // namespace dataflow
