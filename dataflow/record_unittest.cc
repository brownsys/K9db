#include "dataflow/record.h"

#include <cstdint>

#include "gtest/gtest.h"

namespace dataflow {

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
  Record r(true, std::vector<RecordData>{d_val, d_ptr}, bitmap);

  // should get data out either way when wrapped into Record
  EXPECT_EQ((uint64_t*)r[0], &d_val.data_);
  EXPECT_EQ(*(uint64_t*)r[0], v);
  EXPECT_EQ((uint64_t*)r[1], p);
  EXPECT_EQ(*(uint64_t*)r[1], v);
}

}  // namespace dataflow
