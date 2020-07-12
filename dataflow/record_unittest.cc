#include "dataflow/record.h"

#include <cstdint>

#include "gtest/gtest.h"

// Tests internal storage format
TEST(RecordTest, DataRep) {
  int v = 42;
  RecordData d_ptr;
  d_ptr.data_ = (uintptr_t)&v;
  d_ptr.data_ |= TOP_BIT;

  // deref should return the plain pointer
  EXPECT_EQ(&v, (int*)*d_ptr);

  RecordData d_val;
  d_val.data_ = v;

  // deref should return pointer to value stored inline
  EXPECT_EQ((int*)&d_val.data_, (int*)*d_val);
}
