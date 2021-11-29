#include "pelton/dataflow/future.h"

#include "gtest/gtest.h"

namespace pelton {
namespace dataflow {

TEST(FutureTest, BasicTest) {
  Future future;
  Promise p1 = future.GetPromise();
  Promise p2 = p1.Derive();
  EXPECT_EQ(future.counter(), 2);
  p1.Set();
  p2.Set();
  EXPECT_EQ(future.counter(), 0);
}

}  // namespace dataflow
}  // namespace pelton
