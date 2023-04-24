#include "k9db/dataflow/future.h"

#include "gtest/gtest.h"

namespace k9db {
namespace dataflow {

TEST(FutureTest, BasicTest) {
  Future future(true);
  Promise p1 = future.GetPromise();
  Promise p2 = p1.Derive();
  EXPECT_EQ(future.counter_.load(), 3);
  p1.Resolve();
  p2.Resolve();
  EXPECT_EQ(future.counter_.load(), 1);
  future.Wait();
}

TEST(FutureTest, InconsistentTest) {
  Future future(false);
  Promise p1 = future.GetPromise();
  future.Wait();
  Promise p2 = p1.Derive();
  future.Wait();
  EXPECT_EQ(p1.future_, nullptr);
  EXPECT_EQ(p2.future_, nullptr);
  EXPECT_EQ(future.counter_.load(), 0);
  future.Wait();
  p1.Resolve();
  future.Wait();
  p2.Resolve();
  EXPECT_EQ(future.counter_.load(), 0);
  future.Wait();
}

}  // namespace dataflow
}  // namespace k9db
