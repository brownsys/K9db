#include "k9db/util/ints.h"

#include <string>

#include "gtest/gtest.h"

namespace k9db {

// Test key size.
TEST(IntsHelpers, SignedTest) {
  int64_t x0 = 0;
  int64_t x1 = 1;
  int64_t x2 = -1;
  int64_t x3 = 9223372036854775807ll;
  int64_t x4 = LLONG_MIN;
  EXPECT_EQ(x0, 0_s);
  EXPECT_EQ(x1, 1_s);
  EXPECT_EQ(x2, -1_s);
  EXPECT_EQ(x3, 9223372036854775807_s);
  EXPECT_EQ(x4, -9223372036854775808_s);
  EXPECT_EQ("0", std::to_string(0_s));
  EXPECT_EQ("1", std::to_string(1_s));
  EXPECT_EQ("-1", std::to_string(-1_s));
  EXPECT_EQ("9223372036854775807", std::to_string(9223372036854775807_s));
  EXPECT_EQ("-9223372036854775808", std::to_string(-9223372036854775807_s - 1));
}

TEST(IntsHelpers, UnsignedTest) {
  uint64_t x0 = 0;
  uint64_t x1 = 1;
  uint64_t x2 = 18446744073709551615ull;
  EXPECT_EQ(x0, 0_u);
  EXPECT_EQ(x1, 1_u);
  EXPECT_EQ(x2, 18446744073709551615_u);
  EXPECT_EQ("0", std::to_string(0_u));
  EXPECT_EQ("1", std::to_string(1_u));
  EXPECT_EQ("18446744073709551615", std::to_string(18446744073709551615_u));
}

}  // namespace k9db
