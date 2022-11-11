#include "pelton/util/iterator.h"

#include <algorithm>
#include <string>
#include <vector>

#include "gtest/gtest.h"

namespace pelton {
namespace util {

// Test Map.
struct Mover {
  // NOLINTNEXTLINE
  std::string Map(std::string &s) { return std::move(s); }
};

struct Copier {
  std::string Map(const std::string &s) const { return s; }
};

TEST(MapIt, MoveTestWithDefaultConstructor) {
  std::vector<std::string> v = {"hello", "bye", "hellobye"};
  auto it = Map<Mover>(v.begin());
  auto end = Map<Mover>(v.end());
  std::vector<std::string> moved(it, end);
  EXPECT_EQ(moved, (std::vector<std::string>{"hello", "bye", "hellobye"}));
  EXPECT_EQ(v, (std::vector<std::string>{"", "", ""}));
}

TEST(MapIt, ConstMapWithNonCapturingLambda) {
  std::vector<int> v = {0, 1, 2, 3, 4};
  auto it = MapIt(v.cbegin(), [](const int &i) { return i + 1; });
  auto end = MapIt(v.cend(), [](const int &i) { return i + 1; });
  std::vector<int> added(it, end);
  EXPECT_EQ(added, (std::vector<int>{1, 2, 3, 4, 5}));
}

struct PrefixSum {
  explicit PrefixSum(int s) : start(s) {}
  int Map(const int &i) {
    start += i;
    return start;
  }
  int start;
};

TEST(MapIt, PrefixSumWithStatefulInstance) {
  std::vector<int> v = {0, 1, 2, 3, 4};
  auto it = MapIt(v.cbegin(), PrefixSum(0));
  auto end = MapIt(v.cend(), PrefixSum(0));
  std::vector<int> prefixsums(it, end);
  EXPECT_EQ(prefixsums, (std::vector<int>{0, 1, 3, 6, 10}));
}

TEST(MapIt, PrefixSumWithCapturingLambda) {
  std::vector<int> v = {0, 1, 2, 3, 4};
  int sum = 0;
  auto it = MapIt(v.cbegin(), [&sum](const int &i) {
    sum += i;
    return sum;
  });
  auto end = MapIt(v.cend(), [](const int &i) { return 0; });
  std::vector<int> prefixsums(it, end);
  EXPECT_EQ(prefixsums, (std::vector<int>{0, 1, 3, 6, 10}));
  EXPECT_EQ(sum, 10);
}

TEST(MapIt, PrefixSumWithStdFunction) {
  std::vector<int> v = {0, 1, 2, 3, 4};
  int sum = 0;
  auto lambda = std::function([&sum](const int &i) {
    sum += i;
    return sum;
  });
  auto it = MapIt(v.cbegin(), lambda);
  auto end = MapIt(v.cend(), lambda);
  std::vector<int> prefixsums(it, end);
  EXPECT_EQ(prefixsums, (std::vector<int>{0, 1, 3, 6, 10}));
  EXPECT_EQ(sum, 10);
}

TEST(MapIt, MapIterableHelper) {
  // Moving with a default constructible struct.
  std::vector<std::string> v = {"hello", "bye", "hellobye"};
  auto iterable = Map<Mover>(&v);
  std::vector<std::string> moved(iterable.begin(), iterable.end());
  EXPECT_EQ(moved, (std::vector<std::string>{"hello", "bye", "hellobye"}));
  EXPECT_EQ(v, (std::vector<std::string>{"", "", ""}));

  // Const Map with non capturing lambda.
  const std::vector<int> v2{0, 1, 2, 3, 4};
  auto iterable2 = Map(&v2, [](const int &i) { return i + 1; });
  std::vector<int> added(iterable2.begin(), iterable2.end());
  EXPECT_EQ(added, (std::vector<int>{1, 2, 3, 4, 5}));

  auto iterable2f = Map(&v2, std::function([](const int &i) { return i + 1; }));
  std::vector<int> addedf(iterable2f.begin(), iterable2f.end());
  EXPECT_EQ(addedf, (std::vector<int>{1, 2, 3, 4, 5}));

  // Testing using const_iterator.
  const std::vector<std::string> vv = {"hello", "bye", "hellobye"};
  auto iterable3 = Map<Copier>(&vv);
  std::vector<std::string> copied(iterable3.begin(), iterable3.end());
  EXPECT_EQ(copied, (std::vector<std::string>{"hello", "bye", "hellobye"}));
  EXPECT_EQ(vv, (std::vector<std::string>{"hello", "bye", "hellobye"}));

  // Const Map using stateful struct.
  auto iterable4 = Map(&v2, PrefixSum(0));
  std::vector<int> prefixsums(iterable4.begin(), iterable4.end());
  EXPECT_EQ(prefixsums, (std::vector<int>{0, 1, 3, 6, 10}));
}

TEST(ZipIt, TraverseSameTypeDifferentConst) {
  std::vector<std::string> v1 = {"a", "b", "c", "d"};
  const std::vector<std::string> v2 = {"A", "B", "C", "D"};
  for (auto &&[a, b] : Zip(&v1, &v2)) {
    a += b;
  }
  EXPECT_EQ(v1, (std::vector<std::string>{"aA", "bB", "cC", "dD"}));
  EXPECT_EQ(v2, (std::vector<std::string>{"A", "B", "C", "D"}));
}

TEST(ZipIt, TraverseDifferentTypes) {
  std::vector<std::string> v1 = {"A", "B", "C", "D"};
  std::vector<int> v2 = {10, 12, 14, 16};
  for (auto &&[a, b] : Zip(&v1, &v2)) {
    b += static_cast<int>(a.at(0));
  }
  EXPECT_EQ(v2, (std::vector<int>{75, 78, 81, 84}));
}

TEST(ZipIt, ZipWithMap) {
  std::vector<std::string> v1 = {"A", "B", "C", "D"};
  std::vector<int> v2 = {10, 12, 14, 16};
  auto [b, e] = Zip(&v1, &v2);
  std::function comb([](RefPair<std::string &, int &> p) {
    p.first += std::to_string(p.second);
    p.second = 0;
    return "K" + p.first;
  });
  std::vector<std::string> v3(MapIt(b, comb), MapIt(e, comb));
  EXPECT_EQ(v1, (std::vector<std::string>{"A10", "B12", "C14", "D16"}));
  EXPECT_EQ(v2, (std::vector<int>{0, 0, 0, 0}));
  EXPECT_EQ(v3, (std::vector<std::string>{"KA10", "KB12", "KC14", "KD16"}));
}

TEST(ZipIt, SortZip) {
  std::vector<int> v1 = {10, 0, 20, 25, 3};
  std::vector<std::string> v2 = {"A", "B", "C", "D", "E"};

  auto [b, e] = Zip(&v1, &v2);
  std::sort(b, e);

  EXPECT_EQ(v1, (std::vector<int>{0, 3, 10, 20, 25}));
  EXPECT_EQ(v2, (std::vector<std::string>{"B", "E", "A", "C", "D"}));
}

}  // namespace util
}  // namespace pelton
