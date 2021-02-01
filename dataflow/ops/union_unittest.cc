#include "dataflow/ops/union.h"

#include <memory>

#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "gtest/gtest.h"

namespace dataflow {

TEST(UnionOperatorTest, Empty) {
  Schema s({kUInt, kUInt});  // must outlive records
  std::shared_ptr<UnionOperator> union_op = std::make_shared<UnionOperator>(s);
  std::vector<Record> rs;
  std::vector<Record> proc_rs;

  EXPECT_TRUE(union_op->process(rs, proc_rs));
  // no records have been fed
  EXPECT_EQ(proc_rs, std::vector<Record>());
}

TEST(UnionOperatorTest, Basic) {
  Schema s({kUInt, kUInt});  // must outlive records
  std::shared_ptr<UnionOperator> union_op = std::make_shared<UnionOperator>(s);
  {
    Record r1(s);
    r1.set_uint(0, 1ULL);
    r1.set_uint(1, 2ULL);
    Record r2(s);
    r2.set_uint(0, 2ULL);
    r2.set_uint(1, 2ULL);
    Record r3(s);
    r3.set_uint(0, 3ULL);
    r3.set_uint(1, 5ULL);
    Record r4(s);
    r4.set_uint(0, 4ULL);
    r4.set_uint(1, 5ULL);

    std::vector<Record> rs;

    // feed records
    rs.push_back(r1);
    rs.push_back(r2);
    rs.push_back(r3);
    rs.push_back(r4);

    std::vector<Record> expected_rs = {r1, r2, r3, r4};
    std::vector<Record> proc_rs;

    EXPECT_TRUE(union_op->process(rs, proc_rs));
    EXPECT_EQ(proc_rs.size(), expected_rs.size());
    EXPECT_EQ(proc_rs, expected_rs);
  }
}

}  // namespace dataflow
