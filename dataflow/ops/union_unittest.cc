#include "dataflow/ops/union.h"

#include <memory>

#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "gtest/gtest.h"

namespace dataflow {

TEST(UnionOperatorTest, Basic) {
  std::shared_ptr<UnionOperator> union_op = std::make_shared<UnionOperator>();
  std::vector<Record> rs;
  std::vector<Record> proc_rs;

  EXPECT_TRUE(union_op->process(rs, proc_rs));
  // no records have been fed
  EXPECT_EQ(proc_rs, std::vector<Record>());

  // feed records
  std::vector<RecordData> rd1 = {RecordData(1ULL), RecordData(2ULL)};
  std::vector<RecordData> rd2 = {RecordData(2ULL), RecordData(2ULL)};
  std::vector<RecordData> rd3 = {RecordData(3ULL), RecordData(5ULL)};
  std::vector<RecordData> rd4 = {RecordData(4ULL), RecordData(5ULL)};
  Record r1(true, rd1, 3ULL);
  Record r2(true, rd2, 3ULL);
  Record r3(true, rd3, 3ULL);
  Record r4(true, rd4, 3ULL);
  rs.push_back(r1);
  rs.push_back(r2);
  rs.push_back(r3);
  rs.push_back(r4);

  std::vector<Record> expected_rs = {r1, r2, r3, r4};

  EXPECT_TRUE(union_op->process(rs, proc_rs));
  EXPECT_EQ(proc_rs.size(), expected_rs.size());
  EXPECT_EQ(proc_rs, expected_rs);
}

}  // namespace dataflow
