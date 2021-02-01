#include "dataflow/ops/aggregate.h"

#include <algorithm>
#include <memory>

#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dataflow {

bool comparePos(Record r1, Record r2) {
  if (r1.positive() > r2.positive()) {
    return false;
  }
  return true;
}

bool compareRecordsLT(Record r1, Record r2) {
  EXPECT_EQ(r1.schema(), r2.schema());
  // (specific to these test cases): as_uint is hard coded below; schema should
  // in compliance
  for (size_t i = 0; i < r1.schema().num_columns(); i++) {
    if ((r1.as_uint(i)) < (r2.as_uint(i))) {
      return true;
    } else if ((r1.as_uint(i)) == (r2.as_uint(i))) {
      // need to check next value in the record
      continue;
    } else if ((r1.as_uint(i)) > (r2.as_uint(i))) {
      // should only reach here if some N previous values of the records are
      // equal
      return false;
    }
  }
  return true;
}

std::vector<Record> splitSortMerge(std::vector<Record> rs) {
  // the record vector will have to be sorted separately (based on positives)
  // and then be merged into one record in order to avoid the algorithm flipping
  // records at the positive boundary

  // find the split index (w.r.t the positive boundary)
  size_t splitIndex;
  for (size_t i = 0; i < rs.size(); i++) {
    if (rs[i].positive() == 1) {
      splitIndex = i;
      break;
    }
  }

  if (splitIndex == 0) {
    std::sort(rs.begin(), rs.end(), compareRecordsLT);
    return rs;
  }

  std::vector<Record> v1(rs.cbegin(), rs.cbegin() + splitIndex);
  std::vector<Record> v2(rs.cbegin() + splitIndex, rs.cend());
  // sort the above two vectors independently
  std::sort(v1.begin(), v1.end(), compareRecordsLT);
  std::sort(v2.begin(), v2.end(), compareRecordsLT);
  // merge the two vectors
  v1.insert(v1.end(), v2.begin(), v2.end());

  return v1;
}

void compareRecordStreams(std::vector<Record> rs1, std::vector<Record> rs2) {
  EXPECT_EQ(rs1.size(), rs2.size());

  // first sort the records based positves
  std::sort(rs1.begin(), rs1.end(), comparePos);
  std::sort(rs2.begin(), rs2.end(), comparePos);

  // // then sort the subsets based on the actual values
  rs1 = splitSortMerge(rs1);
  rs2 = splitSortMerge(rs2);

  // Two records are equal if their positives are equal
  // and their values are equal
  for (size_t i = 0; i < rs1.size(); i++) {
    // positive comparision handled by record.h
    EXPECT_EQ(rs1[i], rs2[i]);
  }
}

TEST(AggregateOperatorTest, SumPos) {
  Schema s1({kUInt, kUInt, kUInt});
  Schema s2({kUInt, kUInt});

  std::vector<ColumnID> group_cols = {1};
  AggregateOperator::Func agg_func = AggregateOperator::FuncSum;
  ColumnID agg_col = 2;
  std::vector<ColumnID> out_cols = group_cols;
  out_cols.push_back(agg_col);

  std::shared_ptr<AggregateOperator> aggregate_op =
      std::make_shared<AggregateOperator>(group_cols, agg_func, agg_col, s1,
                                          out_cols);

  {
    Record r1(s1);
    r1.set_uint(0, 1ULL);
    r1.set_uint(1, 2ULL);
    r1.set_uint(2, 9ULL);
    Record r2(s1);
    r2.set_uint(0, 2ULL);
    r2.set_uint(1, 2ULL);
    r2.set_uint(2, 7ULL);
    Record r3(s1);
    r3.set_uint(0, 3ULL);
    r3.set_uint(1, 5ULL);
    r3.set_uint(2, 5ULL);
    Record r4(s1);
    r4.set_uint(0, 4ULL);
    r4.set_uint(1, 5ULL);
    r4.set_uint(2, 6ULL);

    std::vector<Record> rs;
    // feed records
    rs.push_back(r1);
    rs.push_back(r2);
    rs.push_back(r3);
    rs.push_back(r4);

    Record e1(s2);
    e1.set_uint(0, 2ULL);
    e1.set_uint(1, 16ULL);
    Record e2(s2);
    e2.set_uint(0, 5ULL);
    e2.set_uint(1, 11ULL);

    std::vector<Record> expected_rs = {e1, e2};

    std::vector<Record> proc_rs;
    EXPECT_TRUE(aggregate_op->process(rs, proc_rs));

    compareRecordStreams(proc_rs, expected_rs);
  }
}

TEST(AggregateOperatorTest, SumPosNeg) {
  Schema s1({kUInt, kUInt, kUInt});
  Schema s2({kUInt, kUInt});

  std::vector<ColumnID> group_cols = {1};
  AggregateOperator::Func agg_func = AggregateOperator::FuncSum;
  ColumnID agg_col = 2;
  std::vector<ColumnID> out_cols = group_cols;
  out_cols.push_back(agg_col);

  std::shared_ptr<AggregateOperator> aggregate_op =
      std::make_shared<AggregateOperator>(group_cols, agg_func, agg_col, s1,
                                          out_cols);

  {
    // Description: The test consists of two stages:
    // STAGE1: records are fed for the first time, expect all output records to
    // be positive; STAGE2: a mix of positive and negative records are fed,
    // output records can contain either positive or negative records.
    Record r1(s1);
    r1.set_uint(0, 1ULL);
    r1.set_uint(1, 2ULL);
    r1.set_uint(2, 9ULL);
    Record r2(s1);
    r2.set_uint(0, 2ULL);
    r2.set_uint(1, 2ULL);
    r2.set_uint(2, 7ULL);
    Record r3(s1);
    r3.set_uint(0, 3ULL);
    r3.set_uint(1, 5ULL);
    r3.set_uint(2, 5ULL);
    Record r4(s1);
    r4.set_uint(0, 4ULL);
    r4.set_uint(1, 5ULL);
    r4.set_uint(2, 6ULL);

    std::vector<Record> rs;
    // feed records
    rs.push_back(r1);
    rs.push_back(r2);
    rs.push_back(r3);
    rs.push_back(r4);

    // STAGE1
    std::vector<Record> proc_rs;
    EXPECT_TRUE(aggregate_op->process(rs, proc_rs));

    Record r5(s1, false);
    r5.set_uint(0, 5ULL);
    r5.set_uint(1, 2ULL);
    r5.set_uint(2, 6ULL);
    Record r6(s1);
    r6.set_uint(0, 6ULL);
    r6.set_uint(1, 7ULL);
    r6.set_uint(2, 7ULL);

    std::vector<Record> rs1;
    // feed records
    rs1.push_back(r5);
    rs1.push_back(r6);

    // STAGE2
    std::vector<Record> proc_rs1;
    EXPECT_TRUE(aggregate_op->process(rs1, proc_rs1));

    Record e1(s2, false);
    e1.set_uint(0, 2ULL);
    e1.set_uint(1, 16ULL);
    Record e2(s2);
    e2.set_uint(0, 2ULL);
    e2.set_uint(1, 10ULL);
    Record e3(s2);
    e3.set_uint(0, 7ULL);
    e3.set_uint(1, 7ULL);

    std::vector<Record> expected_rs = {e1, e2, e3};

    compareRecordStreams(proc_rs1, expected_rs);
  }
}

TEST(AggregateOperatorTest, CountPos) {
  Schema s1({kUInt, kUInt, kUInt});
  Schema s2({kUInt, kUInt});

  std::vector<ColumnID> group_cols = {1};
  AggregateOperator::Func agg_func = AggregateOperator::FuncCount;
  ColumnID agg_col = 2;
  std::vector<ColumnID> out_cols = group_cols;
  out_cols.push_back(agg_col);

  std::shared_ptr<AggregateOperator> aggregate_op =
      std::make_shared<AggregateOperator>(group_cols, agg_func, agg_col, s1,
                                          out_cols);

  {
    Record r1(s1);
    r1.set_uint(0, 1ULL);
    r1.set_uint(1, 2ULL);
    r1.set_uint(2, 9ULL);
    Record r2(s1);
    r2.set_uint(0, 2ULL);
    r2.set_uint(1, 2ULL);
    r2.set_uint(2, 7ULL);
    Record r3(s1);
    r3.set_uint(0, 3ULL);
    r3.set_uint(1, 5ULL);
    r3.set_uint(2, 5ULL);
    Record r4(s1);
    r4.set_uint(0, 4ULL);
    r4.set_uint(1, 5ULL);
    r4.set_uint(2, 6ULL);

    std::vector<Record> rs;
    // feed records
    rs.push_back(r1);
    rs.push_back(r2);
    rs.push_back(r3);
    rs.push_back(r4);

    Record e1(s2);
    e1.set_uint(0, 2ULL);
    e1.set_uint(1, 2ULL);
    Record e2(s2);
    e2.set_uint(0, 5ULL);
    e2.set_uint(1, 2ULL);

    std::vector<Record> expected_rs = {e1, e2};

    std::vector<Record> proc_rs;
    EXPECT_TRUE(aggregate_op->process(rs, proc_rs));

    compareRecordStreams(proc_rs, expected_rs);
  }
}

TEST(AggregateOperatorTest, CountPosNeg) {
  Schema s1({kUInt, kUInt, kUInt});
  Schema s2({kUInt, kUInt});

  std::vector<ColumnID> group_cols = {1};
  AggregateOperator::Func agg_func = AggregateOperator::FuncCount;
  ColumnID agg_col = 2;
  std::vector<ColumnID> out_cols = group_cols;
  out_cols.push_back(agg_col);

  std::shared_ptr<AggregateOperator> aggregate_op =
      std::make_shared<AggregateOperator>(group_cols, agg_func, agg_col, s1,
                                          out_cols);

  {
    // Description: The test consists of two stages:
    // STAGE1: records are fed for the first time, expect all output records to
    // be positive; STAGE2: a mix of positive and negative records are fed,
    // output records can contain either positive or negative records.
    Record r1(s1);
    r1.set_uint(0, 1ULL);
    r1.set_uint(1, 2ULL);
    r1.set_uint(2, 9ULL);
    Record r2(s1);
    r2.set_uint(0, 2ULL);
    r2.set_uint(1, 2ULL);
    r2.set_uint(2, 7ULL);
    Record r3(s1);
    r3.set_uint(0, 3ULL);
    r3.set_uint(1, 5ULL);
    r3.set_uint(2, 5ULL);
    Record r4(s1);
    r4.set_uint(0, 4ULL);
    r4.set_uint(1, 5ULL);
    r4.set_uint(2, 6ULL);

    std::vector<Record> rs;
    // feed records
    rs.push_back(r1);
    rs.push_back(r2);
    rs.push_back(r3);
    rs.push_back(r4);

    // STAGE1
    std::vector<Record> proc_rs;
    EXPECT_TRUE(aggregate_op->process(rs, proc_rs));

    Record r5(s1, false);
    r5.set_uint(0, 5ULL);
    r5.set_uint(1, 2ULL);
    r5.set_uint(2, 6ULL);
    Record r6(s1);
    r6.set_uint(0, 6ULL);
    r6.set_uint(1, 5ULL);
    r6.set_uint(2, 7ULL);
    Record r7(s1);
    r7.set_uint(0, 7ULL);
    r7.set_uint(1, 7ULL);
    r7.set_uint(2, 7ULL);
    Record r8(s1, false);
    r8.set_uint(0, 5ULL);
    r8.set_uint(1, 2ULL);
    r8.set_uint(2, 6ULL);

    std::vector<Record> rs1;
    // feed records
    rs1.push_back(r5);
    rs1.push_back(r6);
    rs1.push_back(r7);
    rs1.push_back(r8);

    // STAGE2
    std::vector<Record> proc_rs1;
    EXPECT_TRUE(aggregate_op->process(rs1, proc_rs1));

    Record e1(s2, false);
    e1.set_uint(0, 2ULL);
    e1.set_uint(1, 2ULL);
    Record e2(s2);
    e2.set_uint(0, 2ULL);
    e2.set_uint(1, 0ULL);
    Record e3(s2, false);
    e3.set_uint(0, 5ULL);
    e3.set_uint(1, 2ULL);
    Record e4(s2);
    e4.set_uint(0, 5ULL);
    e4.set_uint(1, 3ULL);
    Record e5(s2);
    e5.set_uint(0, 7ULL);
    e5.set_uint(1, 1ULL);

    std::vector<Record> expected_rs = {e1, e2, e3, e4, e5};

    compareRecordStreams(proc_rs1, expected_rs);
  }
}

}  // namespace dataflow
