#include "dataflow/ops/aggregate.h"

#include <memory>

#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dataflow {

void compareRecordStreams(std::vector<Record>& rs1, std::vector<Record>& rs2){
  //NOTE: Only compares records of size 2 (specific to these test cases)
  EXPECT_EQ(rs1.size(), rs2.size());
  //the records can be out of order, hence can't use EXPECT_EQ(proc_rs,expected_rs) for data comparision
  for(size_t i = 0; i<rs1.size(); i++){
    bool flag = false;
    for(size_t j = 0; j<rs2.size(); j++){
      if(rs1[i].positive()==rs2[j].positive() && rs1[i].raw_at(0).as_val()==rs2[j].raw_at(0).as_val() && rs1[i].raw_at(1).as_val() == rs2[j].raw_at(1).as_val()){
        flag = true;
      }
    }
    EXPECT_EQ(flag, true);
  }
}

TEST(AggregateOperatorTest, Sum) {
  //Description: The test consists of two stages:
  //STAGE1: records are fed for the first time, expect all out records to be positive
  //STAGE2: a mix of positive and negative records are fed, out records can contain either positive or negative records.
  std::vector<ColumnID> group_cols = {1};
  AggregateOperator::Func agg_func = AggregateOperator::FuncSum;
  ColumnID agg_col = 2;

  std::shared_ptr<AggregateOperator> aggregate_op =
      std::make_shared<AggregateOperator>(group_cols, agg_func, agg_col);
  std::vector<Record> rs;
  std::vector<Record> proc_rs;

  EXPECT_TRUE(aggregate_op->process(rs, proc_rs));
  // no records have been fed
  EXPECT_EQ(proc_rs, std::vector<Record>());

  // feed records
  std::vector<RecordData> rd1 = {RecordData(1ULL), RecordData(2ULL),
                                 RecordData(9ULL)};
  std::vector<RecordData> rd2 = {RecordData(2ULL), RecordData(2ULL),
                                 RecordData(7ULL)};
  std::vector<RecordData> rd3 = {RecordData(3ULL), RecordData(5ULL),
                                 RecordData(5ULL)};
  std::vector<RecordData> rd4 = {RecordData(4ULL), RecordData(5ULL),
                                 RecordData(6ULL)};
  Record r1(true, rd1, 3ULL);
  Record r2(true, rd2, 3ULL);
  Record r3(true, rd3, 3ULL);
  Record r4(true, rd4, 3ULL);
  rs.push_back(r1);
  rs.push_back(r2);
  rs.push_back(r3);
  rs.push_back(r4);

  //STAGE1
  EXPECT_TRUE(aggregate_op->process(rs, proc_rs));

  std::vector<RecordData> erd1 = {RecordData(2ULL), RecordData(16ULL)};
  std::vector<RecordData> erd2 = {RecordData(5ULL), RecordData(11ULL)};
  Record er1(true, erd1, 3ULL);
  Record er2(true, erd2, 3ULL);
  std::vector<Record> expected_rs = {er1, er2};

  compareRecordStreams(proc_rs, expected_rs);

  std::vector<Record> rs1;
  std::vector<Record> proc_rs1;
  std::vector<RecordData> rd5 = {RecordData(5ULL), RecordData(2ULL),
                                 RecordData(6ULL)};
  std::vector<RecordData> rd6 = {RecordData(6ULL), RecordData(7ULL),
                                 RecordData(7ULL)};
  Record r5(false, rd5, 3ULL);
  Record r6(true, rd6, 3ULL);
  Record r7(true, rd6, 3ULL);
  rs1.push_back(r5);
  rs1.push_back(r6);

  //STAGE2
  EXPECT_TRUE(aggregate_op->process(rs1, proc_rs1));

  std::vector<RecordData> erd3 = {RecordData(2ULL), RecordData(16ULL)};
  std::vector<RecordData> erd4 = {RecordData(2ULL), RecordData(10ULL)};
  std::vector<RecordData> erd5 = {RecordData(7ULL), RecordData(7ULL)};
  Record er3(false, erd3, 3ULL);
  Record er4(true, erd4, 3ULL);
  Record er5(true, erd5, 3ULL);
  std::vector<Record> expected_rs1 = {er3, er4, er5};

  compareRecordStreams(proc_rs1, expected_rs1);
}

TEST(AggregateOperatorTest, Count) {
  //Description: The test consists of two stages:
  //STAGE1: records are fed for the first time, expect all out records to be positive
  //STAGE2: a mix of positive and negative records are fed, out records can contain either positive or negative records.
  std::vector<ColumnID> group_cols = {1};
  AggregateOperator::Func agg_func = AggregateOperator::FuncCount;
  ColumnID agg_col = 2;

  std::shared_ptr<AggregateOperator> aggregate_op =
      std::make_shared<AggregateOperator>(group_cols, agg_func, agg_col);
  std::vector<Record> rs;
  std::vector<Record> proc_rs;

  EXPECT_TRUE(aggregate_op->process(rs, proc_rs));
  // no records have been fed
  EXPECT_EQ(proc_rs, std::vector<Record>());

  // feed records
  std::vector<RecordData> rd1 = {RecordData(1ULL), RecordData(2ULL),
                                 RecordData(9ULL)};
  std::vector<RecordData> rd2 = {RecordData(2ULL), RecordData(2ULL),
                                 RecordData(7ULL)};
  std::vector<RecordData> rd3 = {RecordData(3ULL), RecordData(5ULL),
                                 RecordData(5ULL)};
  std::vector<RecordData> rd4 = {RecordData(4ULL), RecordData(5ULL),
                                 RecordData(6ULL)};
  Record r1(true, rd1, 3ULL);
  Record r2(true, rd2, 3ULL);
  Record r3(true, rd3, 3ULL);
  Record r4(true, rd4, 3ULL);
  rs.push_back(r1);
  rs.push_back(r2);
  rs.push_back(r3);
  rs.push_back(r4);

  //STAGE1
  EXPECT_TRUE(aggregate_op->process(rs, proc_rs));

  std::vector<RecordData> erd1 = {RecordData(2ULL), RecordData(2ULL)};
  std::vector<RecordData> erd2 = {RecordData(5ULL), RecordData(2ULL)};
  Record er1(true, erd1, 3ULL);
  Record er2(true, erd2, 3ULL);
  std::vector<Record> expected_rs = {er1, er2};

  compareRecordStreams(proc_rs, expected_rs);

  std::vector<Record> rs1;
  std::vector<Record> proc_rs1;
  std::vector<RecordData> rd5 = {RecordData(5ULL), RecordData(2ULL),
                                 RecordData(6ULL)};
  std::vector<RecordData> rd6 = {RecordData(6ULL), RecordData(5ULL),
                                 RecordData(7ULL)};
  std::vector<RecordData> rd7 = {RecordData(7ULL), RecordData(7ULL), RecordData(7ULL)};
  Record r5(false, rd5, 3ULL);
  Record r6(true, rd6, 3ULL);
  Record r7(true, rd7, 3ULL);
  Record r8(false, rd5, 3ULL);
  rs1.push_back(r5);
  rs1.push_back(r6);
  rs1.push_back(r7);
  rs1.push_back(r8);
  rs1.push_back(r8);

  //STAGE2
  EXPECT_TRUE(aggregate_op->process(rs1, proc_rs1));

  std::vector<RecordData> erd3 = {RecordData(2ULL), RecordData(2ULL)};
  std::vector<RecordData> erd4 = {RecordData(2ULL), RecordData(0ULL)};
  std::vector<RecordData> erd5 = {RecordData(5ULL), RecordData(2ULL)};
  std::vector<RecordData> erd6 = {RecordData(5ULL), RecordData(3ULL)};
  std::vector<RecordData> erd7 = {RecordData(7ULL), RecordData(1ULL)};
  Record er3(false, erd3, 3ULL);
  Record er4(true, erd4, 3ULL);
  Record er5(false, erd5, 3ULL);
  Record er6(true, erd6, 3ULL);
  Record er7(true, erd7, 3ULL);
  std::vector<Record> expected_rs1 = {er3, er4, er5, er6, er7};

  compareRecordStreams(proc_rs1, expected_rs1);

  // for (auto i : proc_rs) {
  //   LOG(INFO) << "[OUT_RS]" << i.positive() << " : " << i.raw_at(0).as_val() << ", " << i.raw_at(1).as_val();
  // }
}

}  // namespace dataflow
