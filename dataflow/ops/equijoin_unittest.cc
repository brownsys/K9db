#include <memory>

#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/equijoin.h"
#include "dataflow/ops/filter.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"
#include "gtest/gtest.h"

namespace dataflow {

TEST(JoinOperatorTest, Basic) {
  using namespace std;

  auto leftSrc = make_shared<InputOperator>();
  auto rightSrc = make_shared<InputOperator>();

  DataFlowGraph df;
  df.AddInputNode(leftSrc);
  df.AddInputNode(rightSrc);

  auto join_op = make_shared<EquiJoin>(0ul, 0ul);
  df.AddNode(join_op, {leftSrc, rightSrc});

  // push data into
  Schema s_left({DataType::kInt, DataType::kInt});
  Schema s_right({DataType::kInt, DataType::kInt});

  vector<Record> r_left;
  vector<Record> r_right;
  vector<Record> r_ref;

  {
    Record r1(s_left);
    r1.set_int(0, 10);
    r1.set_int(1, -2);
    Record r2(s_left);
    r2.set_int(0, 20);
    r2.set_int(1, 0);
    Record r3(s_left);
    r3.set_int(0, 30);
    r3.set_int(1, 2);
    r_left = vector<Record>{r1, r2, r3};
  }

  {
      Record r1(s_right);
      r1.set_int(0, 10);
      r1.set_int(1, 200);
      Record r2(s_right);
      r2.set_int(0, 10);
      r2.set_int(1, 190);
      Record r3(s_right);
      r3.set_int(0, 30);
      r3.set_int(1, 180);
      r_right = vector<Record>{r1, r2, r3};
  }

  // reference result:
  // for join, we basically stream both in & then push the results.


  // std::vector<ColumnID> cids = {0, 1};
  // std::vector<FilterOperator::Ops> comp_ops = {FilterOperator::OpsGT_Eq,
  //                                              FilterOperator::OpsEq};
  // std::vector<RecordData> comp_vals = {RecordData(3ULL), RecordData(5ULL)};
  //
  // std::shared_ptr<FilterOperator> filter =
  //     std::make_shared<FilterOperator>(cids, comp_ops, comp_vals);
  // std::vector<Record> rs;
  // std::vector<Record> proc_rs;
  //
  // EXPECT_TRUE(filter->process(rs, proc_rs));
  // // no records have been fed
  // EXPECT_EQ(proc_rs, std::vector<Record>());
  //
  // // feed records
  // std::vector<RecordData> rd1 = {RecordData(1ULL), RecordData(2ULL)};
  // std::vector<RecordData> rd2 = {RecordData(2ULL), RecordData(2ULL)};
  // std::vector<RecordData> rd3 = {RecordData(3ULL), RecordData(5ULL)};
  // std::vector<RecordData> rd4 = {RecordData(4ULL), RecordData(5ULL)};
  // Record r1(true, rd1, 3ULL);
  // Record r2(true, rd2, 3ULL);
  // Record r3(true, rd3, 3ULL);
  // Record r4(true, rd4, 3ULL);
  // rs.push_back(r1);
  // rs.push_back(r2);
  // rs.push_back(r3);
  // rs.push_back(r4);
  //
  // std::vector<RecordData> keys = {RecordData(3ULL), RecordData(4ULL)};
  // std::vector<Record> expected_rs = {r3, r4};
  //
  // EXPECT_TRUE(filter->process(rs, proc_rs));
  // EXPECT_EQ(proc_rs.size(), expected_rs.size());
  // EXPECT_EQ(proc_rs, expected_rs);
}

}  // namespace dataflow
