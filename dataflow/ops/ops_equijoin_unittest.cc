#include <memory>

#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/filter.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/ops/equijoin.h"
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

  auto join_op = make_shared<EquiJoin<(0ul, 0ul);
  df.addNode(join_op, {leftSrc, rightSrc});

  // push data into 

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
