#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/filter.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"

#include <memory>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dataflow {

DataFlowGraph makeTrivialGraph() {
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>();
  auto ident = std::make_shared<IdentityOperator>();

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(OperatorType::IDENTITY, ident, in));
  std::vector<ColumnID> keycol = {0};
  EXPECT_TRUE(g.AddNode(OperatorType::MAT_VIEW,
                        std::make_shared<MatViewOperator>(keycol), ident));

  return g;
}

DataFlowGraph makeFilterGraph() {
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>();

  std::vector<ColumnID> cids = {0, 1};
  std::vector<FilterOperator::Ops> comp_ops = {
      FilterOperator::GreaterThanOrEqual, FilterOperator::Equal};
  std::vector<RecordData> filter_vals = {RecordData(10ULL), RecordData(6ULL)};
  auto filter = std::make_shared<FilterOperator>(cids, comp_ops, filter_vals);

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(OperatorType::FILTER, filter, in));
  std::vector<ColumnID> keycol = {0};
  EXPECT_TRUE(g.AddNode(OperatorType::MAT_VIEW,
                        std::make_shared<MatViewOperator>(keycol), filter));

  return g;
}

TEST(DataFlowGraphTest, Construct) { DataFlowGraph g = makeTrivialGraph(); }

TEST(DataFlowGraphTest, Basic) {
  DataFlowGraph g = makeTrivialGraph();

  std::shared_ptr<InputOperator> in = g.inputs()[0];
  std::shared_ptr<MatViewOperator> out = g.outputs()[0];

  std::vector<Record> rs;
  RecordData key(42ULL);

  std::vector<Record> proc_rs;

  EXPECT_TRUE(g.Process(*in, rs));
  // no records should have made it to the materialized view
  EXPECT_EQ(out->lookup(key), std::vector<Record>());

  std::vector<RecordData> rd = {key, RecordData(5ULL)};
  Record r(true, rd, 3ULL);
  rs.push_back(r);

  EXPECT_TRUE(g.Process(*in, rs));
  EXPECT_EQ(out->lookup(key), rs);
}

TEST(DataFlowGraphTest, SinglePathFilter) {
  DataFlowGraph g = makeFilterGraph();

  std::shared_ptr<InputOperator> in = g.inputs()[0];
  std::shared_ptr<MatViewOperator> out = g.outputs()[0];

  RecordData key(42ULL);

  std::vector<Record> rs1;
  std::vector<RecordData> rd1 = {key, RecordData(6ULL)};
  Record r1(true, rd1, 3ULL);
  rs1.push_back(r1);

  EXPECT_TRUE(g.Process(*in, rs1));
  // record should be in mat view
  EXPECT_EQ(out->lookup(key), rs1);

  std::vector<Record> rs2;

  std::vector<RecordData> rd2 = {key, RecordData(7ULL)};
  Record r2(true, rd2, 3ULL);
  rs2.push_back(r2);

  EXPECT_TRUE(g.Process(*in, rs2));
  // should still only have the first record
  EXPECT_EQ(out->lookup(key), rs1);
}

}  // namespace dataflow

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  FLAGS_v = 5;
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
