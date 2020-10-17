#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"
#include "dataflow/filter.h"

#include <memory>

#include "gtest/gtest.h"

namespace dataflow {

DataFlowGraph makeGraph() {
  DataFlowGraph g;

  std::vector<ColumnID> cids = {0,1};
  std::vector<FilterOperator::Ops> comp_ops = {FilterOperator::OpsGT_Eq, FilterOperator::OpsEq};
  std::vector<RecordData> comp_vals = {RecordData(3ULL), RecordData(5ULL)};
  std::vector<ColumnID> keycol = {0};

  auto in = std::make_shared<InputOperator>();
  auto filter = std::make_shared<FilterOperator>(cids, comp_ops, comp_vals);
  auto matview = std::make_shared<MatViewOperator>(keycol));

  EXPECT_TRUE(g.AddNode(OperatorType::INPUT, in));
  EXPECT_TRUE(g.AddNode(OperatorType::FILTER, filter));
  EXPECT_TRUE(g.AddNode(OperatorType::MAT_VIEW, matview);

  EXPECT_TRUE(g.AddEdge(in, filter));
  EXPECT_TRUE(g.AddEdge(filter, matview));

  return g;
}

TEST(FilterOperatorTest, Construct) { DataFlowGraph g = makeGraph(); }

TEST(FilterOperatorTest, Basic) {
  DataFlowGraph g = makeGraph();

  std::shared_ptr<InputOperator> in = g.inputs()[0];
  std::shared_ptr<MatViewOperator> out = g.outputs()[0];

  std::vector<Record> rs;
  std::vector<Record> proc_rs;

  EXPECT_TRUE(in->process(rs, proc_rs));
  // no records should have made it to the materialized view
  EXPECT_EQ(out->lookup(key), std::vector<Record>());

  // feed records
  std::vector<RecordData> rd1 = {RecordData(1ULL), RecordData(2ULL)};
  std::vector<RecordData> rd2 = {RecordData(2ULL), RecordData(2ULL)};
  std::vector<RecordData> rd3 = {RecordData(3ULL), RecordData(5ULL)};
  std::vector<RecordData> rd4 = {RecordData(4ULL), RecordData(5ULL)};
  Record r1(true, rd1, 0ULL);
  Record r2(true, rd2, 0ULL);
  Record r3(true, rd3, 0ULL);
  Record r4(true, rd4, 0ULL);
  rs.push_back(r1);
  rs.push_back(r2);
  rs.push_back(r3);
  rs.push_back(r4);

  std::vector<RecordData> keys = {RecordData(3ULL), RecordData(4ULL)};
  std::vector<Record> expected_rs = {r3,r4}

  EXPECT_TRUE(in->process(rs, proc_rs));
  EXPECT_EQ(out->multi_lookup(keys), expected_rs);
}

}  // namespace dataflow
