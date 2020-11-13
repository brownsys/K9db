#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"

#include <memory>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dataflow {

DataFlowGraph makeGraph() {
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>();
  auto ident = std::make_shared<IdentityOperator>();

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(ident, in));
  std::vector<ColumnID> keycol = {0};
  EXPECT_TRUE(g.AddNode(std::make_shared<MatViewOperator>(keycol), ident));

  return g;
}

TEST(DataFlowGraphTest, Construct) { DataFlowGraph g = makeGraph(); }

TEST(DataFlowGraphTest, Basic) {
  DataFlowGraph g = makeGraph();

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

}  // namespace dataflow

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  FLAGS_v = 5;
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
