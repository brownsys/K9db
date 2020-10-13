#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"

#include <memory>

#include "gtest/gtest.h"

namespace dataflow {

DataFlowGraph makeGraph() {
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>();
  auto ident = std::make_shared<IdentityOperator>();

  EXPECT_TRUE(g.AddNode(OperatorType::INPUT, in));
  EXPECT_TRUE(g.AddNode(OperatorType::IDENTITY, ident));
  std::vector<ColumnID> keycol = {0};
  EXPECT_TRUE(g.AddNode(OperatorType::MAT_VIEW,
                        std::make_shared<MatViewOperator>(keycol)));

  EXPECT_TRUE(g.AddEdge(in, ident));

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

  EXPECT_TRUE(in->process(rs, proc_rs));
  // no records should have made it to the materialized view
  EXPECT_EQ(out->lookup(key), std::vector<Record>());

  std::vector<RecordData> rd = {key, RecordData(5ULL)};
  Record r(true, rd, 0ULL);
  rs.push_back(r);

  EXPECT_TRUE(in->process(rs, proc_rs));
  EXPECT_EQ(out->lookup(key), rs);
}

}  // namespace dataflow
