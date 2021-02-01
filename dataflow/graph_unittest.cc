#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/filter.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"
#include "dataflow/schema.h"

#include <memory>

#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dataflow {

DataFlowGraph makeTrivialGraph() {
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>("test-table");
  auto ident = std::make_shared<IdentityOperator>();

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(ident, in));
  std::vector<ColumnID> keycol = {0};
  EXPECT_TRUE(g.AddNode(std::make_shared<MatViewOperator>(keycol), ident));

  return g;
}

DataFlowGraph makeFilterGraph(const Schema& schema) {
  DataFlowGraph g;

  auto in = std::make_shared<InputOperator>("test-table");

  std::vector<ColumnID> cids = {0, 1};
  std::vector<FilterOperator::Ops> comp_ops = {
      FilterOperator::GreaterThanOrEqual, FilterOperator::Equal};
  Record filter_vals(schema);
  filter_vals.set_uint(0, 10ULL);
  filter_vals.set_uint(1, 6ULL);
  auto filter = std::make_shared<FilterOperator>(cids, comp_ops, filter_vals);

  EXPECT_TRUE(g.AddInputNode(in));
  EXPECT_TRUE(g.AddNode(filter, in));
  EXPECT_TRUE(g.AddNode(std::make_shared<MatViewOperator>(schema.key_columns()),
                        filter));

  return g;
}

TEST(DataFlowGraphTest, Construct) { DataFlowGraph g = makeTrivialGraph(); }

TEST(DataFlowGraphTest, Basic) {
  auto schema = SchemaFactory::create_or_get({DataType::kUInt, DataType::kUInt});
  schema.set_key_columns({0});
  {
    DataFlowGraph g = makeTrivialGraph();

    std::shared_ptr<InputOperator> in = g.inputs()[0];
    std::shared_ptr<MatViewOperator> out = g.outputs()[0];

    Key key(uint64_t(42));

    std::vector<Record> rs;
    std::vector<Record> proc_rs;

    EXPECT_TRUE(g.Process(*in, rs));
    // no records should have made it to the materialized view
    EXPECT_EQ(out->lookup(key), std::vector<Record>());

    Record r(schema);
    r.set_uint(0, 42ULL);
    r.set_uint(1, 5ULL);
    rs.push_back(r);

    EXPECT_TRUE(g.Process(*in, rs));
    EXPECT_EQ(out->lookup(key), rs);
  }
}

TEST(DataFlowGraphTest, SinglePathFilter) {
  auto schema = SchemaFactory::create_or_get({DataType::kUInt, DataType::kUInt});
  schema.set_key_columns({0});
  {
    DataFlowGraph g = makeFilterGraph(schema);

    std::shared_ptr<InputOperator> in = g.inputs()[0];
    std::shared_ptr<MatViewOperator> out = g.outputs()[0];

    Key key(uint64_t(42));

    std::vector<Record> rs1;
    Record r1(schema);
    r1.set_uint(0, uint64_t(42));
    r1.set_uint(1, 6ULL);
    rs1.push_back(r1);

    EXPECT_TRUE(g.Process(*in, rs1));
    // record should be in mat view
    EXPECT_EQ(out->lookup(key), rs1);

    std::vector<Record> rs2;

    Record r2(schema);
    r2.set_uint(0, uint64_t(42));
    r2.set_uint(1, 7ULL);
    rs2.push_back(r2);

    EXPECT_TRUE(g.Process(*in, rs2));
    // should still only have the first record
    EXPECT_EQ(out->lookup(key), rs1);
  }
}

TEST(DataFlowGraphTest, ParentChildren) {
  DataFlowGraph g;
  auto in = std::make_shared<InputOperator>();
  auto ident = std::make_shared<IdentityOperator>();
  g.AddInputNode(in);
  g.AddNode(ident, in);

  auto parents = ident->parents();
  ASSERT_EQ(parents.size(), 1);
  EXPECT_EQ(parents.front()->index(), in->index());
}

}  // namespace dataflow

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  FLAGS_v = 5;
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);

  return RUN_ALL_TESTS();
}
