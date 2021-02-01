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

  static const auto LEFT_KEY_IDX = 0ul;
  static const auto RIGHT_KEY_IDX = 0ul;

  auto join_op = make_shared<EquiJoin>(LEFT_KEY_IDX, RIGHT_KEY_IDX);
  df.AddNode(join_op, {leftSrc, rightSrc});

  // push data into
  auto s_left = SchemaFactory::create_or_get({DataType::kInt, DataType::kInt});
  auto s_right = SchemaFactory::create_or_get({DataType::kInt, DataType::kInt});

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
  // => batches are processed
  // join result should be:
  // 10, -2 joins 10, 200
  // 10, -2 joins 10, 190
  // 30, 2 joins 30, 180

  // output schema of join is defined as concatenated left schema w. key column
  // dropped and right schema
  auto s_joined = SchemaFactory::create_or_get({DataType::kInt, DataType::kInt, DataType::kInt});
  vector<Record> ref;
  {
    Record r1(s_joined);
    r1.set_int(1, -2);
    r1.set_int(0, 10);
    r1.set_int(2, 200);

    Record r2(s_joined);
    r2.set_int(1, -2);
    r2.set_int(0, 10);
    r2.set_int(2, 190);

    Record r3(s_joined);
    r3.set_int(1, 2);
    r3.set_int(0, 30);
    r3.set_int(2, 180);

    ref = vector<Record>{r1, r2, r3};
  }

  // sort ref vector after 3rd column
  std::sort(ref.begin(), ref.end(), [](const Record& a, const Record& b) {
    return a.as_int(2) < b.as_int(2);
  });

  vector<Record> out_rs;
  // process left first
  join_op->process(join_op->left()->index(), r_left, out_rs);
  ASSERT_EQ(out_rs.size(), 0);  // no right records so output should be empty.
                                // => Everything saved to internal hash table
  // process right now => records should occur
  join_op->process(join_op->right()->index(), r_right, out_rs);
  ASSERT_EQ(out_rs.size(), 3);

  // check all records have size 3
  for (const auto& r : out_rs) {
    ASSERT_EQ(r.schema().num_columns(), 3);
  }

    // sort result vector
    std::sort(out_rs.begin(), out_rs.end(), [](const Record& a, const Record&
    b) {
      return a.as_int(2) < b.as_int(2);
    });

  // compare
  EXPECT_EQ(out_rs.size(), ref.size());
    for(unsigned i = 0; i < std::min(out_rs.size(), ref.size()); ++i) {
      EXPECT_EQ(out_rs[i], ref[i]);
    }

  // TODO: fix schema memory errors...
  // ==> global string pool???
  // --> i.e. schema should outlive everything...
}

}  // namespace dataflow
