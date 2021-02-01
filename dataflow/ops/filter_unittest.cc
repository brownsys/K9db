#include <memory>

#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/filter.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"
#include "gtest/gtest.h"

namespace dataflow {

TEST(FilterOperatorTest, Basic) {
  auto s = SchemaFactory::create_or_get({kUInt, kUInt});  // must outlive records

  std::vector<ColumnID> cids = {0, 1};
  std::vector<FilterOperator::Ops> comp_ops = {
      FilterOperator::GreaterThanOrEqual, FilterOperator::Equal};
  Record comp_vals(s);
  comp_vals.set_uint(0, 3ULL);
  comp_vals.set_uint(1, 5ULL);

  std::shared_ptr<FilterOperator> filter =
      std::make_shared<FilterOperator>(cids, comp_ops, comp_vals);

  {
    Record r1(s);
    r1.set_uint(0, 1ULL);
    r1.set_uint(1, 2ULL);
    Record r2(s);
    r2.set_uint(0, 2ULL);
    r2.set_uint(1, 2ULL);
    Record r3(s);
    r3.set_uint(0, 3ULL);
    r3.set_uint(1, 5ULL);
    Record r4(s);
    r4.set_uint(0, 4ULL);
    r4.set_uint(1, 5ULL);

    std::vector<Record> rs;

    // feed records
    rs.push_back(r1);
    rs.push_back(r2);
    rs.push_back(r3);
    rs.push_back(r4);

    std::vector<Record> expected_rs = {r3, r4};
    std::vector<Record> processed_rs;

    EXPECT_TRUE(filter->process(rs, processed_rs));
    EXPECT_EQ(processed_rs.size(), expected_rs.size());
    EXPECT_EQ(processed_rs, expected_rs);
  }
}

}  // namespace dataflow
