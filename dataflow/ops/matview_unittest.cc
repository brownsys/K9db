#include "dataflow/ops/matview.h"

#include <memory>

#include "dataflow/key.h"
#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "dataflow/types.h"
#include "gtest/gtest.h"

namespace dataflow {

TEST(MatViewOperatorTest, Empty) {
  Schema s({kUInt, kUInt});  // must outlive records
  std::vector<ColumnID> keycol = {0};
  std::shared_ptr<MatViewOperator> matview_op =
      std::make_shared<MatViewOperator>(keycol, s);
  std::vector<Record> rs;
  std::vector<Record> proc_rs;

  EXPECT_TRUE(matview_op->process(rs, proc_rs));
  // no records have been fed
  EXPECT_EQ(proc_rs, std::vector<Record>());
}

TEST(MatViewOperatorTest, SingleLookup) {
  Schema s({kUInt, kUInt});  // must outlive records
  std::vector<ColumnID> keycol = {0};
  std::shared_ptr<MatViewOperator> matview_op =
      std::make_shared<MatViewOperator>(keycol, s);

  s.set_key_columns(keycol);
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

    std::vector<Record> expected_rs = {r3};
    std::vector<Record> processed_rs;

    EXPECT_TRUE(matview_op->process(rs, processed_rs));
    EXPECT_EQ(matview_op->lookup(Key(static_cast<uint64_t>(3))), expected_rs);
  }
}

TEST(MatViewOperatorTest, MultiLookup) {
  Schema s({kUInt, kUInt});  // must outlive records
  std::vector<ColumnID> keycol = {0};
  std::shared_ptr<MatViewOperator> matview_op =
      std::make_shared<MatViewOperator>(keycol, s);

  s.set_key_columns(keycol);
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

    std::vector<Record> expected_rs = {r1, r2, r3, r4};
    std::vector<Record> processed_rs;

    EXPECT_TRUE(matview_op->process(rs, processed_rs));

    Key k1(1ULL, DataType::kUInt);
    Key k2(2ULL, DataType::kUInt);
    Key k3(3ULL, DataType::kUInt);
    Key k4(4ULL, DataType::kUInt);
    std::vector<Key> keys = {k1, k2, k3, k4};
    EXPECT_EQ(matview_op->multi_lookup(keys), expected_rs);
  }
}

}  // namespace dataflow
