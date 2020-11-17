#include "dataflow/ops/matview.h"

#include <memory>

#include "dataflow/operator.h"
#include "dataflow/record.h"
#include "gtest/gtest.h"

namespace dataflow {

TEST(MatViewOperatorTest, Basic) {
  std::vector<ColumnID> keycol = {0};
  std::shared_ptr<MatViewOperator> matview =
      std::make_shared<MatViewOperator>(keycol);
  std::vector<Record> rs;
  std::vector<Record> proc_rs;

  EXPECT_TRUE(matview->process(rs, proc_rs));
  // no records have been fed
  EXPECT_EQ(proc_rs, std::vector<Record>());

  // feed records
  std::vector<RecordData> rd1 = {RecordData(1ULL), RecordData(2ULL)};
  std::vector<RecordData> rd2 = {RecordData(2ULL), RecordData(2ULL)};
  std::vector<RecordData> rd3 = {RecordData(3ULL), RecordData(5ULL)};
  std::vector<RecordData> rd4 = {RecordData(4ULL), RecordData(5ULL)};
  Record r1(true, rd1, 3ULL);
  Record r2(true, rd2, 3ULL);
  Record r3(true, rd3, 3ULL);
  Record r4(true, rd4, 3ULL);
  rs.push_back(r1);
  rs.push_back(r2);
  rs.push_back(r3);
  rs.push_back(r4);

  EXPECT_TRUE(matview->process(rs, proc_rs));

  std::vector<Record> expected_rs = {r3};
  EXPECT_EQ(matview->lookup(RecordData(3ULL)), expected_rs);

  std::vector<RecordData> keys = {RecordData(1ULL), RecordData(2ULL),
                                  RecordData(3ULL), RecordData(4ULL)};
  expected_rs = {r1, r2, r3, r4};
  EXPECT_EQ(matview->multi_lookup(keys), expected_rs);
}

}  // namespace dataflow
