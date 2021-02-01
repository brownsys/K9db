#include <memory>

#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/project.h"
#include "dataflow/ops/identity.h"
#include "dataflow/ops/input.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"
#include "gtest/gtest.h"

namespace dataflow {

TEST(ProjectOperatorTest, Basic) {
  Schema s1({kUInt, kUInt, kInt});  // must outlive records
  Schema s2({kUInt, kInt});

  std::vector<ColumnID> cids = {0, 2};

  std::shared_ptr<ProjectOperator> project =
      std::make_shared<ProjectOperator>(cids, s1);

  {
    Record r1(s1);
    r1.set_uint(0, 1ULL);
    r1.set_uint(1, 2ULL);
    r1.set_int(2, 3ULL);
    Record r2(s1);
    r2.set_uint(0, 2ULL);
    r2.set_uint(1, 2ULL);
    r2.set_int(2, 3ULL);
    Record r3(s1);
    r3.set_uint(0, 3ULL);
    r3.set_uint(1, 5ULL);
    r3.set_int(2, 6ULL);
    Record r4(s1);
    r4.set_uint(0, 4ULL);
    r4.set_uint(1, 5ULL);
    r4.set_int(2, 6ULL);

    std::vector<Record> rs;

    // feed records
    rs.push_back(r1);
    rs.push_back(r2);
    rs.push_back(r3);
    rs.push_back(r4);

    Record e1(s2);
    e1.set_uint(0, 1ULL);
    e1.set_int(1, 3ULL);
    Record e2(s2);
    e2.set_uint(0, 2ULL);
    e2.set_int(1, 3ULL);
    Record e3(s2);
    e3.set_uint(0, 3ULL);
    e3.set_int(1, 6ULL);
    Record e4(s2);
    e4.set_uint(0, 4ULL);
    e4.set_int(1, 6ULL);
    std::vector<Record> expected_rs = {e1, e2, e3, e4};

    std::vector<Record> proc_rs;
    EXPECT_TRUE(project->process(rs, proc_rs));
    EXPECT_EQ(proc_rs, expected_rs);
  }
}

}  // namespace dataflow
