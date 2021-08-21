#include "pelton/dataflow/partition.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaRef CreateSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

TEST(PartitionTest, BasicTest) {
  SchemaRef schema = CreateSchema();
  // Create records
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_u,
                       std::make_unique<std::string>("data1"), 10_s);
  records.emplace_back(schema, true, 2_u,
                       std::make_unique<std::string>("data2"), 8_s);

  std::vector<ColumnID> partition_cols = {0, 2};

  absl::flat_hash_map<uint16_t, std::vector<Record>> partitions =
      partition::HashPartition(records, partition_cols, 2);
  EXPECT_EQ(partitions.size(), 2);
  EXPECT_EQ(partitions.at(0).size(), 1);
  EXPECT_EQ(partitions.at(1).size(), 1);
  EXPECT_EQ(partitions.at(0).at(0), records.at(1));
  EXPECT_EQ(partitions.at(1).at(0), records.at(0));
}

}  // namespace dataflow
}  // namespace pelton
