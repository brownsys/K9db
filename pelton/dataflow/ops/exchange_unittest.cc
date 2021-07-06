#include "pelton/dataflow/ops/exchange.h"

#include <memory>
#include <string>
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

TEST(ExchangeOperatorTest, BasicTest) {
  // Create an exchange opertor that is supposed to shared the records into
  // three partitions.
  absl::flat_hash_map<uint16_t, std::shared_ptr<Channel>> peer_chans;
  peer_chans.emplace(1, std::make_shared<Channel>());
  peer_chans.emplace(2, std::make_shared<Channel>());
  peer_chans.emplace(3, std::make_shared<Channel>());

  auto exchange =
      ExchangeOperator(std::make_shared<Channel>(), std::make_shared<Channel>(),
                       peer_chans, std::vector<ColumnID>{0}, 0, 3);

  SchemaRef schema = CreateSchema();
  // Create records
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_u, std::make_unique<std::string>("CS"),
                       9_s);
  records.emplace_back(schema, true, 2_u, std::make_unique<std::string>("CS"),
                       7_s);
  records.emplace_back(schema, true, 3_u, std::make_unique<std::string>("Mech"),
                       5_s);
  records.emplace_back(schema, true, 5_u, std::make_unique<std::string>("Mech"),
                       7_s);

  std::vector<Record> outputs;
  EXPECT_TRUE(exchange.Process(UNDEFINED_NODE_INDEX, records, &outputs));
  // Collect records that are meant for peers and check if they are as expected.
  std::vector<std::shared_ptr<BatchMessage>> peer1 = peer_chans.at(1)->Recv();
  EXPECT_EQ(peer1.size(), 1);
  EXPECT_EQ(peer1.at(0)->records.size(), 1);
  EXPECT_EQ(peer1.at(0)->records.at(0), records.at(0));

  std::vector<std::shared_ptr<BatchMessage>> peer2 = peer_chans.at(2)->Recv();
  EXPECT_EQ(peer2.size(), 1);
  EXPECT_EQ(peer2.at(0)->records.size(), 2);
  EXPECT_EQ(peer2.at(0)->records.at(0), records.at(1));
  EXPECT_EQ(peer2.at(0)->records.at(1), records.at(3));

  // Check for current partition.
  EXPECT_EQ(outputs.size(), 1);
  EXPECT_EQ(outputs.at(0), records.at(2));
}

}  // namespace dataflow
}  // namespace pelton
