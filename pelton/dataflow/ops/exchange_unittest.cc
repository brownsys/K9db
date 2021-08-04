#include "pelton/dataflow/ops/exchange.h"

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/identity.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/stop_message.h"
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
  SchemaRef schema = CreateSchema();

  // Create an exchange opertor that belongs to 0th partition and that is
  // supposed to shard records into three partitions.
  absl::flat_hash_map<uint16_t, std::shared_ptr<Channel>> partition_chans;
  partition_chans.emplace(0, std::make_shared<Channel>());
  partition_chans.emplace(1, std::make_shared<Channel>());
  partition_chans.emplace(2, std::make_shared<Channel>());

  // A basic dataflow graph needs to be setup because the exchange operator
  // makes use of it's graph pointer
  DataFlowGraph g;
  auto input = std::make_shared<InputOperator>("test-table1", schema);
  auto exchange = std::make_shared<ExchangeOperator>(
      partition_chans, std::vector<ColumnID>{0}, 0, 3);
  auto identity = std::make_shared<IdentityOperator>();
  EXPECT_TRUE(g.AddInputNode(input));
  EXPECT_TRUE(g.AddNode(exchange, input));
  // Add a dummy child operator so that the exchange op can make use of its
  // index internally.
  EXPECT_TRUE(g.AddNode(identity, exchange));
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

  std::optional<std::vector<Record>> outputs =
      exchange->Process(UNDEFINED_NODE_INDEX, records);

  // Collect records that are meant for other partitions and check if they are
  // as expected.
  std::vector<std::shared_ptr<Message>> partition1 =
      partition_chans.at(1)->Recv();
  EXPECT_EQ(partition1.size(), 1);
  EXPECT_EQ(std::dynamic_pointer_cast<BatchMessage>(partition1.at(0))
                ->records()
                .size(),
            1);
  EXPECT_EQ(std::dynamic_pointer_cast<BatchMessage>(partition1.at(0))
                ->records()
                .at(0),
            records.at(0));

  std::vector<std::shared_ptr<Message>> partition2 =
      partition_chans.at(2)->Recv();
  EXPECT_EQ(partition2.size(), 1);
  EXPECT_EQ(std::dynamic_pointer_cast<BatchMessage>(partition2.at(0))
                ->records()
                .size(),
            2);
  EXPECT_EQ(std::dynamic_pointer_cast<BatchMessage>(partition2.at(0))
                ->records()
                .at(0),
            records.at(1));
  EXPECT_EQ(std::dynamic_pointer_cast<BatchMessage>(partition2.at(0))
                ->records()
                .at(1),
            records.at(3));

  // Check for current partition (i.e. 0th partition).
  EXPECT_EQ(outputs.value().size(), 1);
  EXPECT_EQ(outputs.value().at(0), records.at(2));
}

}  // namespace dataflow
}  // namespace pelton
