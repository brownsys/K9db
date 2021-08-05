#include "pelton/dataflow/channel.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/batch_message.h"
#include "pelton/dataflow/message.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {
using CType = sqlast::ColumnDefinition::Type;

inline SchemaRef CreateSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::INT, CType::INT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
}

TEST(ChannelTest, BasicTest) {
  std::shared_ptr<Channel> channel = std::make_shared<Channel>();
  SchemaRef schema = CreateSchema();
  // Create records
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_s, 2_s, 9_s);
  records.emplace_back(schema, true, 2_s, 2_s, 7_s);

  std::vector<Record> expected_records;
  expected_records.emplace_back(records.at(0).Copy());
  expected_records.emplace_back(records.at(1).Copy());

  std::shared_ptr<BatchMessage> msg =
      std::make_shared<BatchMessage>(0, 0, std::move(records));
  EXPECT_EQ(records.size(), 0);
  EXPECT_TRUE(channel->Send(msg));
  EXPECT_EQ(channel->size(), 1);
  std::vector<std::shared_ptr<Message>> data = channel->Recv();
  EXPECT_EQ(data.size(), 1);
  EXPECT_EQ(data.at(0)->type(), Message::Type::BATCH);
  EXPECT_EQ(std::dynamic_pointer_cast<BatchMessage>(data.at(0))->records(),
            expected_records);
  EXPECT_EQ(channel->size(), 0);
}

}  // namespace dataflow
}  // namespace pelton
