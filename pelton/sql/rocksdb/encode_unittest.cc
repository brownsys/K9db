#include "pelton/sql/rocksdb/encode.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"
#include "rocksdb/slice.h"

namespace pelton {
namespace sql {

#define SEP std::string({__ROCKSSEP})
#define NUL std::string({__ROCKSNULL})

using CType = sqlast::ColumnDefinition::Type;

dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
    {"ID", "name", "age", "email", "date"},
    {CType::UINT, CType::TEXT, CType::INT, CType::TEXT, CType::DATETIME}, {0});

// RocksdbSequence.
TEST(RocksdbEncodeTest, RocksdbSequence) {
  RocksdbSequence row;
  row.Append(CType::UINT, "0");
  row.Append(CType::TEXT, "'kinan'");
  row.Append(CType::INT, "-10");
  row.Append(CType::TEXT, "NULL");
  row.Append(CType::DATETIME, "'2022-09-01 00:00:00'");

  // Test At.
  EXPECT_EQ(row.At(0), "0");
  EXPECT_EQ(row.At(1), "kinan");
  EXPECT_EQ(row.At(2), "-10");
  EXPECT_EQ(row.At(3), NUL);
  EXPECT_EQ(row.At(4), "2022-09-01 00:00:00");

  // Test Slice.
  EXPECT_EQ(row.Slice(0, 2), "0" + SEP + "kinan" + SEP);
  EXPECT_EQ(row.Slice(2, 2), "-10" + SEP + NUL + SEP);
  EXPECT_EQ(row.Slice(3, 1), NUL + SEP);
  EXPECT_EQ(row.Slice(2, 3),
            "-10" + SEP + NUL + SEP + "2022-09-01 00:00:00" + SEP);
  EXPECT_EQ(row.Slice(0, 5), row.Data().ToString());

  // Test Update.
  row.Replace(1, CType::TEXT, "'user'");
  row.Replace(2, CType::INT, "-20");
  EXPECT_EQ(row.At(1), "user");
  EXPECT_EQ(row.At(2), "-20");

  // Test release / constructor.
  RocksdbSequence read = RocksdbSequence(row.Release());

  // Test record decoding.
  dataflow::Record record = read.DecodeRecord(schema);
  EXPECT_EQ(record.GetUInt(0), 0_u);
  EXPECT_EQ(record.GetString(1), "user");
  EXPECT_EQ(record.GetInt(2), -20_s);
  EXPECT_EQ(record.IsNull(3), true);
  EXPECT_EQ(record.GetDateTime(4), "2022-09-01 00:00:00");

  // Test Iterator.
  std::vector<std::string> expected = {"0", "user", "-20", NUL + "",
                                       "2022-09-01 00:00:00"};

  RocksdbSequence::Iterator it = read.begin();
  for (const std::string &ex : expected) {
    EXPECT_NE(it, read.end());
    EXPECT_EQ(*it, ex);
    ++it;
  }
  EXPECT_EQ(it, read.end());
}

// RocksdbRecord
TEST(RocksdbEncodeTest, RocksdbRecord) {
  sqlast::Insert stmt("table", false);
  stmt.AddValue("0");
  stmt.AddValue("'user'");
  stmt.AddValue("-20");
  stmt.AddValue("NULL");
  stmt.AddValue("'2022-09-01 00:00:00'");

  // Test from insert.
  RocksdbRecord record = RocksdbRecord::FromInsert(stmt, schema, "shard0");

  // Test key.
  EXPECT_EQ(record.Key().Data(), "shard0" + SEP + "0" + SEP);
  EXPECT_EQ(record.GetShard(), "shard0");
  EXPECT_EQ(record.GetPK(), "0");

  // Test value.
  EXPECT_EQ(record.Value().Data(), "0" + SEP + "user" + SEP + "-20" + SEP +
                                       NUL + SEP + "2022-09-01 00:00:00" + SEP);

  // Test other constructor.
  RocksdbRecord read(record.Key().Release(), record.Value().Release());

  // Test Update.
  std::unordered_map<size_t, std::string> update;
  update.emplace(0, "10");
  update.emplace(2, "NULL");
  update.emplace(3, "'email@gmail.com'");
  read = read.Update(update, schema, "shard2");

  EXPECT_EQ(read.Key().Data(), "shard2" + SEP + "10" + SEP);
  EXPECT_EQ(read.Value().Data(), "10" + SEP + "user" + SEP + NUL + SEP +
                                     "email@gmail.com" + SEP +
                                     "2022-09-01 00:00:00" + SEP);
}

// RocksdbIndexInternalRecord
TEST(RocksdbEncodeTest, RocksdbIndexInternalRecord) {
  // Test piece-wise constructor.
  RocksdbIndexInternalRecord r1("10", "shard0", "5");
  RocksdbIndexInternalRecord r2("20", "shard0", "5");
  RocksdbIndexInternalRecord r3("20", "shard1", "5");

  // Test read constructor.
  r1 = RocksdbIndexInternalRecord(r1.Data());
  r2 = RocksdbIndexInternalRecord(r2.Data());
  r3 = RocksdbIndexInternalRecord(r3.Data());

  // Test data.
  EXPECT_EQ(r1.Data(), "10" + SEP + "shard0" + SEP + "5" + SEP);
  EXPECT_EQ(r2.Data(), "20" + SEP + "shard0" + SEP + "5" + SEP);
  EXPECT_EQ(r3.Data(), "20" + SEP + "shard1" + SEP + "5" + SEP);

  // Test getters.
  EXPECT_EQ(r1.GetShard(), "shard0");
  EXPECT_EQ(r2.GetShard(), "shard0");
  EXPECT_EQ(r3.GetShard(), "shard1");
  EXPECT_EQ(r1.GetPK(), "5");
  EXPECT_EQ(r2.GetPK(), "5");
  EXPECT_EQ(r3.GetPK(), "5");

  // Test target keys.
  EXPECT_EQ(r1.TargetKey().Data().ToString(), "shard0" + SEP + "5" + SEP);
  EXPECT_EQ(r2.TargetKey().Data().ToString(), "shard0" + SEP + "5" + SEP);
  EXPECT_EQ(r3.TargetKey().Data().ToString(), "shard1" + SEP + "5" + SEP);

  /*
  // Test hashing / equality.
  EXPECT_EQ(RocksdbIndexInternalRecord::TargetHash()(r1),
            RocksdbIndexInternalRecord::TargetHash()(r2));
  EXPECT_NE(RocksdbIndexInternalRecord::TargetHash()(r2),
            RocksdbIndexInternalRecord::TargetHash()(r3));

  EXPECT_TRUE(RocksdbIndexInternalRecord::TargetEqual()(r1, r2));
  EXPECT_FALSE(RocksdbIndexInternalRecord::TargetEqual()(r2, r3));
  EXPECT_FALSE(RocksdbIndexInternalRecord::TargetEqual()(r1, r3));
  */
}

// RocksdbPKIndexInternalRecord
TEST(RocksdbEncodeTest, RocksdbPKIndexInternalRecord) {
  // Test piece-wise constructor.
  RocksdbPKIndexInternalRecord r1("5", "shard0");
  RocksdbPKIndexInternalRecord r2("5", "shard0");
  RocksdbPKIndexInternalRecord r3("10", "shard1");

  // Test read constructor.
  r1 = RocksdbPKIndexInternalRecord(r1.Data());
  r2 = RocksdbPKIndexInternalRecord(r2.Data());
  r3 = RocksdbPKIndexInternalRecord(r3.Data());

  // Test data.
  EXPECT_EQ(r1.Data(), "5" + SEP + "shard0" + SEP);
  EXPECT_EQ(r2.Data(), "5" + SEP + "shard0" + SEP);
  EXPECT_EQ(r3.Data(), "10" + SEP + "shard1" + SEP);

  // Test getters.
  EXPECT_EQ(r1.GetShard(), "shard0");
  EXPECT_EQ(r2.GetShard(), "shard0");
  EXPECT_EQ(r3.GetShard(), "shard1");

  /*
  // Test hashing / equality.
  EXPECT_EQ(RocksdbPKIndexInternalRecord::ShardNameHash()(r1),
            RocksdbPKIndexInternalRecord::ShardNameHash()(r2));
  EXPECT_NE(RocksdbPKIndexInternalRecord::ShardNameHash()(r2),
            RocksdbPKIndexInternalRecord::ShardNameHash()(r3));

  EXPECT_TRUE(RocksdbPKIndexInternalRecord::ShardNameEqual()(r1, r2));
  EXPECT_FALSE(RocksdbPKIndexInternalRecord::ShardNameEqual()(r2, r3));
  EXPECT_FALSE(RocksdbPKIndexInternalRecord::ShardNameEqual()(r1, r3));
  */
}

}  // namespace sql
}  // namespace pelton
