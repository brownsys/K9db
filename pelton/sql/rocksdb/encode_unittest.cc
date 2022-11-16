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
#define COMP std::string({__ROCKSCOMP})

#define SQL(s) sqlast::Value::FromSQLString(s)

using CType = sqlast::ColumnDefinition::Type;

dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
    {"ID", "name", "age", "email", "date"},
    {CType::UINT, CType::TEXT, CType::INT, CType::TEXT, CType::DATETIME}, {0});

// RocksdbSequence.
TEST(RocksdbEncodeTest, RocksdbSequence) {
  RocksdbSequence row;
  row.Append(sqlast::Value(0_u));
  row.Append(sqlast::Value("kinan"));
  row.Append(sqlast::Value(-10_s));
  row.Append(sqlast::Value());  // NULL.
  row.Append(sqlast::Value("2022-09-01 00:00:00"));

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

  // Test release / constructor.
  RocksdbSequence read = RocksdbSequence(row.Release());

  // Test record decoding.
  dataflow::Record record = read.DecodeRecord(schema, true);
  EXPECT_EQ(record.IsPositive(), true);
  EXPECT_EQ(record.GetUInt(0), 0_u);
  EXPECT_EQ(record.GetString(1), "kinan");
  EXPECT_EQ(record.GetInt(2), -10_s);
  EXPECT_EQ(record.IsNull(3), true);
  EXPECT_EQ(record.GetDateTime(4), "2022-09-01 00:00:00");

  // Test Iterator.
  std::vector<std::string> expected = {"0", "kinan", "-10", NUL + "",
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
  sqlast::Insert stmt("table");
  stmt.SetValues({SQL("0"), SQL("'user'"), SQL("-20"), SQL("NULL"),
                  SQL("'2022-09-01 00:00:00'")});

  // Test from insert.
  RocksdbRecord record =
      RocksdbRecord::FromInsert(stmt, schema, util::ShardName("shard", "0"));

  // Test key.
  EXPECT_EQ(record.Key().Data(), "shard__0" + SEP + "0" + SEP);
  EXPECT_EQ(record.GetShard(), "shard__0");
  EXPECT_EQ(record.GetPK(), "0");

  // Test value.
  EXPECT_EQ(record.Value().Data(), "0" + SEP + "user" + SEP + "-20" + SEP +
                                       NUL + SEP + "2022-09-01 00:00:00" + SEP);

  // Test other constructor.
  RocksdbRecord read(record.Key().Release(), record.Value().Release());

  // Test key.
  EXPECT_EQ(read.Key().Data(), "shard__0" + SEP + "0" + SEP);
  EXPECT_EQ(read.GetShard(), "shard__0");
  EXPECT_EQ(read.GetPK(), "0");

  // Test value.
  EXPECT_EQ(read.Value().Data(), "0" + SEP + "user" + SEP + "-20" + SEP + NUL +
                                     SEP + "2022-09-01 00:00:00" + SEP);
}

// RocksdbIndexInternalRecord
#define IV(s) \
  { rocksdb::Slice(s) }
#define IV2(s1, s2) \
  { rocksdb::Slice(s1), rocksdb::Slice(s2) }
TEST(RocksdbEncodeTest, RocksdbIndexInternalRecord) {
  // Test piece-wise constructor.
  RocksdbIndexInternalRecord r1(IV("10"), "shard__0", "5");
  RocksdbIndexInternalRecord r2(IV("20"), "shard__0", "5");
  RocksdbIndexInternalRecord r3(IV2("20", "v"), "shard__1", "5");

  // Test read constructor.
  r1 = RocksdbIndexInternalRecord(r1.Data());
  r2 = RocksdbIndexInternalRecord(r2.Data());
  r3 = RocksdbIndexInternalRecord(r3.Data());

  // Test data.
  EXPECT_EQ(r1.Data(), "10" + SEP + "shard__0" + SEP + "5" + SEP);
  EXPECT_EQ(r2.Data(), "20" + SEP + "shard__0" + SEP + "5" + SEP);
  EXPECT_EQ(r3.Data(), "20" + COMP + "v" + SEP + "shard__1" + SEP + "5" + SEP);

  // Test getters.
  EXPECT_EQ(r1.GetShard(), "shard__0");
  EXPECT_EQ(r2.GetShard(), "shard__0");
  EXPECT_EQ(r3.GetShard(), "shard__1");
  EXPECT_EQ(r1.GetPK(), "5");
  EXPECT_EQ(r2.GetPK(), "5");
  EXPECT_EQ(r3.GetPK(), "5");

  // Test target keys.
  EXPECT_EQ(r1.TargetKey().Data().ToString(), "shard__0" + SEP + "5" + SEP);
  EXPECT_EQ(r2.TargetKey().Data().ToString(), "shard__0" + SEP + "5" + SEP);
  EXPECT_EQ(r3.TargetKey().Data().ToString(), "shard__1" + SEP + "5" + SEP);
}

// RocksdbPKIndexInternalRecord
TEST(RocksdbEncodeTest, RocksdbPKIndexInternalRecord) {
  // Test piece-wise constructor.
  RocksdbPKIndexInternalRecord r1("5", "shard__0");
  RocksdbPKIndexInternalRecord r2("5", "shard__0");
  RocksdbPKIndexInternalRecord r3("10", "shard__1");

  // Test read constructor.
  r1 = RocksdbPKIndexInternalRecord(r1.Data());
  r2 = RocksdbPKIndexInternalRecord(r2.Data());
  r3 = RocksdbPKIndexInternalRecord(r3.Data());

  // Test data.
  EXPECT_EQ(r1.Data(), "5" + SEP + "shard__0" + SEP);
  EXPECT_EQ(r2.Data(), "5" + SEP + "shard__0" + SEP);
  EXPECT_EQ(r3.Data(), "10" + SEP + "shard__1" + SEP);

  // Test getters.
  EXPECT_EQ(r1.GetShard(), "shard__0");
  EXPECT_EQ(r2.GetShard(), "shard__0");
  EXPECT_EQ(r3.GetShard(), "shard__1");
}

}  // namespace sql
}  // namespace pelton
