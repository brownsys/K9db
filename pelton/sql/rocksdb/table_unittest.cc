#include "pelton/sql/rocksdb/table.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/status.h"
#include "rocksdb/db.h"

namespace pelton {
namespace sql {

#define NUL std::string({__ROCKSNULL})

// Equiv. to "inline T&& COPY(const T& x) { return T(x); }" (w/ copy-elision),
// but cooler.
#define COPY(x) decltype(x)(x)

using CType = sqlast::ColumnDefinition::Type;

dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
    {"ID", "name", "age", "email", "date"},
    {CType::UINT, CType::TEXT, CType::INT, CType::TEXT, CType::DATETIME}, {0});

std::unique_ptr<rocksdb::DB> InitializeDatabase() {
  std::string path = "/tmp/pelton/rocksdb/table_test";

  // Drop the database (if it exists).
  std::filesystem::remove_all(path);

  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  return std::unique_ptr<rocksdb::DB>(db);
}

RocksdbSequence FromVector(const std::vector<std::string> &vec) {
  RocksdbSequence sequence;
  for (const std::string &val : vec) {
    sequence.AppendEncoded(val);
  }
  return sequence;
}

// Put then Get some records.
TEST(TableTest, PutExistsGetDelete) {
  // Create DB.
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  // Create a dummy encryption manager.
  EncryptionManager enc;

  // Create table.
  RocksdbTable tbl(db.get(), "table", schema);

  // Create some records and keys.
  RocksdbSequence k1 = FromVector({"shard0", "10"});
  RocksdbSequence k2 = FromVector({"shard1", "10"});
  RocksdbSequence k3 = FromVector({"shard2", "30"});
  RocksdbSequence k4 = FromVector({"shard0", "20"});
  RocksdbSequence v1 = FromVector({"10", "user0", "-10", "mail", "2012-11-11"});
  RocksdbSequence v2 = FromVector({"10", "user1", "5", "mail", "1992-02-19"});
  RocksdbSequence v3 = FromVector({"30", "user2", NUL, "mail", "2002-01-09"});
  RocksdbSequence v4 = FromVector({"20", "user0", "0", NUL, "2022-11-09"});

  // First update indices.
  tbl.IndexAdd("shard0", v1);
  tbl.IndexAdd("shard1", v2);
  tbl.IndexAdd("shard2", v3);
  tbl.IndexAdd("shard0", v4);

  // Encrypt.
  EncryptedKey e1 = enc.EncryptKey(COPY(k1));
  EncryptedKey e2 = enc.EncryptKey(COPY(k2));
  EncryptedKey e3 = enc.EncryptKey(COPY(k3));
  EncryptedKey e4 = enc.EncryptKey(COPY(k4));
  EncryptedKey e5 = enc.EncryptKey(FromVector({"shard0", "30"}));
  EncryptedKey e6 = enc.EncryptKey(FromVector({"shard3", "50"}));

  // Put.
  tbl.Put(COPY(e1), enc.EncryptValue("shard0", COPY(v1)));
  tbl.Put(COPY(e2), enc.EncryptValue("shard1", COPY(v2)));
  tbl.Put(COPY(e3), enc.EncryptValue("shard2", COPY(v3)));
  tbl.Put(COPY(e4), enc.EncryptValue("shard0", COPY(v4)));

  // Check correctness of exists.
  EXPECT_TRUE(tbl.Exists("10"));
  EXPECT_TRUE(tbl.Exists("20"));
  EXPECT_TRUE(tbl.Exists("30"));
  EXPECT_FALSE(tbl.Exists("40"));
  EXPECT_FALSE(tbl.Exists("50"));

  // Read.
  RocksdbSequence r1 = enc.DecryptValue("shard0", *tbl.Get(e1));
  RocksdbSequence r2 = enc.DecryptValue("shard1", *tbl.Get(e2));
  RocksdbSequence r3 = enc.DecryptValue("shard2", *tbl.Get(e3));
  RocksdbSequence r4 = enc.DecryptValue("shard0", *tbl.Get(e4));

  // Check correctness.
  EXPECT_EQ(v1.Data(), r1.Data());
  EXPECT_EQ(v2.Data(), r2.Data());
  EXPECT_EQ(v3.Data(), r3.Data());
  EXPECT_EQ(v4.Data(), r4.Data());
  EXPECT_FALSE(tbl.Get(e5).has_value());
  EXPECT_FALSE(tbl.Get(e6).has_value());

  // Delete.
  tbl.IndexDelete("shard1", v2);
  tbl.IndexDelete("shard2", v3);
  tbl.Delete(e2);
  tbl.Delete(e3);
  tbl.Delete(e6);

  // Check exists.
  EXPECT_TRUE(tbl.Exists("10"));
  EXPECT_TRUE(tbl.Exists("20"));
  EXPECT_FALSE(tbl.Exists("30"));
  EXPECT_FALSE(tbl.Exists("40"));
  EXPECT_FALSE(tbl.Exists("50"));

  // Read.
  RocksdbSequence d1 = enc.DecryptValue("shard0", *tbl.Get(e1));
  RocksdbSequence d4 = enc.DecryptValue("shard0", *tbl.Get(e4));

  // Check correctness.
  EXPECT_EQ(v1.Data(), d1.Data());
  EXPECT_FALSE(tbl.Get(e2).has_value());
  EXPECT_FALSE(tbl.Get(e3).has_value());
  EXPECT_EQ(v4.Data(), d4.Data());
  EXPECT_FALSE(tbl.Get(e5).has_value());
  EXPECT_FALSE(tbl.Get(e6).has_value());
}

}  // namespace sql
}  // namespace pelton
