#include "pelton/sql/rocksdb/table.h"

#include <algorithm>
// NOLINTNEXTLINE
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/sql/rocksdb/transaction.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/status.h"
#include "rocksdb/utilities/transaction_db.h"

namespace pelton {
namespace sql {
namespace rocks {

#define NUL std::string({__ROCKSNULL})

// Equiv. to "inline T&& COPY(const T& x) { return T(x); }" (w/ copy-elision),
// but cooler.
#define COPY(x) decltype(x)(x)

using CType = sqlast::ColumnDefinition::Type;

dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
    {"ID", "name", "age", "email", "date"},
    {CType::UINT, CType::TEXT, CType::INT, CType::TEXT, CType::DATETIME}, {0});

std::unique_ptr<rocksdb::TransactionDB> InitializeDatabase() {
  std::string path = "/tmp/pelton/rocksdb/table_test";

  // Drop the database (if it exists).
  std::filesystem::remove_all(path);

  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();

  // Transaction options.
  rocksdb::TransactionDBOptions txn_opts;
  txn_opts.transaction_lock_timeout = 10000;

  // Open the database.
  rocksdb::TransactionDB *db;
  PANIC(rocksdb::TransactionDB::Open(opts, txn_opts, path, &db));
  return std::unique_ptr<rocksdb::TransactionDB>(db);
}

RocksdbSequence FromVector(const std::vector<std::string> &vec) {
  RocksdbSequence sequence;
  for (const std::string &val : vec) {
    sequence.AppendEncoded(val);
  }
  return sequence;
}

std::vector<RocksdbSequence> DecryptStream(const EncryptionManager &enc,
                                           RocksdbStream &&s) {
  std::vector<std::pair<EncryptedKey, EncryptedValue>> all(s.begin(), s.end());
  std::vector<RocksdbSequence> rows;
  for (auto &&[enkey, envalue] : all) {
    RocksdbSequence key = enc.DecryptKey(std::move(enkey));
    std::string shard = key.At(0).ToString();
    rows.push_back(enc.DecryptValue(shard, std::move(envalue)));
  }
  return rows;
}

// Put then Get some records.
TEST(TableTest, PutExistsGetDelete) {
  // Create DB.
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  std::unique_ptr<RocksdbTransaction> txn =
      std::make_unique<RocksdbTransaction>(db.get());

  // Create a dummy encryption manager.
  EncryptionManager enc;

  // Create table.
  RocksdbTable tbl(db.get(), "table", schema);

  // Create some records and keys.
  RocksdbSequence k1 = FromVector({"usr0", "10"});
  RocksdbSequence k2 = FromVector({"usr1", "10"});
  RocksdbSequence k3 = FromVector({"usr2", "30"});
  RocksdbSequence k4 = FromVector({"usr0", "20"});
  RocksdbSequence v1 = FromVector({"10", "user0", "-10", "mail", "2012-11-11"});
  RocksdbSequence v2 = FromVector({"10", "user1", "5", "mail", "1992-02-19"});
  RocksdbSequence v3 = FromVector({"30", "user2", NUL, "mail", "2002-01-09"});
  RocksdbSequence v4 = FromVector({"20", "user0", "0", NUL, "2022-11-09"});

  // First update indices.
  tbl.IndexAdd("usr0", v1, txn.get());
  tbl.IndexAdd("usr1", v2, txn.get());
  tbl.IndexAdd("usr2", v3, txn.get());
  tbl.IndexAdd("usr0", v4, txn.get());

  // Commit sometimes but not others (to test RYW).
  txn->Commit();
  txn = std::make_unique<RocksdbTransaction>(db.get());

  // Encrypt.
  EncryptedKey e1 = enc.EncryptKey(COPY(k1));
  EncryptedKey e2 = enc.EncryptKey(COPY(k2));
  EncryptedKey e3 = enc.EncryptKey(COPY(k3));
  EncryptedKey e4 = enc.EncryptKey(COPY(k4));
  EncryptedKey e5 = enc.EncryptKey(FromVector({"usr0", "30"}));
  EncryptedKey e6 = enc.EncryptKey(FromVector({"usr3", "50"}));

  // Put.
  tbl.Put(COPY(e1), enc.EncryptValue("usr0", COPY(v1)), txn.get());
  tbl.Put(COPY(e2), enc.EncryptValue("usr1", COPY(v2)), txn.get());
  tbl.Put(COPY(e3), enc.EncryptValue("usr2", COPY(v3)), txn.get());
  tbl.Put(COPY(e4), enc.EncryptValue("usr0", COPY(v4)), txn.get());

  // Check correctness of exists.
  EXPECT_TRUE(tbl.Exists("10", txn.get()));
  EXPECT_TRUE(tbl.Exists("20", txn.get()));
  EXPECT_TRUE(tbl.Exists("30", txn.get()));
  EXPECT_FALSE(tbl.Exists("40", txn.get()));
  EXPECT_FALSE(tbl.Exists("50", txn.get()));

  // Read.
  RocksdbSequence r1 = enc.DecryptValue("usr0", *tbl.Get(e1, true, txn.get()));
  RocksdbSequence r2 = enc.DecryptValue("usr1", *tbl.Get(e2, true, txn.get()));
  RocksdbSequence r3 = enc.DecryptValue("usr2", *tbl.Get(e3, true, txn.get()));
  RocksdbSequence r4 = enc.DecryptValue("usr0", *tbl.Get(e4, true, txn.get()));

  // Check correctness.
  EXPECT_EQ(v1.Data(), r1.Data());
  EXPECT_EQ(v2.Data(), r2.Data());
  EXPECT_EQ(v3.Data(), r3.Data());
  EXPECT_EQ(v4.Data(), r4.Data());
  EXPECT_FALSE(tbl.Get(e5, true, txn.get()).has_value());
  EXPECT_FALSE(tbl.Get(e6, true, txn.get()).has_value());

  // Check that stream gives us correct data.
  std::vector<RocksdbSequence> rows = DecryptStream(enc, tbl.GetAll(txn.get()));
  EXPECT_NE(std::find(rows.begin(), rows.end(), v1), rows.end());
  EXPECT_NE(std::find(rows.begin(), rows.end(), v2), rows.end());
  EXPECT_NE(std::find(rows.begin(), rows.end(), v3), rows.end());
  EXPECT_NE(std::find(rows.begin(), rows.end(), v4), rows.end());

  // Delete.
  tbl.IndexDelete("usr1", v2, txn.get());
  tbl.IndexDelete("usr2", v3, txn.get());
  tbl.Delete(e2, txn.get());
  tbl.Delete(e3, txn.get());
  tbl.Delete(e6, txn.get());

  // Commit sometimes but not others (to test RYW).
  txn->Commit();
  txn = std::make_unique<RocksdbTransaction>(db.get());

  // Check exists.
  EXPECT_TRUE(tbl.Exists("10", txn.get()));
  EXPECT_TRUE(tbl.Exists("20", txn.get()));
  EXPECT_FALSE(tbl.Exists("30", txn.get()));
  EXPECT_FALSE(tbl.Exists("40", txn.get()));
  EXPECT_FALSE(tbl.Exists("50", txn.get()));

  // Read.
  RocksdbSequence d1 = enc.DecryptValue("usr0", *tbl.Get(e1, true, txn.get()));
  RocksdbSequence d4 = enc.DecryptValue("usr0", *tbl.Get(e4, true, txn.get()));

  // Check correctness.
  EXPECT_EQ(v1.Data(), d1.Data());
  EXPECT_FALSE(tbl.Get(e2, true, txn.get()).has_value());
  EXPECT_FALSE(tbl.Get(e3, true, txn.get()).has_value());
  EXPECT_EQ(v4.Data(), d4.Data());
  EXPECT_FALSE(tbl.Get(e5, true, txn.get()).has_value());
  EXPECT_FALSE(tbl.Get(e6, true, txn.get()).has_value());

  // Check that stream gives us correct data.
  rows = DecryptStream(enc, tbl.GetAll(txn.get()));
  EXPECT_NE(std::find(rows.begin(), rows.end(), v1), rows.end());
  EXPECT_EQ(std::find(rows.begin(), rows.end(), v2), rows.end());
  EXPECT_EQ(std::find(rows.begin(), rows.end(), v3), rows.end());
  EXPECT_NE(std::find(rows.begin(), rows.end(), v4), rows.end());
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
