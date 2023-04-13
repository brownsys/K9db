// Transactions with RYW and REPEATABLE READS isolation.
#include "pelton/sql/rocksdb/transaction.h"

#include <unistd.h>

#include <atomic>
// NOLINTNEXTLINE
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
// NOLINTNEXTLINE
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/util/status.h"

namespace pelton {
namespace sql {
namespace rocks {

#define _ std::optional<std::string>()
using V = std::vector<std::optional<std::string>>;
using VV = std::vector<std::string>;

// Helpers.
namespace {

std::unique_ptr<rocksdb::TransactionDB> InitializeDatabase() {
  std::string path = "/tmp/pelton/rocksdb/transaction_test";

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
  txn_opts.write_policy = rocksdb::TxnDBWritePolicy::WRITE_COMMITTED;
  txn_opts.default_write_batch_flush_threshold = 0;

  // Open the database.
  rocksdb::TransactionDB *db;
  PANIC(rocksdb::TransactionDB::Open(opts, txn_opts, path, &db));
  return std::unique_ptr<rocksdb::TransactionDB>(db);
}

std::unique_ptr<rocksdb::ColumnFamilyHandle> InitializeTable(rocksdb::DB *db) {
  // Create rocksdb table.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  PANIC(db->CreateColumnFamily(options, "test_table", &handle));
  return std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

std::optional<std::string> Get(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *cf,
                               rocksdb::Slice key) {
  std::string result;
  auto status = db->Get(rocksdb::ReadOptions(), cf, key, &result);
  if (status.IsNotFound()) {
    return {};
  } else {
    PANIC(status);
    return result;
  }
}

std::vector<std::string> ToVector(rocksdb::Iterator *it) {
  std::vector<std::string> result;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    result.push_back(it->value().ToString());
  }
  return result;
}

std::vector<std::string> GetAll(rocksdb::DB *db,
                                rocksdb::ColumnFamilyHandle *cf) {
  rocksdb::Iterator *ptr = db->NewIterator(rocksdb::ReadOptions(), cf);
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  return ToVector(it.get());
}

}  // namespace

// Put, Get before committing, Commit, Get after comitting...
TEST(TransactionTest, CommitTest) {
  // Create DB.
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  std::unique_ptr<rocksdb::ColumnFamilyHandle> cf = InitializeTable(db.get());

  RocksdbWriteTransaction txn(db.get());
  RocksdbReadSnapshot snapshot(db.get());

  // Put some data.
  txn.Put(cf.get(), "k1", "v1");
  txn.Put(cf.get(), "k2", "v2");
  txn.Put(cf.get(), "k3", "v3");

  // Data not yet inserted.
  std::string data;
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k1"));
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k2"));
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k3"));
  EXPECT_TRUE(!snapshot.Get(cf.get(), "k1"));
  EXPECT_TRUE(!snapshot.Get(cf.get(), "k2"));
  EXPECT_TRUE(!snapshot.Get(cf.get(), "k3"));

  // However, data is readable via transaction (RYW).
  EXPECT_EQ(txn.Get(cf.get(), "k1"), "v1");
  EXPECT_EQ(txn.Get(cf.get(), "k2"), "v2");
  EXPECT_EQ(txn.Get(cf.get(), "k3"), "v3");

  // Same with multiget.
  std::vector<std::string> result;
  EXPECT_EQ(txn.MultiGet(cf.get(), {"k1", "k3", "k4"}), (V{"v1", "v3", _}));

  // Commit.
  txn.Commit();

  // Data now readable.
  EXPECT_EQ(Get(db.get(), cf.get(), "k1"), "v1");
  EXPECT_EQ(Get(db.get(), cf.get(), "k2"), "v2");
  EXPECT_EQ(Get(db.get(), cf.get(), "k3"), "v3");

  // But it is still unreadable with snapshot!
  EXPECT_TRUE(!snapshot.Get(cf.get(), "k1"));
  EXPECT_TRUE(!snapshot.Get(cf.get(), "k2"));
  EXPECT_TRUE(!snapshot.Get(cf.get(), "k3"));
}

// Put, Get before committing, Rollback, Get after rolling back...
TEST(TransactionTest, RollbackTest) {
  // Create DB.
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  std::unique_ptr<rocksdb::ColumnFamilyHandle> cf = InitializeTable(db.get());

  RocksdbWriteTransaction txn(db.get());

  // Put some data.
  txn.Put(cf.get(), "k1", "v1");
  txn.Put(cf.get(), "k2", "v2");
  txn.Put(cf.get(), "k3", "v3");

  // Data not yet inserted.
  std::string data;
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k1"));
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k2"));
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k3"));

  // However, data is readable via transaction (RYW).
  EXPECT_EQ(txn.Get(cf.get(), "k1"), "v1");
  EXPECT_EQ(txn.Get(cf.get(), "k2"), "v2");
  EXPECT_EQ(txn.Get(cf.get(), "k3"), "v3");

  // Same with multiget.
  std::vector<std::string> result;
  EXPECT_EQ(txn.MultiGet(cf.get(), {"k1", "k3", "k4"}), (V{"v1", "v3", _}));

  // Rollback.
  txn.Rollback();
  txn.Commit();  // Committing after a rollback should be a no-op.

  // Data not inserted and no longer in transaction.
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k1"));
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k2"));
  EXPECT_TRUE(!Get(db.get(), cf.get(), "k3"));
  EXPECT_TRUE(!txn.Get(cf.get(), "k1"));
  EXPECT_TRUE(!txn.Get(cf.get(), "k2"));
  EXPECT_TRUE(!txn.Get(cf.get(), "k3"));
}

// Test mixing of preexisting data and data not yet committed.
TEST(TransactionTest, MixedTest) {
  // Create DB.
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  std::unique_ptr<rocksdb::ColumnFamilyHandle> cf = InitializeTable(db.get());

  // Put some data in the database.
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(db->Put(opts, cf.get(), "k3", "v3"));
  PANIC(db->Put(opts, cf.get(), "k0", "v0"));

  // This test needs to be executed by two threads (to avoid locking deadlocks).
  // These threads will communicate via these variables.
  std::atomic<int> to1 = 0;
  std::atomic<int> to2 = 0;
  std::function sync1([&](int barrier) {
    to2 = barrier;
    while (to1 != barrier) {
    }
  });
  std::function sync2([&](int barrier) {
    to1 = barrier;
    while (to2 != barrier) {
    }
  });

  // First thread.
  std::thread thread1([&]() {
    RocksdbWriteTransaction txn(db.get());

    // Barrier 1.
    sync1(1);

    // Put data in the transaction.
    txn.Put(cf.get(), "k2", "v2");
    txn.Put(cf.get(), "k1", "v1");
    txn.Put(cf.get(), "k4", "v4");

    // Barrier 2.
    sync1(2);

    // Commit after sleeping.
    sleep(1);
    txn.Commit();

    // Signal that we committed.
    to2 = 100;
  });

  // Second thread.
  std::thread thread2([&]() {
    RocksdbWriteTransaction txn(db.get());

    // Barrier 1.
    sync2(1);

    // Put different data, with no overlap with the other transaction.
    txn.Put(cf.get(), "k5", "v5");
    txn.Delete(cf.get(), "k0");

    // Barrier 2.
    sync2(2);

    // These have overlap with other transaction, they will block until the
    // other transaction commits.
    txn.Delete(cf.get(), "k4");
    txn.Put(cf.get(), "k2", "v22");
    EXPECT_EQ(to2, 100);  // Other transaction must have committed!

    // Commit.
    txn.Commit();
  });

  // Wait for both threads to finish.
  thread1.join();
  thread2.join();

  // Database should now effects of both transactions.
  EXPECT_EQ(GetAll(db.get(), cf.get()), (VV{"v1", "v22", "v3", "v5"}));
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
