#include "pelton/sql/rocksdb/index.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/util/status.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace pelton {
namespace sql {

#define DB_NAME "test"
#define DB_PATH "/tmp/pelton/rocksdb/index_test"

// Drop the database (if it exists), and intialize a fresh rocksdb instance.
std::unique_ptr<rocksdb::DB> InitializeDatabase() {
  std::filesystem::remove_all(DB_PATH);

  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *ptr;
  PANIC(rocksdb::DB::Open(opts, DB_PATH, &ptr));
  return std::unique_ptr<rocksdb::DB>(ptr);
}

bool Eq(const IndexSet &result,
        const std::vector<std::pair<rocksdb::Slice, rocksdb::Slice>> &expect) {
  if (result.size() != expect.size()) {
    return false;
  }
  for (const auto &record : result) {
    std::pair<rocksdb::Slice, rocksdb::Slice> pair(record.ExtractShardName(),
                                                   record.ExtractPK());
    if (std::find(expect.begin(), expect.end(), pair) == expect.end()) {
      return false;
    }
  }
  return true;
}

bool Eq(const PKIndexSet &result, const std::vector<rocksdb::Slice> &expect) {
  if (result.size() != expect.size()) {
    return false;
  }
  for (const auto &record : result) {
    rocksdb::Slice shard = record.ExtractShardName();
    if (std::find(expect.begin(), expect.end(), shard) == expect.end()) {
      return false;
    }
  }
  return true;
}

// Values for tests.
rocksdb::Slice z("0");
rocksdb::Slice o("1");
rocksdb::Slice t("2");
rocksdb::Slice pk1("pk1");
rocksdb::Slice pk2("pk10");
rocksdb::Slice pk3("pk3");
rocksdb::Slice pk4("pk4");
rocksdb::Slice pk5("pk40");
rocksdb::Slice pk6("zz");
rocksdb::Slice shard10("shard10");
rocksdb::Slice shard100("shard100");
rocksdb::Slice shard1("shard1");
rocksdb::Slice shard2("shard2");

// Does get and get_all get all data?
TEST(RocksdbIndexTest, GetAll) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", 0);
  index.Add(z, shard10, pk1);
  index.Add(z, shard100, pk2);
  index.Add(o, shard10, pk3);

  EXPECT_TRUE(
      Eq(index.Get({z, o}), {{shard10, pk1}, {shard100, pk2}, {shard10, pk3}}));
}

// Does get work with non-existent shard names
TEST(RocksdbIndexTest, GetNonExistent) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", 0);
  index.Add(z, shard10, pk1);
  index.Add(z, shard100, pk2);
  index.Add(o, shard10, pk3);

  EXPECT_TRUE(Eq(index.Get({t}), {}));
}

// Some column Values passed to get and get_all are non-existent
TEST(RocksdbIndexTest, GetSomeNonExistent) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", 0);
  index.Add(z, shard10, pk1);
  index.Add(z, shard100, pk2);
  index.Add(o, shard10, pk3);

  EXPECT_TRUE(Eq(index.Get({t, z}), {{shard10, pk1}, {shard100, pk2}}));
}

// Do get and get_all work if no data has been inserted yet?
TEST(RocksdbIndexTest, NoData) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", 0);
  EXPECT_TRUE(Eq(index.Get({z, o, t}), {}));
}

// Does get and all confuse entries where the values are swapped
TEST(RocksdbIndexTest, SimpleGet) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", 0);
  index.Add(t, shard1, pk1);  // value, shard name, key
  index.Add(z, shard1, pk2);
  index.Add(z, shard1, pk3);
  index.Add(o, shard2, pk4);
  index.Add(o, shard10, pk5);
  index.Add(z, shard10, pk6);

  // 1 value at a time.
  EXPECT_TRUE(Eq(index.Get({o}), {{shard2, pk4}, {shard10, pk5}}));
  EXPECT_TRUE(Eq(index.Get({t}), {{shard1, pk1}}));
  EXPECT_TRUE(
      Eq(index.Get({z}), {{shard1, pk2}, {shard1, pk3}, {shard10, pk6}}));

  // 2 values at a time.
  EXPECT_TRUE(
      Eq(index.Get({t, z}),
         {{shard1, pk1}, {shard1, pk2}, {shard1, pk3}, {shard10, pk6}}));

  EXPECT_TRUE(
      Eq(index.Get({o, t}), {{shard2, pk4}, {shard10, pk5}, {shard1, pk1}}));

  EXPECT_TRUE(Eq(index.Get({o, z}), {{shard2, pk4},
                                     {shard10, pk5},
                                     {shard1, pk2},
                                     {shard1, pk3},
                                     {shard10, pk6}}));

  // All values.
  EXPECT_TRUE(Eq(index.Get({t, z, o}), {{shard1, pk1},
                                        {shard1, pk2},
                                        {shard1, pk3},
                                        {shard10, pk6},
                                        {shard2, pk4},
                                        {shard10, pk5}}));
}

// Does get and get_all work correctly right after delete
TEST(RocksdbIndexTest, GetAfterDelete) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", 0);
  index.Add(t, shard1, pk1);
  index.Add(z, shard1, pk2);
  index.Add(z, shard1, pk3);
  index.Add(o, shard2, pk4);
  index.Add(o, shard10, pk5);
  index.Add(z, shard10, pk6);

  // Delete some entries.
  index.Delete(t, shard1, pk1);
  index.Delete(z, shard1, pk3);

  // 1 value at a time.
  EXPECT_TRUE(Eq(index.Get({o}), {{shard2, pk4}, {shard10, pk5}}));
  EXPECT_TRUE(Eq(index.Get({t}), {}));
  EXPECT_TRUE(Eq(index.Get({z}), {{shard1, pk2}, {shard10, pk6}}));

  // 2 values at a time.
  EXPECT_TRUE(Eq(index.Get({t, z}), {{shard1, pk2}, {shard10, pk6}}));

  EXPECT_TRUE(Eq(index.Get({o, t}), {{shard2, pk4}, {shard10, pk5}}));

  EXPECT_TRUE(
      Eq(index.Get({o, z}),
         {{shard2, pk4}, {shard10, pk5}, {shard1, pk2}, {shard10, pk6}}));

  // All values.
  EXPECT_TRUE(
      Eq(index.Get({t, z, o}),
         {{shard1, pk2}, {shard10, pk6}, {shard2, pk4}, {shard10, pk5}}));
}

// For PK indices.
TEST(RocksdbIndexTest, GetAfterDeletePK) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbPKIndex index(db.get(), "");
  index.Add(pk1, shard1);  // Gets deleted
  index.Add(pk1, shard2);
  index.Add(pk1, shard10);
  index.Add(pk2, shard1);
  index.Add(pk3, shard1);  // Gets deleted
  index.Add(pk4, shard2);
  index.Add(pk4, shard100);
  index.Add(pk5, shard10);

  // Delete some entries.
  index.Delete(pk1, shard1);
  index.Delete(pk3, shard1);

  // 1 value at a time.
  EXPECT_TRUE(Eq(index.Get({pk1}), {shard2, shard10}));
  EXPECT_TRUE(Eq(index.Get({pk2}), {shard1}));
  EXPECT_TRUE(Eq(index.Get({pk3}), {}));
  EXPECT_TRUE(Eq(index.Get({pk4}), {shard2, shard100}));
  EXPECT_TRUE(Eq(index.Get({pk5}), {shard10}));
  EXPECT_TRUE(Eq(index.Get({pk6}), {}));

  // 2 values at a time.
  EXPECT_TRUE(Eq(index.Get({pk1, pk2}), {shard2, shard10, shard1}));
  EXPECT_TRUE(Eq(index.Get({pk1, pk3}), {shard2, shard10}));
  EXPECT_TRUE(Eq(index.Get({pk3, pk6}), {}));
  EXPECT_TRUE(Eq(index.Get({pk4, pk5}), {shard2, shard10, shard100}));

  // 3 values.
  EXPECT_TRUE(
      Eq(index.Get({pk1, pk2, pk4}), {shard2, shard10, shard1, shard100}));
}

}  // namespace sql
}  // namespace pelton
