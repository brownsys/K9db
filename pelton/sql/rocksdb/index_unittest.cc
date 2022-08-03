#include "pelton/sql/rocksdb/index.h"

#include <filesystem>
#include <iostream>
#include <string>

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
  PANIC_STATUS(rocksdb::DB::Open(opts, DB_PATH, &ptr));
  return std::unique_ptr<rocksdb::DB>(ptr);
}

bool EqualAsSets(
    const std::vector<std::pair<std::string, std::string>> &vec1,
    const std::vector<std::pair<std::string, rocksdb::Slice>> &vec2) {
  if (vec1.size() != vec2.size()) {
    return false;
  }
  for (const auto &[shard, pk] : vec2) {
    std::pair<std::string, std::string> pair{shard, pk.ToString()};
    if (std::find(vec1.begin(), vec1.end(), pair) == vec1.end()) {
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

// Does get and get_all get all data?
TEST(RocksdbConnectionTest, GetAll) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), 0, 0);
  index.Add(z, "shard10", pk1);
  index.Add(z, "shard100", pk2);
  index.Add(o, "shard10", pk3);

  EXPECT_TRUE(
      EqualAsSets(index.Get({z, o}),
                  {{"shard10", pk1}, {"shard100", pk2}, {"shard10", pk3}}));
}

// Does get work with non-existent shard names
TEST(RocksdbConnectionTest, GetNonExistent) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), 0, 0);
  index.Add(z, "shard10", pk1);
  index.Add(z, "shard100", pk2);
  index.Add(o, "shard10", pk3);

  EXPECT_TRUE(EqualAsSets(index.Get({t}), {}));
}

// Some column Values passed to get and get_all are non-existent
TEST(RocksdbConnectionTest, GetSomeNonExistent) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), 0, 0);
  index.Add(z, "shard10", pk1);
  index.Add(z, "shard100", pk2);
  index.Add(o, "shard10", pk3);

  EXPECT_TRUE(
      EqualAsSets(index.Get({t, z}), {{"shard10", pk1}, {"shard100", pk2}}));
}

// Do get and get_all work if no data has been inserted yet?
TEST(RocksdbConnectionTest, NoData) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), 0, 0);
  EXPECT_TRUE(EqualAsSets(index.Get({z, o, t}), {}));
}

// Does get and all confuse entries where the values are swapped
TEST(RocksdbConnectionTest, SimpleGet) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), 0, 0);
  index.Add(t, "shard1", pk1);  // value, shard name, key
  index.Add(z, "shard1", pk2);
  index.Add(z, "shard1", pk3);
  index.Add(o, "shard2", pk4);
  index.Add(o, "shard10", pk5);
  index.Add(z, "shard10", pk6);

  // 1 value at a time.
  EXPECT_TRUE(EqualAsSets(index.Get({o}), {{"shard2", pk4}, {"shard10", pk5}}));
  EXPECT_TRUE(EqualAsSets(index.Get({t}), {{"shard1", pk1}}));
  EXPECT_TRUE(EqualAsSets(
      index.Get({z}), {{"shard1", pk2}, {"shard1", pk3}, {"shard10", pk6}}));

  // 2 values at a time.
  EXPECT_TRUE(EqualAsSets(
      index.Get({t, z}),
      {{"shard1", pk1}, {"shard1", pk2}, {"shard1", pk3}, {"shard10", pk6}}));

  EXPECT_TRUE(EqualAsSets(
      index.Get({o, t}), {{"shard2", pk4}, {"shard10", pk5}, {"shard1", pk1}}));

  EXPECT_TRUE(EqualAsSets(index.Get({o, z}), {{"shard2", pk4},
                                              {"shard10", pk5},
                                              {"shard1", pk2},
                                              {"shard1", pk3},
                                              {"shard10", pk6}}));

  // All values.
  EXPECT_TRUE(EqualAsSets(index.Get({t, z, o}), {{"shard1", pk1},
                                                 {"shard1", pk2},
                                                 {"shard1", pk3},
                                                 {"shard10", pk6},
                                                 {"shard2", pk4},
                                                 {"shard10", pk5}}));
}

// Does get and get_all work correctly right after delete
TEST(RocksdbConnectionTest, GetAfterDelete) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), 0, 0);
  index.Add(t, "shard1", pk1);
  index.Add(z, "shard1", pk2);
  index.Add(z, "shard1", pk3);
  index.Add(o, "shard2", pk4);
  index.Add(o, "shard10", pk5);
  index.Add(z, "shard10", pk6);

  // Delete some entries.
  index.Delete(t, "shard1", pk1);
  index.Delete(z, "shard1", pk3);

  // 1 value at a time.
  EXPECT_TRUE(EqualAsSets(index.Get({o}), {{"shard2", pk4}, {"shard10", pk5}}));
  EXPECT_TRUE(EqualAsSets(index.Get({t}), {}));
  EXPECT_TRUE(EqualAsSets(index.Get({z}), {{"shard1", pk2}, {"shard10", pk6}}));

  // 2 values at a time.
  EXPECT_TRUE(
      EqualAsSets(index.Get({t, z}), {{"shard1", pk2}, {"shard10", pk6}}));

  EXPECT_TRUE(
      EqualAsSets(index.Get({o, t}), {{"shard2", pk4}, {"shard10", pk5}}));

  EXPECT_TRUE(EqualAsSets(
      index.Get({o, z}),
      {{"shard2", pk4}, {"shard10", pk5}, {"shard1", pk2}, {"shard10", pk6}}));

  // All values.
  EXPECT_TRUE(EqualAsSets(
      index.Get({t, z, o}),
      {{"shard1", pk2}, {"shard10", pk6}, {"shard2", pk4}, {"shard10", pk5}}));
}

}  // namespace sql
}  // namespace pelton
