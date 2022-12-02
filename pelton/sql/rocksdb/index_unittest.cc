#include "pelton/sql/rocksdb/index.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/sql/rocksdb/transaction.h"
#include "pelton/util/ints.h"
#include "pelton/util/status.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace pelton {
namespace sql {
namespace rocks {

#define DB_PATH "/tmp/pelton/rocksdb/index_test"

std::vector<std::string> V(const std::vector<rocksdb::Slice> &v) {
  std::vector<std::string> result;
  result.reserve(v.size());
  for (const auto &s : v) {
    result.push_back(s.ToString());
  }
  return result;
}
std::vector<std::string> V(const std::vector<std::vector<rocksdb::Slice>> &v) {
  std::vector<std::string> result;
  result.reserve(v.size());
  for (const auto &c : v) {
    result.push_back("");
    for (const auto &s : c) {
      result.back().append(s.data(), s.size());
      result.back().push_back(__ROCKSCOMP);
    }
    result.back().pop_back();
  }
  return result;
}

// Drop the database (if it exists), and intialize a fresh rocksdb instance.
std::unique_ptr<rocksdb::TransactionDB> InitializeDatabase() {
  std::filesystem::remove_all(DB_PATH);

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
  PANIC(rocksdb::TransactionDB::Open(opts, txn_opts, DB_PATH, &db));
  return std::unique_ptr<rocksdb::TransactionDB>(db);
}

void EXPECT_REQ(
    const IndexSet &result,
    const std::vector<std::pair<rocksdb::Slice, rocksdb::Slice>> &expect) {
  EXPECT_EQ(result.size(), expect.size());
  for (const auto &record : result) {
    std::pair<rocksdb::Slice, rocksdb::Slice> pair(record.GetShard(),
                                                   record.GetPK());
    EXPECT_NE(std::find(expect.begin(), expect.end(), pair), expect.end());
  }
}

void EXPECT_DEQ(
    const DedupIndexSet &result,
    const std::vector<std::pair<std::vector<rocksdb::Slice>, rocksdb::Slice>>
        &expect) {
  EXPECT_EQ(result.size(), expect.size());
  for (const auto &record : result) {
    auto shard = record.GetShard();
    auto pk = record.GetPK();
    bool found = false;
    for (auto &[v, epk] : expect) {
      if (pk != epk) {
        continue;
      }
      for (auto &eshard : v) {
        if (shard == eshard) {
          found = true;
          break;
        }
      }
    }
    EXPECT_TRUE(found);
  }
}

// Values for tests.
rocksdb::Slice z("0");
rocksdb::Slice o("1");
rocksdb::Slice oz("10");
rocksdb::Slice t("2");
rocksdb::Slice h("3");
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
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "", {0});
  index.Add({z}, shard10, pk1, &txn);
  index.Add({z}, shard100, pk2, &txn);
  index.Add({o}, shard10, pk3, &txn);

  EXPECT_REQ(index.Get(V({z, o}), &txn),
             {{shard10, pk1}, {shard100, pk2}, {shard10, pk3}});
}

// Does get work with non-existent shard names
TEST(RocksdbIndexTest, GetNonExistent) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "", {0});
  index.Add({z}, shard10, pk1, &txn);
  index.Add({z}, shard100, pk2, &txn);
  index.Add({o}, shard10, pk3, &txn);

  EXPECT_REQ(index.Get(V({t}), &txn), {});
}

// Some column Values passed to get and get_all are non-existent
TEST(RocksdbIndexTest, GetSomeNonExistent) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "", {0});
  index.Add({z}, shard10, pk1, &txn);
  index.Add({z}, shard100, pk2, &txn);
  index.Add({o}, shard10, pk3, &txn);

  EXPECT_REQ(index.Get(V({t, z}), &txn), {{shard10, pk1}, {shard100, pk2}});
}

// Do get and get_all work if no data has been inserted yet?
TEST(RocksdbIndexTest, NoData) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "", {0});
  EXPECT_REQ(index.Get(V({z, o, t}), &txn), {});
}

// Does get and all confuse entries where the values are swapped
TEST(RocksdbIndexTest, SimpleGet) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "", {0});
  index.Add({t}, shard1, pk1, &txn);  // value, shard name, key
  index.Add({z}, shard1, pk2, &txn);
  index.Add({z}, shard2, pk2, &txn);
  index.Add({o}, shard2, pk4, &txn);
  index.Add({o}, shard10, pk5, &txn);
  index.Add({z}, shard10, pk6, &txn);
  index.Add({h}, shard2, pk2, &txn);

  // 1 value at a time.
  EXPECT_REQ(index.Get(V({o}), &txn), {{shard2, pk4}, {shard10, pk5}});
  EXPECT_REQ(index.Get(V({t, t}), &txn), {{shard1, pk1}});
  EXPECT_REQ(index.Get(V({z}), &txn),
             {{shard1, pk2}, {shard2, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({h}), &txn), {{shard2, pk2}});

  // 2 values at a time.
  EXPECT_REQ(index.Get(V({h, z}), &txn),
             {{shard1, pk2}, {shard2, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({t, z}), &txn),
             {{shard1, pk1}, {shard1, pk2}, {shard2, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({o, t}), &txn),
             {{shard2, pk4}, {shard10, pk5}, {shard1, pk1}});
  EXPECT_REQ(index.Get(V({o, z}), &txn), {{shard2, pk4},
                                          {shard10, pk5},
                                          {shard1, pk2},
                                          {shard2, pk2},
                                          {shard10, pk6}});

  // All values.
  EXPECT_REQ(index.Get(V({t, z, o, h}), &txn), {{shard1, pk1},
                                                {shard1, pk2},
                                                {shard2, pk2},
                                                {shard10, pk6},
                                                {shard2, pk4},
                                                {shard10, pk5}});
}

// Does get and get_all work correctly right after delete
TEST(RocksdbIndexTest, GetAfterDelete) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "", {0});
  index.Add({t}, shard1, pk1, &txn);
  index.Add({z}, shard1, pk2, &txn);
  index.Add({z}, shard1, pk3, &txn);
  index.Add({o}, shard2, pk4, &txn);
  index.Add({o}, shard10, pk5, &txn);
  index.Add({z}, shard10, pk6, &txn);

  // Delete some entries.
  index.Delete({t}, shard1, pk1, &txn);
  index.Delete({z}, shard1, pk3, &txn);

  // 1 value at a time.
  EXPECT_REQ(index.Get(V({o}), &txn), {{shard2, pk4}, {shard10, pk5}});
  EXPECT_REQ(index.Get(V({t}), &txn), {});
  EXPECT_REQ(index.Get(V({z, z}), &txn), {{shard1, pk2}, {shard10, pk6}});

  // 2 values at a time.
  EXPECT_REQ(index.Get(V({t, z}), &txn), {{shard1, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({o, t}), &txn), {{shard2, pk4}, {shard10, pk5}});
  EXPECT_REQ(index.Get(V({o, z}), &txn),
             {{shard2, pk4}, {shard10, pk5}, {shard1, pk2}, {shard10, pk6}});

  // All values.
  EXPECT_REQ(index.Get(V({t, z, o}), &txn),
             {{shard1, pk2}, {shard10, pk6}, {shard2, pk4}, {shard10, pk5}});
}

// Get deduplicate truly deduplicates
TEST(RocksdbIndexTest, GetDedup) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "", {0});
  index.Add({t}, shard1, pk1, &txn);
  index.Add({z}, shard1, pk2, &txn);
  index.Add({z}, shard2, pk2, &txn);
  index.Add({z}, shard10, pk5, &txn);
  index.Add({o}, shard1, pk2, &txn);
  index.Add({o}, shard2, pk2, &txn);
  index.Add({o}, shard1, pk4, &txn);

  // 1 value at a time.
  EXPECT_DEQ(index.GetDedup(V({t}), &txn), {{{shard1}, pk1}});
  EXPECT_DEQ(index.GetDedup(V({z}), &txn),
             {{{shard1, shard2}, pk2}, {{shard10}, pk5}});
  EXPECT_DEQ(index.GetDedup(V({o}), &txn),
             {{{shard1, shard2}, pk2}, {{shard1}, pk4}});

  // 2 values at a time.
  EXPECT_DEQ(index.GetDedup(V({t, z}), &txn),
             {{{shard1}, pk1}, {{shard1, shard2}, pk2}, {{shard10}, pk5}});
  EXPECT_DEQ(index.GetDedup(V({o, t}), &txn),
             {{{shard1}, pk1}, {{shard1, shard2}, pk2}, {{shard1}, pk4}});
  EXPECT_DEQ(index.GetDedup(V({o, z}), &txn),
             {{{shard1, shard2}, pk2}, {{shard10}, pk5}, {{shard1}, pk4}});

  // All values.
  EXPECT_DEQ(index.GetDedup(V({t, z, o}), &txn), {{{shard1}, pk1},
                                                  {{shard1, shard2}, pk2},
                                                  {{shard10}, pk5},
                                                  {{shard1}, pk4}});
}

TEST(RocksdbIndexTest, CompositeIndex) {
  using CType = sqlast::ColumnDefinition::Type;
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create({"", ""}, {CType::INT, CType::INT}, {});

  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbIndex index(db.get(), "composite", {0, 1});
  index.Add({z, z}, shard10, pk1, &txn);
  index.Add({z, z}, shard100, pk2, &txn);
  index.Add({z, z}, shard1, pk3, &txn);
  index.Add({z, o}, shard100, pk1, &txn);
  index.Add({o, o}, shard10, pk1, &txn);
  index.Add({o, o}, shard10, pk2, &txn);
  index.Add({o, z}, shard10, pk3, &txn);
  index.Add({oz, z}, shard10, pk4, &txn);

  // Get some data with duplication.
  EXPECT_REQ(index.Get(V({{z, z}, {o, z}}), &txn),
             {{shard10, pk1}, {shard100, pk2}, {shard1, pk3}, {shard10, pk3}});

  // Delete some data.
  index.Delete({z, z}, shard1, pk3, &txn);

  // Get non-existing data.
  EXPECT_REQ(index.Get(V({{z, t}, {o, t}, {t, o}}), &txn), {});

  // Get some data with dedup on PK.
  EXPECT_DEQ(index.GetDedup(V({{z, z}, {z, o}, {o, o}}), &txn),
             {{{shard10, shard100}, pk1}, {{shard10, shard100}, pk2}});

  // Total with IN.
  sqlast::ValueMapper vm3(schema);
  vm3.AddValues(0, {sqlast::Value(1_s)});
  vm3.AddValues(1, {sqlast::Value(0_s), sqlast::Value(1_s)});
  EXPECT_REQ(index.Get(index.EncodeComposite(&vm3, schema), &txn),
             {{shard10, pk1}, {shard10, pk2}, {shard10, pk3}});

  sqlast::ValueMapper vm4(schema);
  vm4.AddValues(0, {sqlast::Value(0_s), sqlast::Value(1_s)});
  vm4.AddValues(1, {sqlast::Value(1_s)});
  EXPECT_REQ(index.Get(index.EncodeComposite(&vm4, schema), &txn),
             {{shard100, pk1}, {shard10, pk1}, {shard10, pk2}});

  // Get with shards.
  EXPECT_REQ(index.GetWithShards(V({shard100, shard100, shard100}),
                                 V({{z, z}, {z, o}, {o, z}}), &txn),
             {{shard100, pk2}, {shard100, pk1}});
}

// For PK indices.
TEST(RocksdbPKIndex, GetAfterDelete) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbPKIndex index(db.get(), "");
  index.Add({pk1}, shard1, &txn);  // Gets deleted
  index.Add({pk1}, shard2, &txn);
  index.Add({pk1}, shard10, &txn);
  index.Add({pk2}, shard1, &txn);
  index.Add({pk3}, shard1, &txn);  // Gets deleted
  index.Add({pk4}, shard2, &txn);
  index.Add({pk4}, shard100, &txn);
  index.Add({pk5}, shard10, &txn);

  // Delete some entries.
  index.Delete({pk1}, shard1, &txn);
  index.Delete({pk3}, shard1, &txn);

  // 1 value at a time.
  EXPECT_REQ(index.Get(V({pk1}), &txn), {{shard2, pk1}, {shard10, pk1}});
  EXPECT_REQ(index.Get(V({pk2}), &txn), {{shard1, pk2}});
  EXPECT_REQ(index.Get(V({pk3}), &txn), {});
  EXPECT_REQ(index.Get(V({pk4, pk4}), &txn), {{shard2, pk4}, {shard100, pk4}});
  EXPECT_REQ(index.Get(V({pk5}), &txn), {{shard10, pk5}});
  EXPECT_REQ(index.Get(V({pk6}), &txn), {});

  // 2 values at a time.
  EXPECT_REQ(index.Get(V({pk1, pk2}), &txn),
             {{shard2, pk1}, {shard10, pk1}, {shard1, pk2}});
  EXPECT_REQ(index.Get(V({pk1, pk3}), &txn), {{shard2, pk1}, {shard10, pk1}});
  EXPECT_REQ(index.Get(V({pk3, pk6}), &txn), {});
  EXPECT_REQ(index.Get(V({pk4, pk5}), &txn),
             {{shard2, pk4}, {shard10, pk5}, {shard100, pk4}});

  // 3 values.
  EXPECT_REQ(index.Get(V({pk1, pk2, pk4}), &txn), {{shard2, pk1},
                                                   {shard2, pk4},
                                                   {shard10, pk1},
                                                   {shard1, pk2},
                                                   {shard100, pk4}});
}

TEST(RocksdbPKIndex, GetDedup) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbPKIndex index(db.get(), "");
  index.Add({pk1}, shard1, &txn);
  index.Add({pk1}, shard2, &txn);
  index.Add({pk2}, shard1, &txn);
  index.Add({pk3}, shard10, &txn);
  index.Add({pk3}, shard2, &txn);

  // 1 value at a time.
  EXPECT_DEQ(index.GetDedup(V({pk1}), &txn), {{{shard1, shard2}, pk1}});
  EXPECT_DEQ(index.GetDedup(V({pk2}), &txn), {{{shard1}, pk2}});
  EXPECT_DEQ(index.GetDedup(V({pk3}), &txn), {{{shard10, shard2}, pk3}});

  // 2 values at a time.
  EXPECT_DEQ(index.GetDedup(V({pk1, pk2}), &txn),
             {{{shard1, shard2}, pk1}, {{shard1}, pk2}});
  EXPECT_DEQ(index.GetDedup(V({pk1, pk3}), &txn),
             {{{shard1, shard2}, pk1}, {{shard10, shard2}, pk3}});
  EXPECT_DEQ(index.GetDedup(V({pk2, pk3}), &txn),
             {{{shard1}, pk2}, {{shard10, shard2}, pk3}});

  // 3 values.
  EXPECT_DEQ(
      index.GetDedup(V({pk1, pk2, pk3}), &txn),
      {{{shard1, shard2}, pk1}, {{shard1}, pk2}, {{shard10, shard2}, pk3}});
}

TEST(RocksdbPKIndex, CountShards) {
  std::unique_ptr<rocksdb::TransactionDB> db = InitializeDatabase();
  RocksdbTransaction txn(db.get());

  RocksdbPKIndex index(db.get(), "");
  index.Add({pk1}, shard1, &txn);
  index.Add({pk1}, shard2, &txn);
  index.Add({pk2}, shard1, &txn);
  index.Add({pk3}, shard10, &txn);
  index.Add({pk3}, shard2, &txn);

  EXPECT_EQ(index.CountShards(V({pk1}), &txn), (std::vector<size_t>{2}));
  EXPECT_EQ(index.CountShards(V({pk2}), &txn), (std::vector<size_t>{1}));
  EXPECT_EQ(index.CountShards(V({pk3}), &txn), (std::vector<size_t>{2}));
  EXPECT_EQ(index.CountShards(V({pk1, pk2}), &txn),
            (std::vector<size_t>{2, 1}));
  EXPECT_EQ(index.CountShards(V({pk3, pk2}), &txn),
            (std::vector<size_t>{2, 1}));
  EXPECT_EQ(index.CountShards(V({pk3, pk1}), &txn),
            (std::vector<size_t>{2, 2}));
  EXPECT_EQ(index.CountShards(V({pk1, pk2, pk3, pk6}), &txn),
            (std::vector<size_t>{2, 1, 2, 0}));
  EXPECT_EQ(index.CountShards(V({pk3, pk1, pk6, pk2}), &txn),
            (std::vector<size_t>{2, 2, 0, 1}));
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
