#include "pelton/sql/rocksdb/index.h"

#include <filesystem>
#include <iostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/util/ints.h"
#include "pelton/util/status.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

namespace pelton {
namespace sql {

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
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", {0});
  index.Add({z}, shard10, pk1);
  index.Add({z}, shard100, pk2);
  index.Add({o}, shard10, pk3);

  EXPECT_REQ(index.Get(V({z, o})),
             {{shard10, pk1}, {shard100, pk2}, {shard10, pk3}});
}

// Does get work with non-existent shard names
TEST(RocksdbIndexTest, GetNonExistent) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", {0});
  index.Add({z}, shard10, pk1);
  index.Add({z}, shard100, pk2);
  index.Add({o}, shard10, pk3);

  EXPECT_REQ(index.Get(V({t})), {});
}

// Some column Values passed to get and get_all are non-existent
TEST(RocksdbIndexTest, GetSomeNonExistent) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", {0});
  index.Add({z}, shard10, pk1);
  index.Add({z}, shard100, pk2);
  index.Add({o}, shard10, pk3);

  EXPECT_REQ(index.Get(V({t, z})), {{shard10, pk1}, {shard100, pk2}});
}

// Do get and get_all work if no data has been inserted yet?
TEST(RocksdbIndexTest, NoData) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", {0});
  EXPECT_REQ(index.Get(V({z, o, t})), {});
}

// Does get and all confuse entries where the values are swapped
TEST(RocksdbIndexTest, SimpleGet) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", {0});
  index.Add({t}, shard1, pk1);  // value, shard name, key
  index.Add({z}, shard1, pk2);
  index.Add({z}, shard2, pk2);
  index.Add({o}, shard2, pk4);
  index.Add({o}, shard10, pk5);
  index.Add({z}, shard10, pk6);
  index.Add({h}, shard2, pk2);

  // 1 value at a time.
  EXPECT_REQ(index.Get(V({o})), {{shard2, pk4}, {shard10, pk5}});
  EXPECT_REQ(index.Get(V({t, t})), {{shard1, pk1}});
  EXPECT_REQ(index.Get(V({z})), {{shard1, pk2}, {shard2, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({h})), {{shard2, pk2}});

  // 2 values at a time.
  EXPECT_REQ(index.Get(V({h, z})),
             {{shard1, pk2}, {shard2, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({t, z})),
             {{shard1, pk1}, {shard1, pk2}, {shard2, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({o, t})),
             {{shard2, pk4}, {shard10, pk5}, {shard1, pk1}});
  EXPECT_REQ(index.Get(V({o, z})), {{shard2, pk4},
                                    {shard10, pk5},
                                    {shard1, pk2},
                                    {shard2, pk2},
                                    {shard10, pk6}});

  // All values.
  EXPECT_REQ(index.Get(V({t, z, o, h})), {{shard1, pk1},
                                          {shard1, pk2},
                                          {shard2, pk2},
                                          {shard10, pk6},
                                          {shard2, pk4},
                                          {shard10, pk5}});
}

// Does get and get_all work correctly right after delete
TEST(RocksdbIndexTest, GetAfterDelete) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", {0});
  index.Add({t}, shard1, pk1);
  index.Add({z}, shard1, pk2);
  index.Add({z}, shard1, pk3);
  index.Add({o}, shard2, pk4);
  index.Add({o}, shard10, pk5);
  index.Add({z}, shard10, pk6);

  // Delete some entries.
  index.Delete({t}, shard1, pk1);
  index.Delete({z}, shard1, pk3);

  // 1 value at a time.
  EXPECT_REQ(index.Get(V({o})), {{shard2, pk4}, {shard10, pk5}});
  EXPECT_REQ(index.Get(V({t})), {});
  EXPECT_REQ(index.Get(V({z, z})), {{shard1, pk2}, {shard10, pk6}});

  // 2 values at a time.
  EXPECT_REQ(index.Get(V({t, z})), {{shard1, pk2}, {shard10, pk6}});
  EXPECT_REQ(index.Get(V({o, t})), {{shard2, pk4}, {shard10, pk5}});
  EXPECT_REQ(index.Get(V({o, z})),
             {{shard2, pk4}, {shard10, pk5}, {shard1, pk2}, {shard10, pk6}});

  // All values.
  EXPECT_REQ(index.Get(V({t, z, o})),
             {{shard1, pk2}, {shard10, pk6}, {shard2, pk4}, {shard10, pk5}});
}

// Get deduplicate truly deduplicates
TEST(RocksdbIndexTest, GetDedup) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "", {0});
  index.Add({t}, shard1, pk1);
  index.Add({z}, shard1, pk2);
  index.Add({z}, shard2, pk2);
  index.Add({z}, shard10, pk5);
  index.Add({o}, shard1, pk2);
  index.Add({o}, shard2, pk2);
  index.Add({o}, shard1, pk4);

  // 1 value at a time.
  EXPECT_DEQ(index.GetDedup(V({t})), {{{shard1}, pk1}});
  EXPECT_DEQ(index.GetDedup(V({z})),
             {{{shard1, shard2}, pk2}, {{shard10}, pk5}});
  EXPECT_DEQ(index.GetDedup(V({o})),
             {{{shard1, shard2}, pk2}, {{shard1}, pk4}});

  // 2 values at a time.
  EXPECT_DEQ(index.GetDedup(V({t, z})),
             {{{shard1}, pk1}, {{shard1, shard2}, pk2}, {{shard10}, pk5}});
  EXPECT_DEQ(index.GetDedup(V({o, t})),
             {{{shard1}, pk1}, {{shard1, shard2}, pk2}, {{shard1}, pk4}});
  EXPECT_DEQ(index.GetDedup(V({o, z})),
             {{{shard1, shard2}, pk2}, {{shard10}, pk5}, {{shard1}, pk4}});

  // All values.
  EXPECT_DEQ(index.GetDedup(V({t, z, o})), {{{shard1}, pk1},
                                            {{shard1, shard2}, pk2},
                                            {{shard10}, pk5},
                                            {{shard1}, pk4}});
}

TEST(RocksdbIndexTest, CompositeIndex) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbIndex index(db.get(), "composite", {0, 1});
  index.Add({z, z}, shard10, pk1);
  index.Add({z, z}, shard100, pk2);
  index.Add({z, z}, shard1, pk3);
  index.Add({z, o}, shard100, pk1);
  index.Add({o, o}, shard10, pk1);
  index.Add({o, o}, shard10, pk2);
  index.Add({o, z}, shard10, pk3);
  index.Add({oz, z}, shard10, pk4);

  // Get some data with duplication.
  EXPECT_REQ(index.Get(V({{z, z}, {o, z}})),
             {{shard10, pk1}, {shard100, pk2}, {shard1, pk3}, {shard10, pk3}});

  // Delete some data.
  index.Delete({z, z}, shard1, pk3);

  // Get non-existing data.
  EXPECT_REQ(index.Get(V({{z, t}, {o, t}, {t, o}})), {});

  // Get some data with dedup on PK.
  EXPECT_DEQ(index.GetDedup(V({{z, z}, {z, o}, {o, o}})),
             {{{shard10, shard100}, pk1}, {{shard10, shard100}, pk2}});

  // Get some data with partial composite value.
  using CType = sqlast::ColumnDefinition::Type;
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create({"", ""}, {CType::INT, CType::INT}, {});

  // Singular partial value.
  sqlast::ValueMapper vm1(schema);
  vm1.AddValues(0, {sqlast::Value(1_s)});
  EXPECT_REQ(index.Get(index.EncodeComposite(&vm1, schema), true),
             {{shard10, pk1}, {shard10, pk2}, {shard10, pk3}});

  sqlast::ValueMapper vm2(schema);
  vm2.AddValues(0, {sqlast::Value(0_s)});
  EXPECT_DEQ(index.GetDedup(index.EncodeComposite(&vm2, schema), true),
             {{{shard10, shard100}, pk1}, {{shard100}, pk2}});

  // Total with IN.
  sqlast::ValueMapper vm3(schema);
  vm3.AddValues(0, {sqlast::Value(1_s)});
  vm3.AddValues(1, {sqlast::Value(0_s), sqlast::Value(1_s)});
  EXPECT_REQ(index.Get(index.EncodeComposite(&vm3, schema)),
             {{shard10, pk1}, {shard10, pk2}, {shard10, pk3}});

  sqlast::ValueMapper vm4(schema);
  vm4.AddValues(0, {sqlast::Value(0_s), sqlast::Value(1_s)});
  vm4.AddValues(1, {sqlast::Value(1_s)});
  EXPECT_REQ(index.Get(index.EncodeComposite(&vm4, schema)),
             {{shard100, pk1}, {shard10, pk1}, {shard10, pk2}});

  // Get with shards.
  EXPECT_REQ(index.GetWithShards(V({shard100, shard100, shard100}),
                                 V({{z, z}, {z, o}, {o, z}})),
             {{shard100, pk2}, {shard100, pk1}});
}

// For PK indices.
TEST(RocksdbPKIndex, GetAfterDelete) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbPKIndex index(db.get(), "");
  index.Add({pk1}, shard1);  // Gets deleted
  index.Add({pk1}, shard2);
  index.Add({pk1}, shard10);
  index.Add({pk2}, shard1);
  index.Add({pk3}, shard1);  // Gets deleted
  index.Add({pk4}, shard2);
  index.Add({pk4}, shard100);
  index.Add({pk5}, shard10);

  // Delete some entries.
  index.Delete({pk1}, shard1);
  index.Delete({pk3}, shard1);

  // 1 value at a time.
  EXPECT_REQ(index.Get(V({pk1})), {{shard2, pk1}, {shard10, pk1}});
  EXPECT_REQ(index.Get(V({pk2})), {{shard1, pk2}});
  EXPECT_REQ(index.Get(V({pk3})), {});
  EXPECT_REQ(index.Get(V({pk4, pk4})), {{shard2, pk4}, {shard100, pk4}});
  EXPECT_REQ(index.Get(V({pk5})), {{shard10, pk5}});
  EXPECT_REQ(index.Get(V({pk6})), {});

  // 2 values at a time.
  EXPECT_REQ(index.Get(V({pk1, pk2})),
             {{shard2, pk1}, {shard10, pk1}, {shard1, pk2}});
  EXPECT_REQ(index.Get(V({pk1, pk3})), {{shard2, pk1}, {shard10, pk1}});
  EXPECT_REQ(index.Get(V({pk3, pk6})), {});
  EXPECT_REQ(index.Get(V({pk4, pk5})),
             {{shard2, pk4}, {shard10, pk5}, {shard100, pk4}});

  // 3 values.
  EXPECT_REQ(index.Get(V({pk1, pk2, pk4})), {{shard2, pk1},
                                             {shard2, pk4},
                                             {shard10, pk1},
                                             {shard1, pk2},
                                             {shard100, pk4}});
}

TEST(RocksdbPKIndex, GetDedup) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbPKIndex index(db.get(), "");
  index.Add({pk1}, shard1);
  index.Add({pk1}, shard2);
  index.Add({pk2}, shard1);
  index.Add({pk3}, shard10);
  index.Add({pk3}, shard2);

  // 1 value at a time.
  EXPECT_DEQ(index.GetDedup(V({pk1})), {{{shard1, shard2}, pk1}});
  EXPECT_DEQ(index.GetDedup(V({pk2})), {{{shard1}, pk2}});
  EXPECT_DEQ(index.GetDedup(V({pk3})), {{{shard10, shard2}, pk3}});

  // 2 values at a time.
  EXPECT_DEQ(index.GetDedup(V({pk1, pk2})),
             {{{shard1, shard2}, pk1}, {{shard1}, pk2}});
  EXPECT_DEQ(index.GetDedup(V({pk1, pk3})),
             {{{shard1, shard2}, pk1}, {{shard10, shard2}, pk3}});
  EXPECT_DEQ(index.GetDedup(V({pk2, pk3})),
             {{{shard1}, pk2}, {{shard10, shard2}, pk3}});

  // 3 values.
  EXPECT_DEQ(
      index.GetDedup(V({pk1, pk2, pk3})),
      {{{shard1, shard2}, pk1}, {{shard1}, pk2}, {{shard10, shard2}, pk3}});
}

TEST(RocksdbPKIndex, CountShards) {
  std::unique_ptr<rocksdb::DB> db = InitializeDatabase();

  RocksdbPKIndex index(db.get(), "");
  index.Add({pk1}, shard1);
  index.Add({pk1}, shard2);
  index.Add({pk2}, shard1);
  index.Add({pk3}, shard10);
  index.Add({pk3}, shard2);

  EXPECT_EQ(index.CountShards(V({pk1})), (std::vector<size_t>{2}));
  EXPECT_EQ(index.CountShards(V({pk2})), (std::vector<size_t>{1}));
  EXPECT_EQ(index.CountShards(V({pk3})), (std::vector<size_t>{2}));
  EXPECT_EQ(index.CountShards(V({pk1, pk2})), (std::vector<size_t>{2, 1}));
  EXPECT_EQ(index.CountShards(V({pk3, pk2})), (std::vector<size_t>{2, 1}));
  EXPECT_EQ(index.CountShards(V({pk3, pk1})), (std::vector<size_t>{2, 2}));
  EXPECT_EQ(index.CountShards(V({pk1, pk2, pk3, pk6})),
            (std::vector<size_t>{2, 1, 2, 0}));
  EXPECT_EQ(index.CountShards(V({pk3, pk1, pk6, pk2})),
            (std::vector<size_t>{2, 2, 0, 1}));
}

}  // namespace sql
}  // namespace pelton
