#include <filesystem>
#include <iostream>
#include <string>

#include "pelton/sql/connections/rocksdb_index.h"
#include "pelton/util/status.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"

#define PANIC PANIC_STATUS

#include "glog/logging.h"
// NOLINTNEXTLINE
#include "gtest/gtest.h"

#define DB_NAME "test"
#define DB_PATH "/tmp/pelton/rocksdb/index_test"

namespace pelton {
namespace sql {
// Drop the database (if it exists).
void DropDatabase() { std::filesystem::remove_all(DB_PATH); }

// Does get and get_all get all data?
TEST(RocksdbConnectionTest, GetAll) {
  DropDatabase();
  std::string path = DB_PATH;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);

  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("pk2");
  rocksdb::Slice tr("pk3");
  rocksdb::Slice f("pk4");
  rocksdb::Slice fi("pk5");

  RocksdbIndex test_add(db, 1, 4);
  test_add.Add(z, "shard10", t);  // value, shard name, key
  test_add.Add(z, "shard100", tr);
  test_add.Add(o, "shard10", f);
  test_add.Add(o, "shard100", fi);

  std::vector<rocksdb::Slice> vec = {z, o};
  std::vector<std::string> get_test1 = test_add.Get(vec, "shard10");
  std::vector<std::string> r1 = {"pk2", "pk4"};

  std::vector<std::string> get_test2 = test_add.Get(vec, "shard100");
  std::vector<std::string> r2 = {"pk3", "pk5"};
  std::vector<std::pair<std::string, std::string>> get_all_test =
      test_add.Get_all(vec);
  std::vector<std::pair<std::string, std::string>> r3 = {{"shard10", "pk2"},
                                                         {"shard100", "pk3"},
                                                         {"shard10", "pk4"},
                                                         {"shard100", "pk5"}};
  assert(get_test1 == r1);
  assert(get_test2 == r2);
  assert(get_all_test == r3);
}

// Does get work with non-existent shard names
TEST(RocksdbConnectionTest, GetNonExistentPK) {
  DropDatabase();
  std::string path = DB_PATH;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);
  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("pk2");
  rocksdb::Slice tr("pk3");
  rocksdb::Slice f("pk4");
  rocksdb::Slice fi("pk5");

  RocksdbIndex test_add(db, 0, 6);
  test_add.Add(z, "shard10", t);  // value, shard name, key
  test_add.Add(z, "shard100", tr);
  test_add.Add(o, "shard10", f);
  test_add.Add(o, "shard100", fi);

  std::vector<rocksdb::Slice> vec = {z};
  std::vector<std::string> get_test = test_add.Get(vec, "shard1000");
  std::vector<std::string> r1 = {};
  assert(get_test == r1);
}

// Some column Values passed to get and get_all are non-existent
TEST(RocksdbConnectionTest, NonExistentColumnVals) {
  DropDatabase();
  std::string path = DB_PATH;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);
  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("pk2");
  rocksdb::Slice tr("pk3");
  rocksdb::Slice f("pk4");
  rocksdb::Slice fi("pk5");

  RocksdbIndex test_add(db, 2, 5);
  test_add.Add(z, "shard10", t);  // value, shard name, key
  test_add.Add(z, "shard100", tr);
  test_add.Add(o, "shard10", f);
  test_add.Add(o, "shard100", fi);

  std::vector<rocksdb::Slice> vec1 = {t, tr, fi};
  std::vector<rocksdb::Slice> vec2 = {t, tr, o, fi};
  std::vector<std::string> get_test1 = test_add.Get(vec1, "shard10");
  std::vector<std::string> r1 = {};
  std::vector<std::string> get_test2 = test_add.Get(vec1, "shard100");
  std::vector<std::string> r2 = {};
  std::vector<std::string> get_test3 = test_add.Get(vec2, "shard100");
  std::vector<std::string> r3 = {"pk5"};
  std::vector<std::pair<std::string, std::string>> get_all_test1 =
      test_add.Get_all(vec1);
  std::vector<std::pair<std::string, std::string>> r4 = {};
  std::vector<std::pair<std::string, std::string>> get_all_test2 =
      test_add.Get_all(vec2);
  std::vector<std::pair<std::string, std::string>> r5 = {{"shard10", "pk4"},
                                                         {"shard100", "pk5"}};

  assert(get_test1 == r1);
  assert(get_test2 == r2);
  assert(get_test3 == r3);
  assert(get_all_test1 == r4);
  assert(get_all_test2 == r5);
}

// Do get and get_all work if no data has been inserted yet?
TEST(RocksdbConnectionTest, NoData) {
  DropDatabase();
  std::string path = DB_PATH;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);
  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("pk2");
  rocksdb::Slice tr("pk3");
  rocksdb::Slice f("pk4");
  rocksdb::Slice fi("pk5");

  RocksdbIndex test_add(db, 5, 10);

  std::vector<rocksdb::Slice> vec = {t, tr, fi};
  std::vector<std::string> get_test = test_add.Get(vec, "shard10");
  std::vector<std::string> r1 = {};

  std::vector<std::pair<std::string, std::string>> get_all_test =
      test_add.Get_all(vec);
  std::vector<std::pair<std::string, std::string>> r2 = {};

  assert(get_test == r1);
  assert(get_all_test == r2);
}

// Does get and all confuse entries where the values are swapped
TEST(RocksdbConnectionTest, SwappedValues) {
  DropDatabase();
  std::string path = DB_PATH;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);
  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("2");
  rocksdb::Slice tr("3");

  RocksdbIndex test_add(db, 10, 100);
  test_add.Add(tr, "0", z);  // value, shard name, key
  test_add.Add(z, "1", o);
  test_add.Add(z, "1", t);
  test_add.Add(o, "0", o);
  test_add.Add(o, "0", z);
  test_add.Add(z, "0", "0");

  std::vector<rocksdb::Slice> vec1 = {o};
  std::vector<rocksdb::Slice> vec2 = {t};
  std::vector<rocksdb::Slice> vec3 = {z};
  std::vector<std::string> get_test1 = test_add.Get(vec3, "0");
  std::vector<std::string> r1 = {"0"};
  std::vector<std::string> get_test2 = test_add.Get(vec3, "1");
  std::vector<std::string> r2 = {"1", "2"};
  std::vector<std::pair<std::string, std::string>> get_all_test1 =
      test_add.Get_all(vec1);
  std::vector<std::pair<std::string, std::string>> r3 = {{"0", "0"},
                                                         {"0", "1"}};
  std::vector<std::pair<std::string, std::string>> get_all_test2 =
      test_add.Get_all(vec2);
  std::vector<std::pair<std::string, std::string>> r4 = {};
  assert(get_test1 == r1);
  assert(get_test2 == r2);
  assert(get_all_test1 == r3);
  assert(get_all_test2 == r4);
}

// Does get and get_all work correctly right after delete
TEST(RocksdbConnectionTest, GetNonExistent) {
  DropDatabase();
  std::string path = DB_PATH;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);
  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("2");
  rocksdb::Slice tr("3");

  RocksdbIndex test_add(db, 10, 100);
  test_add.Add(tr, "0", z);  // value, shard name, key
  test_add.Add(z, "1", o);
  test_add.Add(z, "1", t);
  test_add.Add(o, "0", o);
  test_add.Add(o, "0", z);
  test_add.Add(z, "0", "0");

  std::vector<rocksdb::Slice> vec1 = {o};
  std::vector<rocksdb::Slice> vec2 = {t};
  std::vector<rocksdb::Slice> vec3 = {z};
  std::vector<std::string> get_test1 = test_add.Get(vec3, "0");
  std::vector<std::string> r1 = {"0"};
  std::vector<std::string> get_test2 = test_add.Get(vec3, "1");
  std::vector<std::string> r2 = {"1", "2"};
  std::vector<std::pair<std::string, std::string>> get_all_test1 =
      test_add.Get_all(vec1);
  std::vector<std::pair<std::string, std::string>> r3 = {{"0", "0"},
                                                         {"0", "1"}};
  std::vector<std::pair<std::string, std::string>> get_all_test2 =
      test_add.Get_all(vec2);
  std::vector<std::pair<std::string, std::string>> r4 = {};
  assert(get_test1 == r1);
  assert(get_test2 == r2);
  assert(get_all_test1 == r3);
  assert(get_all_test2 == r4);
}
TEST(RocksdbConnectionTest, GetDeleted) {
  DropDatabase();
  std::string path = DB_PATH;

  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);
  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("pk2");
  rocksdb::Slice three("3");
  rocksdb::Slice tr("pk3");
  rocksdb::Slice f("pk4");
  rocksdb::Slice fi("pk5");

  RocksdbIndex test_add(db, 108, 1004);
  test_add.Add(z, "shard10", t);  // value, shard name, key
  test_add.Add(z, "shard100", tr);
  test_add.Add(o, "shard10", f);
  test_add.Add(o, "shard100", fi);
  test_add.Add(three, "shard100", f);

  std::vector<rocksdb::Slice> vec1 = {z, o, three};
  test_add.Delete(fi, "shard10", t);  // deleting non-existent data
  test_add.Delete(z, "shard3", t);
  test_add.Delete(z, "shard10", z);
  std::vector<std::pair<std::string, std::string>> get_all_test1 =
      test_add.Get_all(vec1);
  std::vector<std::pair<std::string, std::string>> r1 = {{"shard10", "pk2"},
                                                         {"shard100", "pk3"},
                                                         {"shard10", "pk4"},
                                                         {"shard100", "pk5"},
                                                         {"shard100", "pk4"}};
  test_add.Delete(z, "shard10", t);
  test_add.Delete(o, "shard100", fi);
  std::vector<std::pair<std::string, std::string>> get_all_test2 =
      test_add.Get_all(vec1);
  std::vector<std::pair<std::string, std::string>> r2 = {
      {"shard100", "pk3"}, {"shard10", "pk4"}, {"shard100", "pk4"}};
  std::vector<std::string> get_test1 = test_add.Get(vec1, "shard10");
  std::vector<std::string> r3 = {"pk4"};
  test_add.Delete(z, "shard100", tr);  // Deleting all data
  test_add.Delete(o, "shard10", f);
  test_add.Delete(three, "shard100", f);
  std::vector<std::pair<std::string, std::string>> get_all_test3 =
      test_add.Get_all(vec1);
  std::vector<std::pair<std::string, std::string>> r4 = {};
  std::vector<std::string> get_test2 = test_add.Get(vec1, "shard10");
  std::vector<std::string> r5 = {};
  test_add.Add(z, "shard10", t);  // Reinserting some data
  std::vector<std::pair<std::string, std::string>> get_all_test4 =
      test_add.Get_all(vec1);
  std::vector<std::pair<std::string, std::string>> r6 = {{"shard10", "pk2"}};
  std::vector<std::string> get_test3 = test_add.Get(vec1, "shard10");
  std::vector<std::string> r7 = {"pk2"};
  assert(get_all_test1 == r1);
  assert(get_all_test2 == r2);
  assert(get_test1 == r3);
  assert(get_all_test3 == r4);
  assert(get_test2 == r5);
  assert(get_all_test4 == r6);
  assert(get_test3 == r7);
}

// testing behaviour on multiple similar entries
TEST(RocksdbConnectionTest, Duplicates) {
  DropDatabase();
  std::string path = DB_PATH;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  std::unique_ptr<rocksdb::DB> dbb = std::unique_ptr<rocksdb::DB>(db);
  rocksdb::Slice z("0");
  rocksdb::Slice o("1");
  rocksdb::Slice t("pk2");
  rocksdb::Slice tr("pk3");
  rocksdb::Slice f("pk4");
  rocksdb::Slice fi("pk5");

  RocksdbIndex test_add(db, 1, 4);
  test_add.Add(z, "shard10", t);  // value, shard name, key
  test_add.Add(z, "shard10", tr);
  test_add.Add(z, "shard10", tr);
  std::vector<rocksdb::Slice> vec = {z, o};
  std::vector<std::string> get_test1 = test_add.Get(vec, "shard10");
  std::vector<std::string> r1 = {"pk2", "pk3"};
  test_add.Delete(z, "shard10", tr);
  std::vector<std::string> get_test2 = test_add.Get(vec, "shard10");
  std::vector<std::string> r2 = {"pk2"};
  test_add.Add(z, "shard10", t);
  test_add.Add(z, "shard10", tr);
  std::vector<std::string> get_test3 = test_add.Get(vec, "shard10");
  std::vector<std::string> r3 = {"pk2", "pk3"};
  assert(get_test1 == r1);
  assert(get_test2 == r2);
  assert(get_test3 == r3);
}

}  // namespace sql
}  // namespace pelton
