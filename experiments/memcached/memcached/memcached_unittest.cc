#include "memcached/memcached.h"

#include <iostream>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "gflags/gflags.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "mariadb/conncpp.hpp"

#define DB_NAME "memcached"

#define I SetInt
#define S SetStr

// Command line flags.
DEFINE_string(db_username, "pelton", "MariaDB username to connect with");
DEFINE_string(db_password, "password", "MariaDB pwd to connect with");

// Schema.
static const std::vector<std::string> SCHEMA = {
    "CREATE TABLE students (id INT PRIMARY KEY, name VARCHAR(50), age INT)",
    "CREATE TABLE grades (sid INT, grade INT)"};
static const std::vector<std::string> INSERTS = {
    "INSERT INTO students VALUES(1, 'John', 25)",
    "INSERT INTO students VALUES(2, 'Smith', 35)",
    "INSERT INTO students VALUES(3, 'Kinan', 27)",
    "INSERT INTO students VALUES(4, 'Zidane', -45)",
    "INSERT INTO grades VALUES(1, 50)",
    "INSERT INTO grades VALUES(2, 60)",
    "INSERT INTO grades VALUES(2, 70)",
    "INSERT INTO grades VALUES(3, 100)",
    "INSERT INTO grades VALUES(3, 90)",
    "INSERT INTO grades VALUES(3, 96)",
    "INSERT INTO grades VALUES(4, 1000)",
    "INSERT INTO grades VALUES(4, 2000)",
    "INSERT INTO grades VALUES(4, 5000)"};
static const std::vector<std::string> UPDATES1 = {
    "INSERT INTO students VALUES (5, 'New', 50)",
    "UPDATE students SET age = 28 WHERE name = 'kinan'"};
static const std::vector<std::string> UPDATES2 = {
    "DELETE FROM grades WHERE sid = 4 AND grade = 5000",
    "INSERT INTO grades VALUES (5, 40)",
    // This should not appear, we did not ask for "students" to be updated.
    "UPDATE students SET id = id + 10", "UPDATE grades SET sid = sid + 10"};

// Queries to cache.
static const std::vector<std::string> QUERIES = {
    "SELECT * FROM students;", "SELECT * FROM students WHERE id = ?",
    "SELECT * FROM students WHERE name = ?",
    "SELECT name, age, SUM(grades.grade) FROM students JOIN grades ON "
    "students.id = grades.sid GROUP BY name, age HAVING name = ? AND age = ?"};

namespace {

std::unordered_set<std::string> ReadAsString(
    int id, const std::vector<Value> &values = {}) {
  Value *ptr = new Value[values.size()];
  for (size_t i = 0; i < values.size(); i++) {
    ptr[i] = values.at(i);
  }
  Record record = {values.size(), ptr};
  Result res = Read(id, &record);
  DestroyRecord(&record);
  std::unordered_set<std::string> results;
  for (size_t i = 0; i < res.size; i++) {
    std::string str = "|";
    Record *row = res.records + i;
    for (size_t j = 0; j < row->size; j++) {
      Value *v = row->values + j;
      switch (v->type) {
        case TEXT:
          str += GetStr(v);
          break;
        case INT:
          str += std::to_string(GetInt(v));
          break;
        case UINT:
          str += std::to_string(GetUInt(v));
          break;
      }
      str += "|";
    }
    results.insert(str);
  }
  DestroyResult(&res);
  return results;
}

void EQUALS_AS_SETS(std::unordered_set<std::string> &&actual,
                    std::vector<std::string> &&expected) {
  EXPECT_EQ(actual.size(), expected.size());
  for (const auto &str : expected) {
    EXPECT_TRUE(actual.count(str) == 1) << " !in " << str;
  }
}

void EQUALS_AS_SETS(std::unordered_set<std::string> &&actual,
                    std::string &&expected) {
  EXPECT_EQ(actual.size(), 1) << actual.size() << " != " << expected;
  EXPECT_TRUE(actual.count(expected) == 1) << " !in " << expected;
}

}  // namespace

TEST(MemcachedTest, InitialCache) {
  // Create the cache.
  for (const auto &query : QUERIES) {
    Cache(query.c_str());
  }

  // Read stuff from memcached.
  // View 0.
  EQUALS_AS_SETS(ReadAsString(0), {"|1|John|25|", "|2|Smith|35|",
                                   "|3|Kinan|27|", "|4|Zidane|-45|"});

  // View 1.
  EQUALS_AS_SETS(ReadAsString(1, {I(1)}), "|1|John|25|");
  EQUALS_AS_SETS(ReadAsString(1, {I(2)}), "|2|Smith|35|");
  EQUALS_AS_SETS(ReadAsString(1, {I(3)}), "|3|Kinan|27|");
  EQUALS_AS_SETS(ReadAsString(1, {I(4)}), "|4|Zidane|-45|");
  EQUALS_AS_SETS(ReadAsString(1, {I(10)}), std::vector<std::string>{});

  // View 2.
  EQUALS_AS_SETS(ReadAsString(2, {S("John")}), "|1|John|25|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Smith")}), "|2|Smith|35|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Kinan")}), "|3|Kinan|27|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Zidane")}), "|4|Zidane|-45|");

  // View 3.
  EQUALS_AS_SETS(ReadAsString(3, {S("John"), I(25)}), "|John|25|50|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Smith"), I(35)}), "|Smith|35|130|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Kinan"), I(27)}), "|Kinan|27|286|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Zidane"), I(-45)}), "|Zidane|-45|8000|");
}

TEST(MemcachedTest, Update1) {
  // Update the DB.
  for (const auto &update : UPDATES1) {
    __ExecuteDB(update.c_str());
  }

  // Update the cache.
  Update("students");

  // View 0.
  EQUALS_AS_SETS(ReadAsString(0),
                 {"|1|John|25|", "|2|Smith|35|", "|3|Kinan|28|",
                  "|4|Zidane|-45|", "|5|New|50|"});

  // View 1.
  EQUALS_AS_SETS(ReadAsString(1, {I(1)}), "|1|John|25|");
  EQUALS_AS_SETS(ReadAsString(1, {I(2)}), "|2|Smith|35|");
  EQUALS_AS_SETS(ReadAsString(1, {I(3)}), "|3|Kinan|28|");
  EQUALS_AS_SETS(ReadAsString(1, {I(4)}), "|4|Zidane|-45|");
  EQUALS_AS_SETS(ReadAsString(1, {I(5)}), "|5|New|50|");

  // View 2.
  EQUALS_AS_SETS(ReadAsString(2, {S("John")}), "|1|John|25|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Smith")}), "|2|Smith|35|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Kinan")}), "|3|Kinan|28|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Zidane")}), "|4|Zidane|-45|");
  EQUALS_AS_SETS(ReadAsString(2, {S("New")}), "|5|New|50|");

  // View 3.
  EQUALS_AS_SETS(ReadAsString(3, {S("John"), I(25)}), "|John|25|50|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Smith"), I(35)}), "|Smith|35|130|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Kinan"), I(28)}), "|Kinan|28|286|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Zidane"), I(-45)}), "|Zidane|-45|8000|");
}

TEST(MemcachedTest, Update2) {
  // Update the DB.
  for (const auto &update : UPDATES2) {
    __ExecuteDB(update.c_str());
  }

  // Update the cache.
  Update("grades");

  // View 0.
  EQUALS_AS_SETS(ReadAsString(0),
                 {"|1|John|25|", "|2|Smith|35|", "|3|Kinan|28|",
                  "|4|Zidane|-45|", "|5|New|50|"});

  // View 1.
  EQUALS_AS_SETS(ReadAsString(1, {I(1)}), "|1|John|25|");
  EQUALS_AS_SETS(ReadAsString(1, {I(2)}), "|2|Smith|35|");
  EQUALS_AS_SETS(ReadAsString(1, {I(3)}), "|3|Kinan|28|");
  EQUALS_AS_SETS(ReadAsString(1, {I(4)}), "|4|Zidane|-45|");
  EQUALS_AS_SETS(ReadAsString(1, {I(5)}), "|5|New|50|");

  // View 2.
  EQUALS_AS_SETS(ReadAsString(2, {S("John")}), "|1|John|25|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Smith")}), "|2|Smith|35|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Kinan")}), "|3|Kinan|28|");
  EQUALS_AS_SETS(ReadAsString(2, {S("Zidane")}), "|4|Zidane|-45|");
  EQUALS_AS_SETS(ReadAsString(2, {S("New")}), "|5|New|50|");

  // View 3.
  EQUALS_AS_SETS(ReadAsString(3, {S("John"), I(25)}), "|John|25|50|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Smith"), I(35)}), "|Smith|35|130|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Kinan"), I(28)}), "|Kinan|28|286|");
  EQUALS_AS_SETS(ReadAsString(3, {S("Zidane"), I(-45)}), "|Zidane|-45|3000|");
  EQUALS_AS_SETS(ReadAsString(3, {S("New"), I(50)}), "|New|50|40|");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Command line arugments and help message.
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Command line params.
  const std::string &db_username = FLAGS_db_username;
  const std::string &db_password = FLAGS_db_password;

  // Setup memcached interface.
  Initialize(DB_NAME, db_username.c_str(), db_password.c_str(), "_");

  // Create schema and insert data.
  for (const auto &create : SCHEMA) {
    __ExecuteDB(create.c_str());
  }
  for (const auto &insert : INSERTS) {
    __ExecuteDB(insert.c_str());
  }

  // Run tests.
  auto result = RUN_ALL_TESTS();

  // DROP test DB.
  __ExecuteDB("DROP DATABASE " DB_NAME);

  Close();

  return result;
}
