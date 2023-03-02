#ifndef PELTON_SHARDS_SQLENGINE_TESTS_HELPERS_H_
#define PELTON_SHARDS_SQLENGINE_TESTS_HELPERS_H_

#include <filesystem>
#include <iostream>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "pelton/connection.h"
#include "pelton/sql/result.h"

// Has to be included after parser to avoid FAIL symbol redefinition with ANTLR.
// clang-format off
// NOLINTNEXTLINE
#include "gtest/gtest.h"
// clang-format on

namespace pelton {
namespace sql {

// Equality modulo order.
bool operator==(const sql::SqlResultSet &set,
                const std::vector<std::string> &vec);

bool operator==(const std::vector<sql::SqlResultSet> &left,
                const std::vector<std::vector<std::string>> &right);

}  // namespace sql
}  // namespace pelton

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * Helpers to simplify / reduce redundancy when writing CREATE TABLE
 * statements.s
 */
#define I " int "
#define STR " text "
#define PK " PRIMARY KEY "
#define FK " REFERENCES "

#define ON_GET " ON GET "
#define ON_DEL " ON DEL "
#define ANON " ANON "
#define DEL_ROW " DELETE_ROW "

#define NO " ONLY "
#define OB " OWNED_BY "
#define OW " OWNS "
#define AB " ACCESSED_BY "
#define AC " ACCESSES "

#define INT sqlast::ColumnDefinition::Type::INT
#define TEXT sqlast::ColumnDefinition::Type::TEXT

/*
 * Easy creation of sqlast::Statements.
 */
std::string MakeCreate(const std::string &tbl_name,
                       const std::vector<std::string> &cols,
                       bool data_subject = false, std::string anon_str = "");

std::pair<std::string, std::string> MakeInsert(
    const std::string &tbl_name, const std::vector<std::string> &vals);

std::string MakeDelete(const std::string &tbl_name,
                       const std::vector<std::string> &conds);

/*
 * Easy creation of sqlast::Statements.
 */
std::string MakeGDPRGet(const std::string &tbl_name,
                        const std::string &user_id);

/*
 * Easy creation of sqlast::Statements.
 */
std::string MakeGDPRForget(const std::string &tbl_name,
                           const std::string &user_id);

/*
 * Execute a statement.
 */
sql::SqlResult Execute(const std::string &sql, Connection *conn);

/*
 * Macros for EXPECTing that SqlResults are correct.
 */
#define EXPECT_SUCCESS(r) EXPECT_TRUE(r.Success())
#define EXPECT_UPDATE(r, c) EXPECT_EQ(r.UpdateCount(), c)

/*
 * google test fixture class, allows us to manage the pelton state and
 * initialize / destruct it for every test.
 */
#define PELTON_FIXTURE(NAME)                                               \
  static const char *DB_PATH = "/tmp/pelton/rocksdb/" #NAME;               \
  class NAME : public ::testing::Test {                                    \
   protected:                                                              \
    void SetUp() override {                                                \
      std::filesystem::remove_all(DB_PATH);                                \
      this->pelton_state_ = std::make_unique<State>(3, true);              \
      this->pelton_state_->Initialize(#NAME, "");                          \
    }                                                                      \
    void TearDown() override {                                             \
      this->pelton_state_ = nullptr;                                       \
      std::filesystem::remove_all(DB_PATH);                                \
    }                                                                      \
    Connection CreateConnection() {                                        \
      Connection connection;                                               \
      connection.state = this->pelton_state_.get();                        \
      connection.session = this->pelton_state_->Database()->OpenSession(); \
      return connection;                                                   \
    }                                                                      \
                                                                           \
   private:                                                                \
    std::unique_ptr<State> pelton_state_;                                  \
  }

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton

namespace std {

// For logging / debugging.
template <typename T>
ostream &operator<<(ostream &os, const vector<T> &v) {
  os << "[";
  for (size_t i = 0; i < v.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << v.at(i);
  }
  os << "]";
  return os;
}

template <typename T>
ostream &operator<<(ostream &os, const unordered_set<T> &v) {
  os << "[";
  for (size_t i = 0; i < v.size(); i++) {
    if (i > 0) {
      os << ", ";
    }
    os << v.at(i);
  }
  os << "]";
  return os;
}

ostream &operator<<(ostream &os, const pelton::sql::SqlResultSet &result) {
  os << "{";
  for (size_t i = 0; i < result.size(); i++) {
    if (i == 0) {
      os << endl;
    }
    os << result.rows().at(i) << endl;
  }
  os << "}";
  return os;
}

}  // namespace std

#endif  // PELTON_SHARDS_SQLENGINE_TESTS_HELPERS_H_
