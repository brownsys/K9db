#include "pelton/sql/rocksdb/bench/common.h"

#include <cassert>
// NOLINTNEXTLINE
#include <chrono>
// NOLINTNEXTLINE
#include <filesystem>
#include <fstream>
#include <utility>

#include "glog/logging.h"
#include "pelton/sqlast/parser.h"

namespace pelton {
namespace sql {

#define DB_PATH "/tmp/pelton/rocksdb/"
#define FILE_PATH "pelton/sql/rocksdb/bench/data/"

namespace {

// Type aliases.
using millis = std::chrono::milliseconds;

// Extract Shard ID / name from query.
std::string FindShard(const std::string &query) {
  auto start = query.find("'user");
  auto len = query.substr(++start).find("'");
  return query.substr(start, len);
}

std::string FindValue(const std::string &query, IndexColumn column) {
  auto start = query.find(column == PK ? "'key" : "'purpose");
  auto len = query.substr(++start).find("'");
  return query.substr(start, len);
}

// Parse sql string into pelton's AST.
std::unique_ptr<sqlast::AbstractStatement> Parse(const std::string &sql) {
  sqlast::SQLParser parser;
  // std::unique_ptr<sqlast::AbstractStatement> statement,
  auto statusor = parser.Parse(sql);
  CHECK_EQ(statusor.ok(), true) << statusor.status();
  return std::move(statusor.value());
}

// Get the values the select statement is conditioning on.
// E.g. SELECT * FROM table WHERE pk IN (1, 2, 3)
// returns [1, 2, 3]
std::vector<std::string> GetConditionValues(
    const sqlast::AbstractStatement *stmt) {
  const sqlast::BinaryExpression *where =
      reinterpret_cast<const sqlast::Select *>(stmt)->GetWhereClause();
  const sqlast::Expression *expr = where->GetRight();
  switch (expr->type()) {
    case sqlast::Expression::Type::LITERAL: {
      const sqlast::LiteralExpression *literal =
          reinterpret_cast<const sqlast::LiteralExpression *>(expr);
      return {literal->value()};
    }
    case sqlast::Expression::Type::LIST: {
      const sqlast::LiteralListExpression *list =
          reinterpret_cast<const sqlast::LiteralListExpression *>(expr);
      return {list->values()};
    }
    default:
      assert(false);
  }
}

}  // namespace

// Drop the database (if it exists).
void DropDatabase(const std::string &db_name) {
  std::filesystem::remove_all(DB_PATH + db_name);
}

// Open a pelton connection.
std::unique_ptr<RocksdbConnection> OpenConnection(const std::string &db_name) {
  std::unique_ptr<RocksdbConnection> ptr =
      std::make_unique<RocksdbConnection>();
  ptr->Open(db_name);
  return ptr;
}

// Extract the table schema.
dataflow::SchemaRef ExtractSchema(const std::string &filename) {
  std::fstream s;
  s.open(FILE_PATH + filename + ".sql", std::ios::in);
  assert(s.is_open());

  size_t count = 0;
  std::string cmd;
  dataflow::SchemaRef schema;
  while (getline(s, cmd)) {
    if (++count == 2) {
      std::unique_ptr<sqlast::AbstractStatement> parsed = Parse(cmd);
      schema = dataflow::SchemaFactory::Create(
          *static_cast<sqlast::CreateTable *>(parsed.get()));
      s.close();
      return schema;
    }
  }
  assert(false);
}

// Parse a file
std::vector<std::unique_ptr<sqlast::AbstractStatement> > ParseFile(
    const std::string &filename, IndexColumn index_column, MemoryIndex *index) {
  std::fstream s;
  s.open(FILE_PATH + filename + ".sql", std::ios::in);
  assert(s.is_open());

  // Read and parse.
  std::vector<std::unique_ptr<sqlast::AbstractStatement> > statements;
  std::string cmd;
  while (getline(s, cmd)) {
    if (index_column != NO_INDEX) {
      (*index)[FindValue(cmd, index_column)].insert(FindShard(cmd));
    }
    statements.push_back(Parse(cmd));
  }
  s.close();

  return statements;
}

// Execute and time statements.
uint64_t ExecuteStatements(
    RocksdbConnection *conn,
    const std::vector<std::unique_ptr<sqlast::AbstractStatement> > &statements,
    const dataflow::SchemaRef &schema, IndexColumn index_column,
    const MemoryIndex *index) {
  // Execute with timing.
  auto start = std::chrono::steady_clock::now();
  for (const auto &stmt : statements) {
    switch (stmt->type()) {
      case sqlast::AbstractStatement::Type::CREATE_TABLE: {
        // const sqlast::CreateTable &create =
        // *static_cast<pelton::sqlast::CreateTable *>(stmt.get());
        conn->ExecuteCreateTable(
            *static_cast<pelton::sqlast::CreateTable *>(stmt.get()));

        break;
      }
      case sqlast::AbstractStatement::Type::CREATE_INDEX:
      case sqlast::AbstractStatement::Type::CREATE_VIEW: {
        conn->ExecuteCreateIndex(*static_cast<sqlast::CreateIndex *>(
            stmt.get()));  // Do views go into index too?
        break;
      }
      case sqlast::AbstractStatement::Type::INSERT: {
        const sqlast::Insert *insert =
            reinterpret_cast<const sqlast::Insert *>(stmt.get());
        const std::string &shard = insert->GetValues().at(2);
        std::string dequote = shard.substr(1, shard.size() - 2);
        conn->ExecuteInsert(*insert, dequote);
        break;
      }
      case sqlast::AbstractStatement::Type::SELECT: {
        if (index_column == NO_INDEX) {
          auto set = conn->ExecuteSelect(
              *static_cast<sqlast::Select *>(stmt.get()), schema, {});
        } else {
          SqlResultSet result(schema);
          std::unordered_set<std::string> shards;
          for (const auto &value : GetConditionValues(stmt.get())) {
            std::string dequote = value.substr(1, value.size() - 2);
            for (const std::string &shard : index->at(dequote)) {
              shards.insert(shard);
            }
          }
          for (const auto &shard : shards) {
            result.Append(conn->ExecuteQueryShard(
                              *static_cast<sqlast::Select *>(stmt.get()),
                              schema, {}, shard),
                          false);
          }
        }
        break;
      }
      default: {
        assert(false);
      }
    }
  }
  auto end = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<millis>(end - start).count();
}

}  // namespace sql
}  // namespace pelton
