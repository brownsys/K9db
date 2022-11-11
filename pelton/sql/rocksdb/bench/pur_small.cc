#include <iostream>
#include <string>

#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/bench/common.h"

namespace pelton {
namespace sql {
namespace {

#define DB_NAME "pur_small"

#define SCHEMA_FILE "pur_schema"
#define INSERT_FILE "pur_small_insert"
#define LOAD_FILE "pur_small_load"

void BenchmarkNew() {
  DropDatabase(DB_NAME);

  dataflow::SchemaRef schema = ExtractSchema(SCHEMA_FILE);

  // Execute files.
  auto conn = OpenConnection(DB_NAME);

  // Schema.
  auto stmts = ParseFile(SCHEMA_FILE);
  ExecuteStatements(conn.get(), stmts, schema);

  // Inserts.
  stmts = ParseFile(INSERT_FILE);
  ExecuteStatements(conn.get(), stmts, schema);

  // Load.
  stmts = ParseFile(LOAD_FILE);
  uint64_t time = ExecuteStatements(conn.get(), stmts, schema);
  std::cout << "(New) Time taken: " << time << "ms" << std::endl;
}

void BenchmarkOld() {
  DropDatabase(DB_NAME);

  dataflow::SchemaRef schema = ExtractSchema(SCHEMA_FILE);
  MemoryIndex index;

  // Execute files.
  auto conn = OpenConnection(DB_NAME);

  // Schema.
  auto stmts = ParseFile(SCHEMA_FILE);
  ExecuteStatements(conn.get(), stmts, schema);

  // Inserts.
  stmts = ParseFile(INSERT_FILE, PUR, &index);
  ExecuteStatements(conn.get(), stmts, schema, PK, &index);

  // Load.
  stmts = ParseFile(LOAD_FILE);
  uint64_t time = ExecuteStatements(conn.get(), stmts, schema, PK, &index);
  std::cout << "(Old) Time taken: " << time << "ms" << std::endl;
}

}  // namespace
}  // namespace sql
}  // namespace pelton

int main(int argc, char** argv) {
  pelton::sql::BenchmarkNew();
  pelton::sql::BenchmarkOld();
}
