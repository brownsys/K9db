#ifndef PELTON_SQL_ROCKSDB_BENCH_COMMON_H_
#define PELTON_SQL_ROCKSDB_BENCH_COMMON_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/connection.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

// In memory index (for old code benchmarking).
enum IndexColumn { PK, PUR, NO_INDEX };
using MemoryIndex =
    std::unordered_map<std::string, std::unordered_set<std::string>>;

// Drop the database (if it exists).
void DropDatabase(const std::string &db_name);

// Open a pelton connection.
std::unique_ptr<RocksdbConnection> OpenConnection(const std::string &db_name);

// Extract the table schema.
dataflow::SchemaRef ExtractSchema(const std::string &filename);

// Parse a file
std::vector<std::unique_ptr<sqlast::AbstractStatement>> ParseFile(
    const std::string &filename, IndexColumn index_column = NO_INDEX,
    MemoryIndex *index = nullptr);

// Execute and time statements.
uint64_t ExecuteStatements(
    RocksdbConnection *conn,
    const std::vector<std::unique_ptr<sqlast::AbstractStatement>> &statements,
    const dataflow::SchemaRef &schema, IndexColumn index_column = NO_INDEX,
    const MemoryIndex *index = nullptr);

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_BENCH_COMMON_H_
