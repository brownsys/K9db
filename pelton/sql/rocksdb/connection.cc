#include "pelton/sql/rocksdb/connection.h"

#include "pelton/util/status.h"
#include "rocksdb/options.h"

namespace pelton {
namespace sql {

// Open a connection.
void RocksdbConnection::Open(const std::string &db_name) {
  std::string path = "/tmp/pelton/rocksdb/" + db_name;

  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  this->db_ = std::unique_ptr<rocksdb::DB>(db);
}

// Close the connection.
void RocksdbConnection::Close() {
  this->tables_.clear();
  this->db_ = nullptr;
}

}  // namespace sql
}  // namespace pelton
