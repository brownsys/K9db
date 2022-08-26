#include "pelton/sql/rocksdb/connection.h"

#include <unordered_set>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

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
  opts.prefix_extractor.reset(new PeltonPrefixTransform());
  opts.comparator = PeltonComparator();

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  this->db_ = std::unique_ptr<rocksdb::DB>(db);
}

// Close the connection.
void RocksdbConnection::Close() {
  this->schemas_.clear();
  this->handlers_.clear();
  this->indices_.clear();
  this->db_ = nullptr;
}

}  // namespace sql
}  // namespace pelton
