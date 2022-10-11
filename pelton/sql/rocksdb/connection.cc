#include "pelton/sql/rocksdb/connection.h"

#include "glog/logging.h"
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

// Combine shard kind and user id to get a qualified shard name.
std::string RocksdbConnection::CombineShardName(
    const std::string &shard_kind, const std::string &user_id) const {
  return shard_kind + "__" + user_id;
}
std::pair<std::string, std::string> RocksdbConnection::SplitShardName(
    const std::string &shard_name) const {
  std::string shard_kind = "";
  for (size_t i = 0; i < shard_name.size() - 1; i++) {
    if (shard_name.at(i) == '_' && shard_name.at(i + 1) == '_') {
      return std::pair(std::move(shard_kind),
                       shard_name.substr(i + 2, shard_name.size() - i - 2));
    }
    shard_kind.push_back(shard_name.at(i));
  }
  LOG(FATAL) << "Cannot split shard name '" << shard_name << "'";
}

}  // namespace sql
}  // namespace pelton
