#ifndef PELTON_SQL_CONNECTIONS_ROCKSDB_INDEX_H__
#define PELTON_SQL_CONNECTIONS_ROCKSDB_INDEX_H__

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/db.h"

namespace pelton {
namespace sql {

using ShardID = std::string;

class RocksdbIndex {
 public:
  RocksdbIndex(rocksdb::DB *db, size_t table_id, size_t column);

  void Add(const std::string &index_value, const rocksdb::Slice &key);
  void Delete(const std::string &index_value, const rocksdb::Slice &key);

  std::vector<std::string> Get(const ShardID &shard_id,
                               const std::vector<rocksdb::Slice> &values);

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTIONS_ROCKSDB_INDEX_H__
