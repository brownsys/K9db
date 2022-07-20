#ifndef PELTON_SQL_ROCKSDB_INDEX_H__
#define PELTON_SQL_ROCKSDB_INDEX_H__

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

  // Consider table (id PK, name index, age); sharded by some <shard_name>
  // shard_name is the shard_name specialized with the user_id.
  // index_value is the value of 'name'.
  // key is the value of 'id' (the PK) without a shard prefix.
  void Add(const rocksdb::Slice &index_value, const std::string &shard_name,
           const rocksdb::Slice &key);
  void Delete(const rocksdb::Slice &index_value, const std::string &shard_name,
              const rocksdb::Slice &key);

  // shard_name is the shard_name (same as Add/Delete).
  // values are values we know the index column (e.g. name) can take.
  // returns a list of PKs (no shard_name prefix) that correspond to rows
  // where the index value is IN values.
  std::vector<std::string> GetShard(const std::vector<rocksdb::Slice> &values,
                                    const std::string &shard_name);

  // new added method to get from all shards
  std::vector<std::pair<std::string, std::string>> Get(
      const std::vector<rocksdb::Slice> &values);

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_INDEX_H__
