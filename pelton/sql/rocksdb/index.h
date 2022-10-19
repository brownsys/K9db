#ifndef PELTON_SQL_ROCKSDB_INDEX_H__
#define PELTON_SQL_ROCKSDB_INDEX_H__

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "pelton/sql/rocksdb/dedup.h"
#include "pelton/sql/rocksdb/encode.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace pelton {
namespace sql {

class RocksdbIndex {
 public:
  RocksdbIndex(rocksdb::DB *db, const std::string &table_name, size_t column);

  // Consider table (id PK, name index, age); sharded by some <shard_name>
  // shard_name is the shard_name specialized with the user_id.
  // index_value is the value of 'name'.
  // key is the value of 'id' (the PK) without a shard prefix.
  void Add(const rocksdb::Slice &index_value, const rocksdb::Slice &shard_name,
           const rocksdb::Slice &pk);
  void Delete(const rocksdb::Slice &index_value,
              const rocksdb::Slice &shard_name, const rocksdb::Slice &pk);

  // Get the shard and pk of matching records for given values.
  IndexSet Get(const std::vector<rocksdb::Slice> &values) const;
  DedupIndexSet GetDedup(const std::vector<rocksdb::Slice> &values) const;

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;

  using IRecord = RocksdbIndexInternalRecord;
};

class RocksdbPKIndex {
 public:
  RocksdbPKIndex(rocksdb::DB *db, const std::string &table_name);

  // Updating.
  void Add(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name);
  void Delete(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name);

  // Reading.
  IndexSet Get(const std::vector<rocksdb::Slice> &pk_values) const;
  DedupIndexSet GetDedup(const std::vector<rocksdb::Slice> &pk_values) const;

  // Count how many shard each pk value is in.
  std::vector<size_t> CountShards(
      const std::vector<rocksdb::Slice> &pk_values) const;

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;

  using IRecord = RocksdbPKIndexInternalRecord;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_INDEX_H__
