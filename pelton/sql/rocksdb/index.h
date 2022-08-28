#ifndef PELTON_SQL_ROCKSDB_INDEX_H__
#define PELTON_SQL_ROCKSDB_INDEX_H__

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "pelton/sql/rocksdb/encode.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace pelton {
namespace sql {

using IndexSet =
    std::unordered_set<RocksdbIndexRecord, RocksdbIndexRecord::TargetHash,
                       RocksdbIndexRecord::TargetEqual>;
using PKIndexSet = std::unordered_set<RocksdbPKIndexRecord,
                                      RocksdbPKIndexRecord::ShardNameHash,
                                      RocksdbPKIndexRecord::ShardNameEqual>;

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
  IndexSet Get(const std::vector<rocksdb::Slice> &values);

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
};

class RocksdbPKIndex {
 public:
  RocksdbPKIndex(rocksdb::DB *db, const std::string &table_name);

  // Updating.
  void Add(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name);
  void Delete(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name);

  // Reading.
  PKIndexSet Get(const std::vector<rocksdb::Slice> &pk_values);

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
};

class PrefixTransform : public rocksdb::SliceTransform {
 public:
  PrefixTransform() = default;

  const char *Name() const override { return "ShardPrefix"; }
  bool InDomain(const rocksdb::Slice &key) const override { return true; }
  rocksdb::Slice Transform(const rocksdb::Slice &key) const override;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_INDEX_H__
