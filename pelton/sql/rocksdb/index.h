#ifndef PELTON_SQL_ROCKSDB_INDEX_H__
#define PELTON_SQL_ROCKSDB_INDEX_H__

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "pelton/dataflow/schema.h"
#include "pelton/sql/rocksdb/dedup.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/sql/rocksdb/transaction.h"
#include "pelton/sqlast/value_mapper.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"

namespace pelton {
namespace sql {
namespace rocks {

class RocksdbIndex {
 public:
  RocksdbIndex(rocksdb::DB *db, const std::string &table_name,
               const std::vector<size_t> &columns);

  // Consider table (id PK, name index, age); sharded by some <shard_name>
  // shard_name is the shard_name specialized with the user_id.
  // index_value is the value of 'name'.
  // key is the value of 'id' (the PK) without a shard prefix.
  void Add(const std::vector<rocksdb::Slice> &index_values,
           const rocksdb::Slice &shard_name, const rocksdb::Slice &pk,
           RocksdbTransaction *txn);
  void Delete(const std::vector<rocksdb::Slice> &index_values,
              const rocksdb::Slice &shard_name, const rocksdb::Slice &pk,
              RocksdbTransaction *txn);

  // Use to encode condition values for composite index.
  std::vector<std::string> EncodeComposite(
      sqlast::ValueMapper *vm, const dataflow::SchemaRef &schema) const;

  // Get the shard and pk of matching records for given values.
  IndexSet Get(std::vector<std::string> &&values,
               const RocksdbTransaction *txn) const;
  DedupIndexSet GetDedup(std::vector<std::string> &&values,
                         const RocksdbTransaction *txn) const;

  IndexSet GetWithShards(std::vector<std::string> &&shards,
                         std::vector<std::string> &&values,
                         const RocksdbTransaction *txn) const;

  // Get the columns over which this index is defined.
  const std::vector<size_t> &GetColumns() const { return this->columns_; }

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
  std::vector<size_t> columns_;

  using IRecord = RocksdbIndexInternalRecord;
};

class RocksdbPKIndex {
 public:
  RocksdbPKIndex(rocksdb::DB *db, const std::string &table_name);

  // Updating.
  void Add(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name,
           RocksdbTransaction *txn);
  void Delete(const rocksdb::Slice &pk, const rocksdb::Slice &shard_name,
              RocksdbTransaction *txn);

  // Reading.
  bool CheckUniqueAndLock(const rocksdb::Slice &slice,
                          const RocksdbTransaction *txn) const;

  IndexSet Get(std::vector<std::string> &&pk_values,
               const RocksdbTransaction *txn) const;
  DedupIndexSet GetDedup(std::vector<std::string> &&pk_values,
                         const RocksdbTransaction *txn) const;

  // Count how many shard each pk value is in.
  std::vector<size_t> CountShards(std::vector<std::string> &&pk_values,
                                  const RocksdbTransaction *txn) const;

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;

  using IRecord = RocksdbPKIndexInternalRecord;
};

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_INDEX_H__
