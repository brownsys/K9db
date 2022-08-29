#include "pelton/sql/rocksdb/index.h"

#include <algorithm>

#include "pelton/util/status.h"

namespace pelton {
namespace sql {

/*
 * RocksdbIndex
 */
// Constructor.
RocksdbIndex::RocksdbIndex(rocksdb::DB *db, const std::string &table_name,
                           size_t column)
    : db_(db) {
  // Create a name for the index.
  std::string name = table_name + "_index" + std::to_string(column);

  // Create a column family for this index.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.prefix_extractor.reset(new IndexPrefixTransform());
  options.comparator = rocksdb::BytewiseComparator();
  PANIC(this->db_->CreateColumnFamily(options, name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbIndex::Add(const rocksdb::Slice &value,
                       const rocksdb::Slice &shard_name,
                       const rocksdb::Slice &pk) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  RocksdbIndexRecord e(value, shard_name, pk);
  PANIC(this->db_->Put(rocksdb::WriteOptions(), handle, e.Data(), ""));
}

// Deleting things from index.
void RocksdbIndex::Delete(const rocksdb::Slice &value,
                          const rocksdb::Slice &shard_name,
                          const rocksdb::Slice &pk) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  RocksdbIndexRecord e(value, shard_name, pk);
  PANIC(this->db_->Delete(rocksdb::WriteOptions(), handle, e.Data()));
}

// Get by value.
IndexSet RocksdbIndex::Get(const std::vector<rocksdb::Slice> &values) const {
  // Iterator options.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.total_order_seek = false;
  options.prefix_same_as_start = true;
  options.verify_checksums = false;

  // Open iterator.
  rocksdb::Iterator *ptr = this->db_->NewIterator(options, this->handle_.get());
  std::unique_ptr<rocksdb::Iterator> it(ptr);

  // Make and sort all prefixes.
  std::vector<std::string> prefixes;
  prefixes.reserve(values.size());
  for (const rocksdb::Slice &slice : values) {
    prefixes.push_back(slice.ToString() + __ROCKSSEP);
  }
  std::sort(prefixes.begin(), prefixes.end());

  // Holds the result.
  IndexSet result;

  // Go through prefixes in order.
  for (const std::string &prefix : prefixes) {
    rocksdb::Slice pslice(prefix);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      result.emplace(it->key());
    }
  }
  return result;
}

/*
 * RocksdbPKIndex
 */
// Constructor.
RocksdbPKIndex::RocksdbPKIndex(rocksdb::DB *db, const std::string &table_name)
    : db_(db) {
  // Create a name for the index.
  std::string name = table_name + "_pk_index";

  // Create a column family for this index.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.prefix_extractor.reset(new IndexPrefixTransform());
  options.comparator = rocksdb::BytewiseComparator();
  PANIC(this->db_->CreateColumnFamily(options, name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbPKIndex::Add(const rocksdb::Slice &pk_value,
                         const rocksdb::Slice &shard_name) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  RocksdbPKIndexRecord e(pk_value, shard_name);
  PANIC(this->db_->Put(rocksdb::WriteOptions(), handle, e.Data(), ""));
}

// Deleting things from index.
void RocksdbPKIndex::Delete(const rocksdb::Slice &pk_value,
                            const rocksdb::Slice &shard_name) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  RocksdbPKIndexRecord e(pk_value, shard_name);
  PANIC(this->db_->Delete(rocksdb::WriteOptions(), handle, e.Data()));
}

// Get by value.
PKIndexSet RocksdbPKIndex::Get(const std::vector<rocksdb::Slice> &vals) const {
  // Iterator options.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.total_order_seek = false;
  options.prefix_same_as_start = true;
  options.verify_checksums = false;

  // Open iterator.
  rocksdb::Iterator *ptr = this->db_->NewIterator(options, this->handle_.get());
  std::unique_ptr<rocksdb::Iterator> it(ptr);

  // Make and sort all prefixes.
  std::vector<std::string> prefixes;
  prefixes.reserve(vals.size());
  for (const rocksdb::Slice &slice : vals) {
    prefixes.push_back(slice.ToString() + __ROCKSSEP);
  }
  std::sort(prefixes.begin(), prefixes.end());

  // Holds the result.
  PKIndexSet result;

  // Go through prefixes in order.
  for (const std::string &prefix : prefixes) {
    rocksdb::Slice pslice(prefix);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      result.emplace(it->key());
    }
  }
  return result;
}

// PrefixTransform
rocksdb::Slice IndexPrefixTransform::Transform(
    const rocksdb::Slice &key) const {
  return ExtractColumn(key, 0);
}

}  // namespace sql
}  // namespace pelton
