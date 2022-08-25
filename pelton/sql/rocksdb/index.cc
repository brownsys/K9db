#include "pelton/sql/rocksdb/index.h"

#include <algorithm>

#include "pelton/sql/rocksdb/encode.h"
#include "pelton/util/status.h"

namespace pelton {
namespace sql {

// Constructor.
RocksdbIndex::RocksdbIndex(rocksdb::DB *db, size_t table_id, size_t column)
    : db_(db) {
  // Create a name for the index.
  std::string name = std::to_string(table_id) + "_" + std::to_string(column);

  // Create a column family for this index.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.prefix_extractor.reset(new PrefixTransform());
  options.comparator = rocksdb::BytewiseComparator();
  PANIC_STATUS(this->db_->CreateColumnFamily(options, name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbIndex::Add(const rocksdb::Slice &value,
                       const std::string &shard_name,
                       const rocksdb::Slice &pk) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  RocksdbIndexRecord e(value, shard_name, pk);
  PANIC_STATUS(this->db_->Put(rocksdb::WriteOptions(), handle, e.Key(), ""));
}

// Deleting things from index.
void RocksdbIndex::Delete(const rocksdb::Slice &value,
                          const std::string &shard_name,
                          const rocksdb::Slice &pk) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  RocksdbIndexRecord e(value, shard_name, pk);
  PANIC_STATUS(this->db_->Delete(rocksdb::WriteOptions(), handle, e.Key()));
}

// Get by value.
std::vector<std::pair<std::string, std::string>> RocksdbIndex::Get(
    const std::vector<rocksdb::Slice> &values) {
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
  std::vector<std::pair<std::string, std::string>> result;

  // Go through prefixes in order.
  for (const std::string &prefix : prefixes) {
    rocksdb::Slice pslice(prefix);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      RocksdbIndexRecord entry(it->key());
      rocksdb::Slice shard = entry.ExtractShardName();
      rocksdb::Slice pk = entry.ExtractPK();
      result.push_back({shard.ToString(), pk.ToString()});
    }
  }
  return result;
}

// PrefixTransform
rocksdb::Slice PrefixTransform::Transform(const rocksdb::Slice &key) const {
  return ExtractColumn(key, 0);
}

}  // namespace sql
}  // namespace pelton
