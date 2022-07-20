#include "pelton/sql/rocksdb/index.h"

#include <algorithm>

#include "pelton/sql/rocksdb/util.h"
#include "pelton/util/status.h"

#define __ROCKSSEP static_cast<char>(30)

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
  options.prefix_extractor.reset(new ShardPrefixTransform(1));
  options.comparator = rocksdb::BytewiseComparator();
  PANIC_STATUS(this->db_->CreateColumnFamily(options, name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbIndex::Add(const rocksdb::Slice &value,
                       const std::string &shard_name,
                       const rocksdb::Slice &key) {
  // key.ToString() << std::endl;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::string str = value.ToString() + __ROCKSSEP + shard_name + __ROCKSSEP +
                    key.ToString() + __ROCKSSEP;
  rocksdb::Slice kslice(str);
  PANIC_STATUS(this->db_->Put(rocksdb::WriteOptions(), handle, kslice, ""));
}

// Deleting things from index.
void RocksdbIndex::Delete(const rocksdb::Slice &value,
                          const std::string &shard_name,
                          const rocksdb::Slice &key) {
  // key.ToString() << std::endl;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::string str = value.ToString() + __ROCKSSEP + shard_name + __ROCKSSEP +
                    key.ToString() + __ROCKSSEP;
  rocksdb::Slice kslice(str);
  PANIC_STATUS(this->db_->Delete(rocksdb::WriteOptions(), handle, kslice));
}

// Lookup by multiple values.
std::vector<std::string> RocksdbIndex::GetShard(
    const std::vector<rocksdb::Slice> &values, const std::string &shard_name) {
  if (values.size() == 0) {
    return {};
  }
  // Will store result.
  std::vector<std::string> result;
  // Open an iterator.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.total_order_seek = false;
  options.prefix_same_as_start = true;
  options.verify_checksums = false;
  auto ptr = this->db_->NewIterator(options, this->handle_.get());
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  // Make and sort all prefixes.
  std::vector<std::string> prefixes;
  prefixes.reserve(values.size());
  for (const rocksdb::Slice &slice : values) {
    prefixes.push_back(slice.ToString() +
                       __ROCKSSEP);  // + shard_name + __ROCKSSEP);
  }
  std::sort(prefixes.begin(), prefixes.end());
  // Go through prefixes in order.
  for (const std::string &prefix : prefixes) {
    rocksdb::Slice pslice(prefix);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      rocksdb::Slice row = it->key();
      if (ExtractColumn(row, 1) == shard_name) {
        result.push_back(ExtractColumn(row, 2).ToString());
      }
    }
  }
  return result;
}
std::vector<std::pair<std::string, std::string>> RocksdbIndex::Get(
    const std::vector<rocksdb::Slice> &values) {
  if (values.size() == 0) {
    return {};
  }
  // Will store result.
  std::vector<std::pair<std::string, std::string>> result;
  // Open an iterator.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.total_order_seek = false;
  options.prefix_same_as_start = true;
  options.verify_checksums = false;
  auto ptr = this->db_->NewIterator(options, this->handle_.get());
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  // Make and sort all prefixes.
  std::vector<std::string> prefixes;
  prefixes.reserve(values.size());
  for (const rocksdb::Slice &slice : values) {
    prefixes.push_back(slice.ToString() + __ROCKSSEP);
  }
  std::sort(prefixes.begin(), prefixes.end());
  // Go through prefixes in order.
  for (const std::string &prefix : prefixes) {
    rocksdb::Slice pslice(prefix);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      rocksdb::Slice row = it->key();
      result.push_back(
          {ExtractColumn(row, 1).ToString(), ExtractColumn(row, 2).ToString()});
    }
  }
  return result;
}
}  // namespace sql
}  // namespace pelton
