#include "pelton/sql/connections/rocksdb_index.h"

#include <algorithm>

#include "pelton/sql/connections/rocksdb_util.h"
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
  PANIC_STATUS(this->db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                             name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbIndex::Add(const std::string &value, const rocksdb::Slice &key) {
  // key.ToString() << std::endl;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::string str = value + __ROCKSSEP + key.ToString() + __ROCKSSEP;
  rocksdb::Slice kslice(str);
  PANIC_STATUS(this->db_->Put(rocksdb::WriteOptions(), handle, kslice, ""));
}

// Deleting things from index.
void RocksdbIndex::Delete(const std::string &value, const rocksdb::Slice &key) {
  // key.ToString() << std::endl;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::string str = value + __ROCKSSEP + key.ToString() + __ROCKSSEP;
  rocksdb::Slice kslice(str);
  PANIC_STATUS(this->db_->Delete(rocksdb::WriteOptions(), handle, kslice));
}

// Lookup by multiple values.
std::vector<std::string> RocksdbIndex::Get(
    const ShardID &shard_id, const std::vector<rocksdb::Slice> &values) {
  if (values.size() == 0) {
    return {};
  }
  // Will store result.
  std::vector<std::string> result;
  // Open an iterator.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  auto ptr = this->db_->NewIterator(options, this->handle_.get());
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  // Make and sort all prefixes.
  std::vector<std::string> prefixes;
  prefixes.reserve(values.size());
  for (const rocksdb::Slice &slice : values) {
    prefixes.push_back(shard_id + slice.ToString() + __ROCKSSEP);
  }
  std::sort(prefixes.begin(), prefixes.end());
  // Go through prefixes in order.
  for (const std::string &prefix : prefixes) {
    rocksdb::Slice pslice(prefix);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      rocksdb::Slice row = it->key();
      if (row.size() < pslice.size()) {
        break;
      }
      if (!SlicesEq(pslice, rocksdb::Slice(row.data(), pslice.size()))) {
        break;
      }
      result.push_back(ExtractColumn(row, 1).ToString());
    }
  }
  return result;
}

}  // namespace sql
}  // namespace pelton
