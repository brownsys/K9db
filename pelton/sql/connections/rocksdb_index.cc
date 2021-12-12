#include "pelton/sql/connections/rocksdb_index.h"

#include "pelton/sql/connections/rocksdb_util.h"
#include "pelton/util/status.h"

#define __ROCKSSEP static_cast<char>(30)

namespace pelton {
namespace sql {

// Constructor.
RocksdbIndex::RocksdbIndex(rocksdb::DB *db, const std::string &table_name,
                           size_t column)
    : db_(db) {
  // Create a name for the index.
  std::string index_name = table_name + "_" + std::to_string(column);
  // Create a column family for this index.
  rocksdb::ColumnFamilyHandle *handle;
  PANIC_STATUS(this->db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                             index_name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbIndex::Add(const rocksdb::Slice &value, const rocksdb::Slice &key) {
  // key.ToString() << std::endl;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::string str = value.ToString() + __ROCKSSEP + key.ToString() + __ROCKSSEP;
  rocksdb::Slice kslice(str);
  PANIC_STATUS(this->db_->Put(rocksdb::WriteOptions(), handle, kslice, ""));
}

// Deleting things from index.
void RocksdbIndex::Delete(const rocksdb::Slice &value,
                          const rocksdb::Slice &key) {
  // key.ToString() << std::endl;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::string str = value.ToString() + __ROCKSSEP + key.ToString() + __ROCKSSEP;
  rocksdb::Slice kslice(str);
  PANIC_STATUS(this->db_->Delete(rocksdb::WriteOptions(), handle, kslice));
}

// Lookup by a single value.
std::vector<std::string> RocksdbIndex::Get(const rocksdb::Slice &value) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::string prefix = value.ToString() + __ROCKSSEP;
  rocksdb::Slice pslice(prefix);
  // Read using prefix.
  std::vector<std::string> result;
  auto ptr = this->db_->NewIterator(rocksdb::ReadOptions(), handle);
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  for (it->Seek(pslice); it->Valid(); it->Next()) {
    rocksdb::Slice row = it->key();
    if (row.size() < pslice.size()) {
      break;
    }
    if (!SlicesEq(pslice, rocksdb::Slice(row.data(), pslice.size()))) {
      break;
    }
    rocksdb::Slice key = ExtractColumn(row, 1);
    result.push_back(key.ToString());
  }
  return result;
}

// Lookup by multiple values.
std::vector<std::string> RocksdbIndex::Get(
    const std::vector<rocksdb::Slice> &values) {
  if (values.size() == 0) {
    return {};
  }
  std::vector<std::string> result = this->Get(values.front());
  for (size_t i = 1; i < values.size(); i++) {
    std::vector<std::string> tmp = this->Get(values.at(i));
    for (auto &str : tmp) {
      result.push_back(std::move(str));
    }
  }
  return result;
}

}  // namespace sql
}  // namespace pelton
