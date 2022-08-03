#include "pelton/sql/rocksdb/encode.h"

#include "glog/logging.h"

namespace pelton {
namespace sql {

namespace {

rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col) {
  const char *data = slice.data();
  size_t start = 0;
  size_t size = 0;
  for (size_t i = 0; i < slice.size(); i++) {
    if (data[i] == __ROCKSSEP) {
      if (col == 0) {
        return rocksdb::Slice(data + start, size);
      } else {
        col--;
        start = i + 1;
        size = 0;
        continue;
      }
    }
    size++;
  }

  LOG(FATAL) << "Cannot extract column " << col << " from '" << slice.ToString()
             << "'";
}

}  // namespace

/*
 * RocksdbRecord
 */
// Construct a record when reading from rocksdb.
RocksdbRecord::RocksdbRecord(const rocksdb::Slice &key,
                             const rocksdb::Slice &value)
    : key_(key.ToString()), value_(value.ToString()) {}

// Construct a record when handling SQL statements.
RocksdbRecord::RocksdbRecord(const std::string &shard_name,
                             const std::string &pk,
                             const std::vector<std::string> &values)
    : key_(), value_() {
  this->key_ = shard_name + __ROCKSSEP + pk + __ROCKSSEP;
  for (const std::string &value : values) {
    if (value == "NULL") {
      this->value_ += __ROCKSNULL + __ROCKSSEP;
    } else if (value.starts_with('"') || value.starts_with('\'')) {
      this->value_ += value.substr(1, value.size() - 2) + __ROCKSSEP;
    } else {
      this->value_ += value + __ROCKSSEP;
    }
  }
}

rocksdb::Slice RocksdbRecord::Key() const { return rocksdb::Slice(this->key_); }
rocksdb::Slice RocksdbRecord::Value() const {
  return rocksdb::Slice(this->value_);
}

/*
 * RocksdbIndexRecord
 */
// Construct when reading from rocksdb.
RocksdbIndexRecord::RocksdbIndexRecord(const rocksdb::Slice &slice)
    : key_(slice.ToString()) {}

// When handling SQL statements.
RocksdbIndexRecord::RocksdbIndexRecord(const rocksdb::Slice &index_value,
                                       const std::string &shard_name,
                                       const rocksdb::Slice &pk)
    : key_() {
  this->key_ = index_value.ToString() + __ROCKSSEP + shard_name + __ROCKSSEP +
               pk.ToString() + __ROCKSSEP;
}

// For writing/encoding.
rocksdb::Slice RocksdbIndexRecord::Key() const {
  return rocksdb::Slice(this->key_);
}

// For reading/decoding.
rocksdb::Slice RocksdbIndexRecord::ExtractShardName() const {
  return ExtractColumn(this->key_, 1);
}
rocksdb::Slice RocksdbIndexRecord::ExtractPK() const {
  return ExtractColumn(this->key_, 2);
}

}  // namespace sql
}  // namespace pelton
