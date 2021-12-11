#ifndef PELTON_SQL_CONNECTIONS_ROCKSDB_INDEX_H__
#define PELTON_SQL_CONNECTIONS_ROCKSDB_INDEX_H__

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/db.h"

namespace pelton {
namespace sql {

class RocksdbIndex {
 public:
  RocksdbIndex(rocksdb::DB *db, const std::string &table_name, size_t column);

  void Add(const rocksdb::Slice &index_value, const rocksdb::Slice &key);
  void Delete(const rocksdb::Slice &index_value, const rocksdb::Slice &key);

  std::vector<std::string> Get(const rocksdb::Slice &index_value);
  std::vector<std::string> Get(const std::vector<rocksdb::Slice> &values);

 private:
  rocksdb::DB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> handle_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTIONS_ROCKSDB_INDEX_H__
