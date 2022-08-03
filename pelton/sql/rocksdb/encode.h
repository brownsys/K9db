#ifndef PELTON_SQL_ROCKSDB_ENCODE_H__
#define PELTON_SQL_ROCKSDB_ENCODE_H__

#include <string>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "rocksdb/slice.h"

#define __ROCKSSEP static_cast<char>(30)
#define __ROCKSNULL static_cast<char>(0)

namespace pelton {
namespace sql {

class RocksdbRecord {
 public:
  // Construct a record when reading from rocksdb.
  RocksdbRecord(const rocksdb::Slice &key, const rocksdb::Slice &value);

  // Construct a record when handling SQL statements.
  RocksdbRecord(const std::string &shard_name, const std::string &pk,
                const std::vector<std::string> &values);

  // For writing/encoding.
  rocksdb::Slice Key() const;
  rocksdb::Slice Value() const;

 private:
  std::string key_;
  std::string value_;
};

class RocksdbIndexRecord {
 public:
  // Construct when reading from rocksdb.
  explicit RocksdbIndexRecord(const rocksdb::Slice &slice);

  // When handling SQL statements.
  RocksdbIndexRecord(const rocksdb::Slice &index_value,
                     const std::string &shard_name, const rocksdb::Slice &pk);

  // For writing/encoding.
  rocksdb::Slice Key() const;

  // For reading/decoding.
  rocksdb::Slice ExtractShardName() const;
  rocksdb::Slice ExtractPK() const;

 private:
  std::string key_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_ENCODE_H__
