#ifndef PELTON_SQL_ROCKSDB_ENCODE_H__
#define PELTON_SQL_ROCKSDB_ENCODE_H__

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"
#include "rocksdb/slice.h"

#define __ROCKSSEP static_cast<char>(30)
#define __ROCKSNULL static_cast<char>(0)

namespace pelton {
namespace sql {

// Helpers.
std::string EncodeValue(sqlast::ColumnDefinition::Type type,
                        const std::string &value);

rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col);

class RocksdbRecord {
 public:
  // Construct a record when reading from rocksdb.
  RocksdbRecord(const rocksdb::Slice &key, const rocksdb::Slice &value)
      : key_(key.data(), key.size()), value_(value.data(), value.size()) {}

  RocksdbRecord(std::string &&key, std::string &&value)
      : key_(std::move(key)), value_(std::move(value)) {}

  // Construct a record when handling SQL statements.
  static RocksdbRecord FromInsert(const sqlast::Insert &stmt,
                                  const dataflow::SchemaRef &schema,
                                  const std::string &shard_name);

  // For writing/encoding.
  rocksdb::Slice Key() const { return rocksdb::Slice(this->key_); }
  rocksdb::Slice Value() const { return rocksdb::Slice(this->value_); }
  std::string &&ReleaseKey() { return std::move(this->key_); }
  std::string &&ReleaseValue() { return std::move(this->value_); }

  // For reading/decoding.
  dataflow::Record DecodeRecord(const dataflow::SchemaRef &schema) const;
  rocksdb::Slice GetShard() const { return ExtractColumn(this->key_, 0); }
  rocksdb::Slice GetPK() const { return ExtractColumn(this->key_, 1); }

  // For updating.
  RocksdbRecord Update(const std::unordered_map<size_t, std::string> &update,
                       const dataflow::SchemaRef &schema,
                       const std::string &shard_name) const;

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
