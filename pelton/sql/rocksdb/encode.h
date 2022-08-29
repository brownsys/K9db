#ifndef PELTON_SQL_ROCKSDB_ENCODE_H__
#define PELTON_SQL_ROCKSDB_ENCODE_H__

#include <iterator>
#include <string>
#include <unordered_map>
#include <utility>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"
#include "rocksdb/slice.h"

#define __ROCKSSEP static_cast<char>(30)
#define __ROCKSNULL static_cast<char>(0)

namespace pelton {
namespace sql {

rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col);

rocksdb::Slice ExtractSlice(const rocksdb::Slice &slice, size_t spos,
                            size_t count);

class RocksdbSequence {
 public:
  // Constructing a record while reading from db.
  explicit RocksdbSequence(const rocksdb::Slice &slice);
  explicit RocksdbSequence(std::string &&str);

  // Constructing a record when handling an SQL statement (to write into DB).
  RocksdbSequence() = default;

  // Access underlying data.
  std::string Release() { return std::move(this->data_); }
  rocksdb::Slice Data() const { return rocksdb::Slice(this->data_); }

  // Inserting into sequence.
  void Append(sqlast::ColumnDefinition::Type type, const rocksdb::Slice &slice);
  void AppendEncoded(const rocksdb::Slice &slice);
  // TODO(babman): need to handle the type of shard name too!
  void AppendShardname(const std::string &shard_name);

  // Replacing inside the sequence.
  void Replace(size_t pos, sqlast::ColumnDefinition::Type type,
               const rocksdb::Slice &slice);

  // Getting from the sequence.
  // Does not include trailing __ROCKSSEP.
  rocksdb::Slice At(size_t pos) const;

  // Includes trailing __ROCKSSEP, as well as internal __ROCKSSEP.
  rocksdb::Slice Slice(size_t spos, size_t count) const;

  // Iterator class.
  class Iterator {
   public:
    Iterator &operator++();
    Iterator operator++(int);

    bool operator==(const Iterator &o) const;
    bool operator!=(const Iterator &o) const;

    // Access current element.
    rocksdb::Slice operator*() const;

    // Iterator traits
    using value_type = rocksdb::Slice;
    using iterator_category = std::output_iterator_tag;

   private:
    Iterator(const char *ptr, size_t sz);

    const char *ptr_;
    size_t size_;
    size_t next_;

    friend RocksdbSequence;
  };

  // Iterator API.
  Iterator begin() const;
  Iterator end() const;

  // For reading/decoding into dataflow.
  dataflow::Record DecodeRecord(const dataflow::SchemaRef &schema) const;

 private:
  std::string data_;

  // Helper: encode an SQL value.
  std::string EncodeValue(sqlast::ColumnDefinition::Type type,
                          const rocksdb::Slice &value);
};

class RocksdbRecord {
 public:
  // Construct a record when reading from rocksdb.
  RocksdbRecord(const rocksdb::Slice &key, const rocksdb::Slice &value)
      : key_(key), value_(value) {}

  RocksdbRecord(std::string &&key, std::string &&value)
      : key_(std::move(key)), value_(std::move(value)) {}

  RocksdbRecord(RocksdbSequence &&key, RocksdbSequence &&value)
      : key_(std::move(key)), value_(std::move(value)) {}

  // Construct a record when handling SQL statements.
  static RocksdbRecord FromInsert(const sqlast::Insert &stmt,
                                  const dataflow::SchemaRef &schema,
                                  const std::string &shard_name);

  // For writing/encoding.
  RocksdbSequence &Key() { return this->key_; }
  RocksdbSequence &Value() { return this->value_; }
  const RocksdbSequence &Key() const { return this->key_; }
  const RocksdbSequence &Value() const { return this->value_; }

  // For reading/decoding.
  rocksdb::Slice GetShard() const { return this->key_.At(0); }
  rocksdb::Slice GetPK() const { return this->key_.At(1); }

  // For updating.
  RocksdbRecord Update(const std::unordered_map<size_t, std::string> &update,
                       const dataflow::SchemaRef &schema,
                       const std::string &shard_name) const;

 private:
  RocksdbSequence key_;
  RocksdbSequence value_;
};

class RocksdbIndexRecord {
 public:
  // Construct when reading from rocksdb.
  explicit RocksdbIndexRecord(const rocksdb::Slice &slice) : data_(slice) {}
  explicit RocksdbIndexRecord(std::string &&str) : data_(std::move(str)) {}

  // When handling SQL statements.
  RocksdbIndexRecord(const rocksdb::Slice &index_value,
                     const rocksdb::Slice &shard_name,
                     const rocksdb::Slice &pk);

  // For writing/encoding.
  const RocksdbSequence &Sequence() const { return this->data_; }
  rocksdb::Slice Data() const { return this->data_.Data(); }

  // For reading/decoding.
  rocksdb::Slice GetShard() const;
  rocksdb::Slice GetPK() const;

  // For looking up records corresponding to index entry.
  rocksdb::Slice TargetKey() const;

  // Allows us to use this type in unordered_set.
  struct TargetHash {
    size_t operator()(const RocksdbIndexRecord &r) const;
  };
  struct TargetEqual {
    bool operator()(const RocksdbIndexRecord &l,
                    const RocksdbIndexRecord &r) const;
  };

 private:
  RocksdbSequence data_;
};

class RocksdbPKIndexRecord {
 public:
  // Construct when reading from rocksdb.
  explicit RocksdbPKIndexRecord(const rocksdb::Slice &slice) : data_(slice) {}
  explicit RocksdbPKIndexRecord(std::string &&str) : data_(std::move(str)) {}

  // When handling SQL statements.
  RocksdbPKIndexRecord(const rocksdb::Slice &pk_value,
                       const rocksdb::Slice &shard_name);

  // For writing/encoding.
  const RocksdbSequence &Sequence() const { return this->data_; }
  rocksdb::Slice Data() const { return this->data_.Data(); }

  // For reading/decoding.
  rocksdb::Slice GetShard() const;

  // Allows us to use this type in unordered_set.
  struct ShardNameHash {
    size_t operator()(const RocksdbPKIndexRecord &r) const;
  };
  struct ShardNameEqual {
    bool operator()(const RocksdbPKIndexRecord &l,
                    const RocksdbPKIndexRecord &r) const;
  };

 private:
  RocksdbSequence data_;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_ENCODE_H__
