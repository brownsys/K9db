#ifndef K9DB_SQL_ROCKSDB_ENCODE_H__
#define K9DB_SQL_ROCKSDB_ENCODE_H__

#include <functional>
#include <iterator>
#include <string>
// NOLINTNEXTLINE
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/shard_name.h"
#include "rocksdb/slice.h"

#define __ROCKSSEP static_cast<char>(30)
#define __ROCKSNULL static_cast<char>(0)
#define __ROCKSCOMP static_cast<char>(1)

namespace k9db {
namespace sql {
namespace rocks {

rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col);

std::string EncodeValue(sqlast::ColumnDefinition::Type type,
                        const sqlast::Value &v);

std::vector<std::string> EncodeValues(sqlast::ColumnDefinition::Type type,
                                      const std::vector<sqlast::Value> &vals);

class RocksdbSequence {
 public:
  // Constructing a record while reading from db.
  explicit RocksdbSequence(const rocksdb::Slice &slice)
      : data_(slice.data(), slice.size()) {}
  explicit RocksdbSequence(std::string &&str) : data_(std::move(str)) {}

  // Constructing a record when handling an SQL statement (to write into DB).
  RocksdbSequence() = default;

  // From a given record.
  static RocksdbSequence FromRecord(const dataflow::Record &record);

  // Access underlying data.
  std::string &&Release() { return std::move(this->data_); }
  rocksdb::Slice Data() const { return rocksdb::Slice(this->data_); }

  // Inserting into sequence.
  void Append(const sqlast::Value &val);
  void Append(const util::ShardName &shard_name);
  void AppendEncoded(const rocksdb::Slice &slice);
  void AppendComposite(const std::vector<rocksdb::Slice> &slices);

  // Getting from the sequence.
  // Does not include trailing __ROCKSSEP.
  rocksdb::Slice At(size_t pos) const;

  // Includes trailing __ROCKSSEP, as well as internal __ROCKSSEP.
  rocksdb::Slice Slice(size_t spos, size_t count) const;

  // Split components into vector.
  std::vector<rocksdb::Slice> Split() const;

  // Equality is over underlying data.
  bool operator==(const RocksdbSequence &o) const {
    return this->data_ == o.data_;
  }
  bool operator!=(const RocksdbSequence &o) const {
    return this->data_ != o.data_;
  }

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
    using difference_type = int64_t;
    using value_type = rocksdb::Slice;
    using reference = rocksdb::Slice &;
    using pointer = rocksdb::Slice *;
    using iterator_category = std::input_iterator_tag;

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
  dataflow::Record DecodeRecord(const dataflow::SchemaRef &schema,
                                bool positive) const;

 private:
  std::string data_;
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
                                  const util::ShardName &shard_name);

  // For writing/encoding.
  RocksdbSequence &Key() { return this->key_; }
  RocksdbSequence &Value() { return this->value_; }
  const RocksdbSequence &Key() const { return this->key_; }
  const RocksdbSequence &Value() const { return this->value_; }

  // For reading/decoding.
  rocksdb::Slice GetShard() const { return this->key_.At(0); }
  rocksdb::Slice GetPK() const { return this->key_.At(1); }

 private:
  RocksdbSequence key_;
  RocksdbSequence value_;
};

// The output/lookup records returned by both RocksdbIndex and RocksdbPKIndex.
class RocksdbIndexRecord {
 public:
  // Construct when reading from rocksdb.
  explicit RocksdbIndexRecord(const rocksdb::Slice &slice) : data_(slice) {}
  explicit RocksdbIndexRecord(RocksdbSequence &&data)
      : data_(std::move(data)) {}

  // For writing/encoding.
  const RocksdbSequence &Sequence() const { return this->data_; }
  rocksdb::Slice Data() const { return this->data_.Data(); }
  RocksdbSequence &&Release() { return std::move(this->data_); }

  // For reading/decoding.
  rocksdb::Slice GetShard() const;
  rocksdb::Slice GetPK() const;

 private:
  RocksdbSequence data_;

  // For use in hashing based containers.
  enum class Target { PK, SHARD_AND_PK };
  template <Target T>
  struct EqualT {
    // Equal function.
    bool operator()(const RocksdbIndexRecord &l,
                    const RocksdbIndexRecord &r) const {
      if constexpr (T == Target::PK) {
        return l.GetPK() == r.GetPK();
      } else {
        return l.Data() == r.Data();
      }
    }
  };
  template <Target T>
  struct HashT {
    size_t operator()(const RocksdbIndexRecord &o) const {
      rocksdb::Slice slice;
      if constexpr (T == Target::PK) {
        slice = o.GetPK();
      } else {
        slice = o.Data();
      }
      return this->hash(std::string_view(slice.data(), slice.size()));
    }
    std::hash<std::string_view> hash;
  };

 public:
  // Aliases.
  using DedupHash = HashT<Target::PK>;
  using DedupEqual = EqualT<Target::PK>;
  using Hash = HashT<Target::SHARD_AND_PK>;
  using Equal = EqualT<Target::SHARD_AND_PK>;
};

// The internal record format physically stored inside RocksdbIndex.
class RocksdbIndexInternalRecord {
 public:
  // Construct when reading from rocksdb.
  explicit RocksdbIndexInternalRecord(const rocksdb::Slice &slice)
      : data_(slice) {}
  explicit RocksdbIndexInternalRecord(std::string &&str)
      : data_(std::move(str)) {}

  // When handling SQL statements.
  // Vector because index can be composite.
  RocksdbIndexInternalRecord(const std::vector<rocksdb::Slice> &index_values,
                             const rocksdb::Slice &shard_name,
                             const rocksdb::Slice &pk);

  // For writing/encoding.
  const RocksdbSequence &Sequence() const { return this->data_; }
  rocksdb::Slice Data() const { return this->data_.Data(); }
  RocksdbSequence &&Release() { return std::move(this->data_); }

  // For reading/decoding.
  rocksdb::Slice GetShard() const;
  rocksdb::Slice GetPK() const;

  // For looking up records corresponding to index entry.
  RocksdbIndexRecord TargetKey() const;
  bool HasPrefix(const rocksdb::Slice &slice) const;

 private:
  RocksdbSequence data_;
};

// The internal record format physically stored inside RocksdbPKIndex.
class RocksdbPKIndexInternalRecord {
 public:
  // Construct the first time we are inserting.
  RocksdbPKIndexInternalRecord() = default;

  // Construct when reading from rocksdb.
  explicit RocksdbPKIndexInternalRecord(const rocksdb::Slice &slice)
      : data_(slice) {}
  explicit RocksdbPKIndexInternalRecord(std::string &&str)
      : data_(std::move(str)) {}

  // Check if empty.
  bool Empty() const { return this->data_.Data().size() == 0; }

  // Appends a new shard.
  void AppendNewShard(const rocksdb::Slice &shard);
  void RemoveShard(const rocksdb::Slice &shard);

  // For writing/encoding.
  const RocksdbSequence &Sequence() const { return this->data_; }
  rocksdb::Slice Data() const { return this->data_.Data(); }
  RocksdbSequence &&Release() { return std::move(this->data_); }

  // Iterator over all the shards in this record.
  RocksdbSequence::Iterator begin() const { return this->data_.begin(); }
  RocksdbSequence::Iterator end() const { return this->data_.end(); }

 private:
  RocksdbSequence data_;
};

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_ENCODE_H__
