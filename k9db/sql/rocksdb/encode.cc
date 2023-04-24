#include "k9db/sql/rocksdb/encode.h"

// NOLINTNEXTLINE
#include <charconv>
#include <memory>

#include "glog/logging.h"

namespace k9db {
namespace sql {
namespace rocks {

/*
 * Helpers.
 */
namespace {

rocksdb::Slice ExtractSlice(const rocksdb::Slice &slice, size_t spos,
                            size_t count) {
  const char *data = slice.data();

  // Find the starting index of the target slice.
  size_t start_idx = 0;
  if (spos > 0) {
    for (; start_idx < slice.size(); start_idx++) {
      if (data[start_idx] == __ROCKSSEP) {
        if (--spos == 0) {
          start_idx++;
          break;
        }
      }
    }
  }

  // Empty slice.
  if (count == 0) {
    return rocksdb::Slice(data + start_idx, 0);
  }

  // Find the size of the target slice.
  for (size_t end_idx = start_idx; end_idx < slice.size(); end_idx++) {
    if (data[end_idx] == __ROCKSSEP) {
      if (--count == 0) {
        return rocksdb::Slice(data + start_idx, end_idx - start_idx + 1);
      }
    }
  }

  LOG(FATAL) << "Cannot extract [" << spos << ":" << count << "] from '"
             << slice.ToString() << "'";
}

std::string EncodeValue(const sqlast::Value &val) {
  switch (val.type()) {
    case sqlast::Value::Type::_NULL: {
      std::string result;
      result.push_back(__ROCKSNULL);
      return result;
    }
    case sqlast::Value::Type::INT:
      return std::to_string(val.GetInt());
    case sqlast::Value::Type::UINT:
      return std::to_string(val.GetUInt());
    case sqlast::Value::Type::TEXT:
      return val.GetString();
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
}

}  // namespace

/*
 * Free functions.
 */
rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col) {
  rocksdb::Slice result = ExtractSlice(slice, col, 1);
  return rocksdb::Slice(result.data(), result.size() - 1);
}

std::string EncodeValue(sqlast::ColumnDefinition::Type type,
                        const sqlast::Value &v) {
  CHECK(v.TypeCompatible(type));
  return EncodeValue(v);
}

std::vector<std::string> EncodeValues(sqlast::ColumnDefinition::Type type,
                                      const std::vector<sqlast::Value> &vals) {
  std::vector<std::string> result;
  for (const sqlast::Value &v : vals) {
    CHECK(v.TypeCompatible(type));
    result.push_back(EncodeValue(v));
  }
  return result;
}

/*
 * RocksdbSequence
 */
// Writing to sequence.
void RocksdbSequence::Append(const sqlast::Value &val) {
  this->data_.append(EncodeValue(val));
  this->data_.push_back(__ROCKSSEP);
}
void RocksdbSequence::Append(const util::ShardName &shard_name) {
  this->data_.append(shard_name.AsView());
  this->data_.push_back(__ROCKSSEP);
}
void RocksdbSequence::AppendEncoded(const rocksdb::Slice &slice) {
  this->data_.append(slice.data(), slice.size());
  this->data_.push_back(__ROCKSSEP);
}
void RocksdbSequence::AppendComposite(
    const std::vector<rocksdb::Slice> &slices) {
  for (size_t i = 0; i < slices.size(); i++) {
    this->data_.append(slices.at(i).data(), slices.at(i).size());
    this->data_.push_back(__ROCKSCOMP);
  }
  this->data_.at(this->data_.size() - 1) = __ROCKSSEP;
}

// Reading from sequence.
rocksdb::Slice RocksdbSequence::At(size_t pos) const {
  return ExtractColumn(this->data_, pos);
}
rocksdb::Slice RocksdbSequence::Slice(size_t spos, size_t count) const {
  return ExtractSlice(this->data_, spos, count);
}
std::vector<rocksdb::Slice> RocksdbSequence::Split() const {
  return std::vector<rocksdb::Slice>(this->begin(), this->end());
}

// Iterating over sequence.
RocksdbSequence::Iterator RocksdbSequence::begin() const {
  return RocksdbSequence::Iterator(this->data_.data(), this->data_.size());
}
RocksdbSequence::Iterator RocksdbSequence::end() const {
  return RocksdbSequence::Iterator(this->data_.data() + this->data_.size(), 0);
}

// For reading/decoding.
dataflow::Record RocksdbSequence::DecodeRecord(
    const dataflow::SchemaRef &schema, bool positive) const {
  dataflow::Record record{schema, positive};
  size_t idx = 0;
  for (rocksdb::Slice v : *this) {
    if (v.size() == 1 && *v.data() == __ROCKSNULL) {
      record.SetNull(true, idx++);
      continue;
    }
    switch (schema.TypeOf(idx)) {
      case sqlast::ColumnDefinition::Type::UINT: {
        uint64_t num;
        auto result = std::from_chars(v.data(), v.data() + v.size(), num);
        CHECK(result.ec == std::errc{});
        record.SetUInt(num, idx++);
        break;
      }
      case sqlast::ColumnDefinition::Type::INT: {
        int64_t num;
        auto result = std::from_chars(v.data(), v.data() + v.size(), num);
        CHECK(result.ec == std::errc{});
        record.SetInt(num, idx++);
        break;
      }
      case sqlast::ColumnDefinition::Type::TEXT: {
        record.SetString(std::make_unique<std::string>(v.data(), v.size()),
                         idx++);
        break;
      }
      case sqlast::ColumnDefinition::Type::DATETIME: {
        record.SetDateTime(std::make_unique<std::string>(v.data(), v.size()),
                           idx++);
        break;
      }
      default:
        LOG(FATAL) << "UNREACHABLE";
    }
  }
  return record;
}

// From a record.
RocksdbSequence RocksdbSequence::FromRecord(const dataflow::Record &record) {
  RocksdbSequence encoded;
  for (size_t i = 0; i < record.schema().size(); i++) {
    encoded.Append(record.GetValue(i));
  }
  return encoded;
}

/*
 * RocksdbSequence::Iterator
 */
// Constructor.
RocksdbSequence::Iterator::Iterator(const char *ptr, size_t sz)
    : ptr_(ptr), size_(sz) {
  for (this->next_ = 0; this->next_ < this->size_; this->next_++) {
    if (this->ptr_[this->next_] == __ROCKSSEP) {
      break;
    }
  }
}

// Increment API.
RocksdbSequence::Iterator &RocksdbSequence::Iterator::operator++() {
  if (this->size_ > this->next_) {
    this->ptr_ += this->next_ + 1;
    this->size_ -= this->next_ + 1;
    for (this->next_ = 0; this->next_ < this->size_; this->next_++) {
      if (this->ptr_[this->next_] == __ROCKSSEP) {
        break;
      }
    }
  }
  return *this;
}
RocksdbSequence::Iterator RocksdbSequence::Iterator::operator++(int) {
  Iterator ret_val = *this;
  ++(*this);
  return ret_val;
}

// Equality check.
bool RocksdbSequence::Iterator::operator==(const Iterator &o) const {
  return this->ptr_ == o.ptr_;
}
bool RocksdbSequence::Iterator::operator!=(const Iterator &o) const {
  return this->ptr_ != o.ptr_;
}

// Access.
rocksdb::Slice RocksdbSequence::Iterator::operator*() const {
  return rocksdb::Slice(this->ptr_, this->next_);
}

/*
 * RocksdbRecord
 */
// Construct a record when handling SQL statements.
RocksdbRecord RocksdbRecord::FromInsert(const sqlast::Insert &stmt,
                                        const dataflow::SchemaRef &schema,
                                        const util::ShardName &shard_name) {
  // Encode key.
  RocksdbSequence key;

  // Ensure PK is valid.
  const auto &keys = schema.keys();
  CHECK_EQ(keys.size(), 1u) << "schema has too many keys " << schema;
  int idx = keys.at(0);
  const sqlast::Value &pk_value = stmt.GetValue(schema.NameOf(idx), idx);
  CHECK(!pk_value.IsNull());
  CHECK(pk_value.TypeCompatible(schema.TypeOf(keys.at(0))));

  // Append shardname and PK to key.
  key.Append(shard_name);
  key.Append(pk_value);

  // Encode value.
  RocksdbSequence value;
  for (size_t i = 0; i < schema.size(); i++) {
    const sqlast::Value &val = stmt.GetValue(schema.NameOf(i), i);
    CHECK(val.TypeCompatible(schema.TypeOf(i)));
    value.Append(val);
  }

  return RocksdbRecord(std::move(key), std::move(value));
}

/*
 * RocksdbIndexInternalRecord.
 */
RocksdbIndexInternalRecord::RocksdbIndexInternalRecord(
    const std::vector<rocksdb::Slice> &index_values,
    const rocksdb::Slice &shard_name, const rocksdb::Slice &pk)
    : data_() {
  this->data_.AppendComposite(index_values);
  this->data_.AppendEncoded(shard_name);
  this->data_.AppendEncoded(pk);
}

// For reading/decoding.
rocksdb::Slice RocksdbIndexInternalRecord::GetShard() const {
  return this->data_.At(1);
}
rocksdb::Slice RocksdbIndexInternalRecord::GetPK() const {
  return this->data_.At(2);
}

// For looking up records corresponding to index entry.
RocksdbIndexRecord RocksdbIndexInternalRecord::TargetKey() const {
  return RocksdbIndexRecord(this->data_.Slice(1, 2));
}
bool RocksdbIndexInternalRecord::HasPrefix(const rocksdb::Slice &slice) const {
  rocksdb::Slice data = this->data_.Data();
  if (slice.size() > data.size()) {
    return false;
  }
  return slice == rocksdb::Slice(data.data(), slice.size());
}

/*
 * RocksdbPKIndexInternalRecord.
 */
void RocksdbPKIndexInternalRecord::AppendNewShard(const rocksdb::Slice &shard) {
  for (rocksdb::Slice slice : this->data_) {
    CHECK(shard != slice) << "Duplicate shard for PK";
  }
  this->data_.AppendEncoded(shard);
}
void RocksdbPKIndexInternalRecord::RemoveShard(const rocksdb::Slice &shard) {
  RocksdbSequence updated;
  for (rocksdb::Slice slice : this->data_) {
    if (slice != shard) {
      updated.AppendEncoded(slice);
    }
  }
  this->data_ = std::move(updated);
}

/*
 * RocksdbIndexRecord.
 */
rocksdb::Slice RocksdbIndexRecord::GetShard() const { return data_.At(0); }
rocksdb::Slice RocksdbIndexRecord::GetPK() const { return data_.At(1); }

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
