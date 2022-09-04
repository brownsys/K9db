#include "pelton/sql/rocksdb/encode.h"

// NOLINTNEXTLINE
#include <charconv>
#include <memory>

#include "glog/logging.h"

namespace pelton {
namespace sql {

/*
 * Helpers
 */
rocksdb::Slice ExtractColumn(const rocksdb::Slice &slice, size_t col) {
  rocksdb::Slice result = ExtractSlice(slice, col, 1);
  return rocksdb::Slice(result.data(), result.size() - 1);
}

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

/*
 * RocksdbSequence
 */
// Writing to sequence.
void RocksdbSequence::Append(sqlast::ColumnDefinition::Type type,
                             const rocksdb::Slice &slice) {
  this->data_.append(this->EncodeValue(type, slice));
  this->data_.push_back(__ROCKSSEP);
}
void RocksdbSequence::AppendEncoded(const rocksdb::Slice &slice) {
  this->data_.append(slice.data(), slice.size());
  this->data_.push_back(__ROCKSSEP);
}

// TODO(babman): need to handle the type of shard name too!
void RocksdbSequence::AppendShardname(const std::string &shard_name) {
  this->data_.append(shard_name);
  this->data_.push_back(__ROCKSSEP);
}

void RocksdbSequence::Replace(size_t pos, sqlast::ColumnDefinition::Type type,
                              const rocksdb::Slice &slice) {
  // Encode value.
  std::string value = this->EncodeValue(type, slice);
  // Find replace location.
  rocksdb::Slice replace = this->At(pos);
  size_t start = replace.data() - this->data_.data();
  size_t end = start + replace.size();
  // Create new data with replaced value.
  std::string data;
  data.reserve(start + (this->data_.size() - end) + value.size());
  data.append(this->data_.data(), start);
  data.append(value.data(), value.size());
  data.append(this->data_.data() + end, this->data_.size() - end);
  this->data_ = std::move(data);
}

// Reading from sequence.
rocksdb::Slice RocksdbSequence::At(size_t pos) const {
  return ExtractColumn(this->data_, pos);
}
rocksdb::Slice RocksdbSequence::Slice(size_t spos, size_t count) const {
  return ExtractSlice(this->data_, spos, count);
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
    const dataflow::SchemaRef &schema) const {
  dataflow::Record record{schema, false};
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

// Helper: encoding an SQL value.
std::string RocksdbSequence::EncodeValue(sqlast::ColumnDefinition::Type type,
                                         const rocksdb::Slice &value) {
  if (value == rocksdb::Slice("NULL", 4)) {
    std::string result;
    result.push_back(__ROCKSNULL);
    return result;
  }
  switch (type) {
    case sqlast::ColumnDefinition::Type::INT:
    case sqlast::ColumnDefinition::Type::UINT:
      return std::string(value.data(), value.size());
    case sqlast::ColumnDefinition::Type::TEXT:
      return std::string(value.data() + 1, value.size() - 2);
    case sqlast::ColumnDefinition::Type::DATETIME:
      if (value.size() > 0) {
        if (value.data()[0] == '\'' || value.data()[0] == '"') {
          return std::string(value.data() + 1, value.size() - 2);
        }
      }
      return std::string(value.data(), value.size());
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
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
 * RocksdbSequence::{Slicer, Hash, Equal}
 */
RocksdbSequence::Slicer::Slicer() : start_(0), count_(0) {}
RocksdbSequence::Slicer::Slicer(size_t pos) : start_(pos), count_(1) {}
RocksdbSequence::Slicer::Slicer(size_t start, size_t count)
    : start_(start), count_(count) {
  CHECK_GE(count, 1u);
}

std::string_view RocksdbSequence::Slicer::Slice(
    const RocksdbSequence &o) const {
  rocksdb::Slice slice;
  if (this->count_ == 0) {  // Everything.
    slice = o.Data();
  } else if (this->count_ == 1) {  // Single entry.
    slice = o.At(this->start_);
  } else {  // A slice.
    slice = o.Slice(this->start_, this->count_);
  }
  return std::string_view(slice.data(), slice.size());
}

size_t RocksdbSequence::Hash::operator()(const RocksdbSequence &o) const {
  return this->hash_(this->slicer_.Slice(o));
}

bool RocksdbSequence::Equal::operator()(const RocksdbSequence &l,
                                        const RocksdbSequence &r) const {
  return this->slicer_.Slice(l) == this->slicer_.Slice(r);
}

/*
 * RocksdbRecord
 */
// Construct a record when handling SQL statements.
RocksdbRecord RocksdbRecord::FromInsert(const sqlast::Insert &stmt,
                                        const dataflow::SchemaRef &schema,
                                        const std::string &shard_name) {
  // Encode key.
  RocksdbSequence key;

  // Ensure PK is valid.
  const auto &keys = schema.keys();
  CHECK_EQ(keys.size(), 1u) << "schema has too many keys " << schema;
  int idx = keys.at(0);
  if (stmt.HasColumns()) {
    idx = stmt.GetColumnIndex(schema.NameOf(idx));
    CHECK_GE(idx, 0) << "Insert does not specify PK";
  }
  const std::string &pk_value = stmt.GetValue(idx);
  CHECK_NE(pk_value, "NULL");

  // Append shardname and PK to key.
  key.AppendShardname(shard_name);
  key.Append(schema.TypeOf(keys.at(0)), pk_value);

  // Encode value.
  RocksdbSequence value;
  for (size_t i = 0; i < schema.size(); i++) {
    int idx = i;
    if (stmt.HasColumns()) {
      idx = stmt.GetColumnIndex(schema.NameOf(i));
      CHECK_GE(idx, 0) << "Insert does not specify " << schema.NameOf(i);
    }
    const std::string &val = stmt.GetValue(idx);
    value.Append(schema.TypeOf(i), val);
  }

  return RocksdbRecord(std::move(key), std::move(value));
}

// For updating.
RocksdbRecord RocksdbRecord::Update(
    const std::unordered_map<size_t, std::string> &update,
    const dataflow::SchemaRef &schema, const std::string &shard_name) const {
  // Encode key.
  RocksdbSequence key;
  key.AppendShardname(shard_name);

  // Ensure PK is valid.
  const auto &keys = schema.keys();
  CHECK_EQ(keys.size(), 1u) << "schema has too many keys " << schema;
  size_t pk_idx = keys.at(0);
  if (update.count(pk_idx) == 1) {
    CHECK_NE(update.at(pk_idx), "NULL");
    key.Append(schema.TypeOf(pk_idx), rocksdb::Slice(update.at(pk_idx)));
  } else {
    key.AppendEncoded(this->GetPK());
  }

  // Encode Value.
  RocksdbSequence value;
  size_t idx = 0;
  for (rocksdb::Slice v : this->Value()) {
    if (update.count(idx) == 1) {
      value.Append(schema.TypeOf(idx), rocksdb::Slice(update.at(idx)));
    } else {
      value.AppendEncoded(v);
    }
    idx++;
  }

  return RocksdbRecord(std::move(key), std::move(value));
}

/*
 * RocksdbIndexRecord
 */
// When handling SQL statements.
RocksdbIndexRecord::RocksdbIndexRecord(const rocksdb::Slice &index_value,
                                       const rocksdb::Slice &shard_name,
                                       const rocksdb::Slice &pk)
    : data_() {
  this->data_.AppendEncoded(index_value);
  this->data_.AppendEncoded(shard_name);
  this->data_.AppendEncoded(pk);
}

// For reading/decoding.
rocksdb::Slice RocksdbIndexRecord::GetShard() const {
  return this->data_.At(1);
}
rocksdb::Slice RocksdbIndexRecord::GetPK() const { return this->data_.At(2); }
// For looking up records corresponding to index entry.
rocksdb::Slice RocksdbIndexRecord::TargetKey() const {
  return this->data_.Slice(1, 2);
}

/*
 * RocksdbIndexRecord::{TargetHash, TargetEqual}
 */
RocksdbIndexRecord::TargetHash::TargetHash()
    : hash_(RocksdbSequence::Slicer(1, 2)) {}

size_t RocksdbIndexRecord::TargetHash::operator()(
    const RocksdbIndexRecord &r) const {
  return this->hash_(r.Sequence());
}

RocksdbIndexRecord::TargetEqual::TargetEqual()
    : equal_(RocksdbSequence::Slicer(1, 2)) {}

bool RocksdbIndexRecord::TargetEqual::operator()(
    const RocksdbIndexRecord &l, const RocksdbIndexRecord &r) const {
  return this->equal_(l.Sequence(), r.Sequence());
}

/*
 * RocksdbPKIndexRecord
 */
RocksdbPKIndexRecord::RocksdbPKIndexRecord(const rocksdb::Slice &pk_value,
                                           const rocksdb::Slice &shard_name)
    : data_() {
  this->data_.AppendEncoded(pk_value);
  this->data_.AppendEncoded(shard_name);
}

// For reading/decoding.
rocksdb::Slice RocksdbPKIndexRecord::GetPK() const { return this->data_.At(0); }
rocksdb::Slice RocksdbPKIndexRecord::GetShard() const {
  return this->data_.At(1);
}

/*
 * RocksdbPKIndexRecord::{ShardNameHash, ShardNameEqual}
 */
RocksdbPKIndexRecord::ShardNameHash::ShardNameHash()
    : hash_(RocksdbSequence::Slicer(1)) {}

size_t RocksdbPKIndexRecord::ShardNameHash::operator()(
    const RocksdbPKIndexRecord &r) const {
  return this->hash_(r.Sequence());
}

RocksdbPKIndexRecord::ShardNameEqual::ShardNameEqual()
    : equal_(RocksdbSequence::Slicer(1)) {}

bool RocksdbPKIndexRecord::ShardNameEqual::operator()(
    const RocksdbPKIndexRecord &l, const RocksdbPKIndexRecord &r) const {
  return this->equal_(l.Sequence(), r.Sequence());
}

}  // namespace sql
}  // namespace pelton
