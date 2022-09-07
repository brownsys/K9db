#include "pelton/sql/rocksdb/table.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

namespace {

std::vector<rocksdb::Slice> Transform(const std::vector<std::string> &v) {
  std::vector<rocksdb::Slice> res;
  res.reserve(v.size());
  for (const std::string &s : v) {
    res.emplace_back(s);
  }
  return res;
}

}  // namespace

// Constructor.
RocksdbTable::RocksdbTable(rocksdb::DB *db, const std::string &table_name,
                           const dataflow::SchemaRef &schema)
    : db_(db),
      table_name_(table_name),
      schema_(schema),
      pk_column_(),
      handle_(),
      pk_index_(db, table_name),
      indices_(schema.size()) {
  const std::vector<dataflow::ColumnID> &keys = this->schema_.keys();
  CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
  this->pk_column_ = keys.front();

  // Create rocksdb table.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.OptimizeLevelStyleCompaction();
  options.prefix_extractor.reset(new PeltonPrefixTransform());
  options.comparator = PeltonComparator();
  PANIC(this->db_->CreateColumnFamily(options, table_name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Create an index.
void RocksdbTable::CreateIndex(size_t column_index) {
  if (column_index != this->pk_column_ &&
      !this->indices_.at(column_index).has_value()) {
    this->indices_.at(column_index) =
        RocksdbIndex(this->db_, this->table_name_, column_index);
  }
}

// Index updating.
void RocksdbTable::IndexAdd(const rocksdb::Slice &shard,
                            const RocksdbSequence &row) {
  rocksdb::Slice pk = row.At(this->pk_column_);

  // Update PK index.
  this->pk_index_.Add(pk, shard);

  // Update other indices.
  size_t i = 0;
  for (rocksdb::Slice value : row) {
    std::optional<RocksdbIndex> &index = this->indices_.at(i++);
    if (index.has_value()) {
      index->Add(value, shard, pk);
    }
  }
}
void RocksdbTable::IndexDelete(const rocksdb::Slice &shard,
                               const RocksdbSequence &row) {
  rocksdb::Slice pk = row.At(this->pk_column_);

  // Update PK index.
  this->pk_index_.Delete(pk, shard);

  // Update other indices.
  size_t i = 0;
  for (rocksdb::Slice value : row) {
    std::optional<RocksdbIndex> &index = this->indices_.at(i++);
    if (index.has_value()) {
      index->Delete(value, shard, pk);
    }
  }
}

// Index Lookup.
std::optional<KeySet> RocksdbTable::IndexLookup(
    sqlast::ValueMapper *value_mapper) const {
  // Hash / equality determined by PK.
  KeySet result(0, RocksdbSequence::Hash(RocksdbSequence::Slicer(1)),
                RocksdbSequence::Equal(RocksdbSequence::Slicer(1)));

  // Lookup by PK index.
  if (value_mapper->HasBefore(this->pk_column_)) {
    // Lookup primary index.
    std::vector<std::string> vals =
        value_mapper->ReleaseBefore(this->pk_column_);
    EncodeValues(this->schema_.TypeOf(this->pk_column_), &vals);
    PKIndexSet set = this->pk_index_.Get(Transform(vals));

    // Transform to result.
    for (const RocksdbPKIndexRecord &ir : set) {
      RocksdbSequence entry;
      entry.AppendEncoded(ir.GetShard());
      entry.AppendEncoded(ir.GetPK());
      result.insert(std::move(entry));
    }
    return result;
  }

  // Lookup by first available non PK index.
  for (size_t col = 0; col < this->schema_.size(); col++) {
    const std::optional<RocksdbIndex> &index = this->indices_.at(col);
    if (!index.has_value() || !value_mapper->HasBefore(col)) {
      continue;
    }

    // Lookup col index.
    std::vector<std::string> vals = value_mapper->ReleaseBefore(col);
    EncodeValues(this->schema_.TypeOf(col), &vals);
    IndexSet set = index->Get(Transform(vals));

    // Transform to result.
    for (const RocksdbIndexRecord &ir : set) {
      result.emplace(ir.TargetKey());
    }
    return result;
  }

  return {};
}

// Check if a record with given PK exists.
bool RocksdbTable::Exists(const rocksdb::Slice &pk_value) const {
  return this->pk_index_.Get({pk_value}).size() > 0;
}

// Write to database.
void RocksdbTable::Put(const EncryptedKey &key, const EncryptedValue &value) {
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(this->db_->Put(opts, this->handle_.get(), key.Data(), value.Data()));
}

// Get from database.
std::optional<EncryptedValue> RocksdbTable::Get(const EncryptedKey &key) const {
  std::string data;

  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  auto status = this->db_->Get(opts, this->handle_.get(), key.Data(), &data);
  if (status.ok()) {
    return Cipher::FromDB(std::move(data));
  } else if (status.IsNotFound()) {
    return {};
  }
  PANIC(status);
  return {};
}

// MultiGet: faster multi key lookup.
std::vector<std::optional<EncryptedValue>> RocksdbTable::MultiGet(
    const std::vector<EncryptedKey> &keys) const {
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;

  // Make C-arrays for rocksdb MultiGet.
  std::unique_ptr<rocksdb::Slice[]> slices =
      std::make_unique<rocksdb::Slice[]>(keys.size());
  std::unique_ptr<rocksdb::PinnableSlice[]> pins =
      std::make_unique<rocksdb::PinnableSlice[]>(keys.size());
  std::unique_ptr<rocksdb::Status[]> statuses =
      std::make_unique<rocksdb::Status[]>(keys.size());

  // Fill rocksdb C-arrays.
  size_t i = 0;
  for (const EncryptedKey &key : keys) {
    slices[i++] = key.Data();
  }

  // Use MultiGet.
  this->db_->MultiGet(opts, this->handle_.get(), keys.size(), slices.get(),
                      pins.get(), statuses.get());

  // Read values that were found.
  std::vector<std::optional<EncryptedValue>> result;
  for (size_t i = 0; i < keys.size(); i++) {
    const rocksdb::Status &status = statuses[i];
    if (status.ok()) {
      result.emplace_back(Cipher::FromDB(pins[i].ToString()));
    } else if (status.IsNotFound()) {
      result.push_back({});
    } else {
      PANIC(status);
    }
  }

  return result;
}

// Delete from database.
void RocksdbTable::Delete(const EncryptedKey &key) {
  rocksdb::WriteOptions opts;
  opts.sync = true;  // Maybe not necessary? Check MYROCKS.
  PANIC(this->db_->Delete(opts, this->handle_.get(), key.Data()));
}

// Get all data in table.
RocksdbStream RocksdbTable::GetAll() const {
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.verify_checksums = false;
  rocksdb::Iterator *it = this->db_->NewIterator(options, this->handle_.get());
  it->SeekToFirst();
  return RocksdbStream(it);
}

// Get all data in shard.
// Read all data from shard.
RocksdbStream RocksdbTable::GetShard(const EncryptedPrefix &shard_name) const {
  // Read options restrict iterations to records with the same prefix.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.total_order_seek = false;
  options.prefix_same_as_start = true;
  options.verify_checksums = false;
  // Iterator that spans the whole shard prefix.
  rocksdb::Iterator *it = this->db_->NewIterator(options, this->handle_.get());
  it->Seek(shard_name.Data());
  return RocksdbStream(it);
}

// Delete all data in a shard.
// Return deleted data.
std::vector<std::pair<EncryptedKey, EncryptedValue>> RocksdbTable::DeleteShard(
    const EncryptedPrefix &shard_name) {
  // Get the shard data.
  std::vector<std::pair<EncryptedKey, EncryptedValue>> data =
      this->GetShard(shard_name).ToVector();
  // Delete the shard data.
  for (const auto &[key, _] : data) {
    this->Delete(key);
  }
  return data;
}

/*
 * RocksdbStream
 */

using StreamIterator = RocksdbStream::Iterator;

// Construct end iterator.
StreamIterator::Iterator() : it_(nullptr) {}

// Construct iterator given a ready rocksdb iterator (with some seek performed).
StreamIterator::Iterator(rocksdb::Iterator *it) : it_(it) {
  this->EnsureValid();
}

// Next key/value pair.
StreamIterator &StreamIterator::operator++() {
  if (this->it_ != nullptr) {
    this->it_->Next();
    this->EnsureValid();
  }
  return *this;
}

// Equality of underlying iterator.
bool StreamIterator::operator==(const StreamIterator &o) const {
  return this->it_ == o.it_;
}
bool StreamIterator::operator!=(const StreamIterator &o) const {
  return this->it_ != o.it_;
}

// Get current key/value pair.
StreamIterator::value_type StreamIterator::operator*() const {
  return StreamIterator::value_type(
      this->it_->key(), Cipher::FromDB(this->it_->value().ToString()));
}

// When the iterator is consumed, set it to nullptr.
void StreamIterator::EnsureValid() {
  if (!this->it_->Valid()) {
    this->it_ = nullptr;
  }
}

// To an actual vector. Consume this stream.
std::vector<std::pair<EncryptedKey, EncryptedValue>> RocksdbStream::ToVector() {
  std::vector<std::pair<EncryptedKey, EncryptedValue>> result;
  for (; this->it_->Valid(); this->it_->Next()) {
    result.emplace_back(this->it_->key(),
                        Cipher::FromDB(this->it_->value().ToString()));
  }
  return result;
}

}  // namespace sql
}  // namespace pelton
