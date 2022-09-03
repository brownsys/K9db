#include "pelton/sql/rocksdb/table.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

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
}

// MultiGet: faster multi key lookup.
std::vector<EncryptedValue> RocksdbTable::MultiGet(
    const std::unordered_set<EncryptedKey> &keys) const {
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
    slices[i] = key.Data();
  }

  // Use MultiGet.
  this->db_->MultiGet(opts, this->handle_.get(), keys.size(), slices.get(),
                      pins.get(), statuses.get());

  // Read values that were found.
  std::vector<EncryptedValue> result;
  for (size_t i = 0; i < keys.size(); i++) {
    const rocksdb::Status &status = statuses[i];
    if (status.ok()) {
      result.push_back(Cipher::FromDB(pins[i].ToString()));
    } else if (!status.IsNotFound()) {
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

LazyVector RocksdbTable::GetAll() const {
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.verify_checksums = false;
  rocksdb::Iterator *it = this->db_->NewIterator(options, this->handle_.get());
  it->SeekToFirst();
  return LazyVector(it);
}

/*
 * LazyVector
 */

// Construct end iterator.
LazyVector::Iterator::Iterator() : it_(nullptr) {}

// Construct iterator given a ready rocksdb iterator (with some seek performed).
LazyVector::Iterator::Iterator(rocksdb::Iterator *it) : it_(it) {
  this->EnsureValid();
}

// Next key/value pair.
LazyVector::Iterator &LazyVector::Iterator::operator++() {
  if (this->it_ != nullptr) {
    this->it_->Next();
    this->EnsureValid();
  }
  return *this;
}

// Equality of underlying iterator.
bool LazyVector::Iterator::operator==(const LazyVector::Iterator &o) const {
  return this->it_ == o.it_;
}
bool LazyVector::Iterator::operator!=(const LazyVector::Iterator &o) const {
  return this->it_ != o.it_;
}

// Get current key/value pair.
LazyVector::Iterator::value_type LazyVector::Iterator::operator*() const {
  return LazyVector::Iterator::value_type(
      this->it_->key(), Cipher::FromDB(this->it_->value().ToString()));
}

// When the iterator is consumed, set it to nullptr.
void LazyVector::Iterator::EnsureValid() {
  if (!this->it_->Valid()) {
    this->it_ = nullptr;
  }
}

}  // namespace sql
}  // namespace pelton
