#include "pelton/sql/rocksdb/table.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

namespace {
template <typename T>
bool IsPrefix(const std::vector<T> &left, const std::vector<T> &right) {
  if (left.size() > right.size()) {
    return false;
  }
  for (size_t i = 0; i < left.size(); i++) {
    if (left.at(i) != right.at(i)) {
      return false;
    }
  }
  return true;
}
}  // namespace

// Constructor.
RocksdbTable::RocksdbTable(rocksdb::DB *db, const std::string &table_name,
                           const dataflow::SchemaRef &schema)
    : db_(db),
      table_name_(table_name),
      schema_(schema),
      pk_column_(),
      unique_columns_(),
      handle_(),
      pk_index_(db, table_name),
      indices_() {
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
void RocksdbTable::CreateIndex(const std::vector<size_t> &cols) {
  CHECK_GT(cols.size(), 0u) << "Index has no columns";
  if (std::find(cols.begin(), cols.end(), this->pk_column_) != cols.end()) {
    LOG(FATAL) << "Index includes PK";
  }
  if (cols.size() > 1) {
    for (size_t unique : this->unique_columns_) {
      if (std::find(cols.begin(), cols.end(), unique) != cols.end()) {
        LOG(FATAL) << "Composite index includes unique column";
      }
    }
  }
  for (const RocksdbIndex &index : this->indices_) {
    if (index.GetColumns() == cols) {
      return;
    }
    if (IsPrefix(cols, index.GetColumns())) {
      LOG(WARNING) << "Index is a prefix of an existing index";
    }
    if (IsPrefix(index.GetColumns(), cols)) {
      LOG(WARNING) << "An existing index is a prefix of index being created";
    }
  }
  this->indices_.emplace_back(this->db_, this->table_name_, cols);
}

// Index updating.
void RocksdbTable::IndexAdd(const rocksdb::Slice &shard,
                            const RocksdbSequence &row, bool update_pk) {
  std::vector<rocksdb::Slice> split = row.Split();
  rocksdb::Slice pk = split.at(this->pk_column_);

  // Update PK index.
  if (update_pk) {
    this->pk_index_.Add(pk, shard);
  }

  // Update other indices.
  for (RocksdbIndex &index : this->indices_) {
    std::vector<rocksdb::Slice> index_values;
    index_values.reserve(index.GetColumns().size());
    for (size_t col : index.GetColumns()) {
      index_values.push_back(split.at(col));
    }
    index.Add(index_values, shard, pk);
  }
}
void RocksdbTable::IndexDelete(const rocksdb::Slice &shard,
                               const RocksdbSequence &row, bool update_pk) {
  std::vector<rocksdb::Slice> split = row.Split();
  rocksdb::Slice pk = split.at(this->pk_column_);

  // Update PK index.
  if (update_pk) {
    this->pk_index_.Delete(pk, shard);
  }

  // Update other indices.
  for (RocksdbIndex &index : this->indices_) {
    std::vector<rocksdb::Slice> index_values;
    index_values.reserve(index.GetColumns().size());
    for (size_t col : index.GetColumns()) {
      index_values.push_back(split.at(col));
    }
    index.Delete(index_values, shard, pk);
  }
}
void RocksdbTable::IndexUpdate(const rocksdb::Slice &shard,
                               const RocksdbSequence &old,
                               const RocksdbSequence &updated) {
  std::vector<rocksdb::Slice> osplit = old.Split();
  std::vector<rocksdb::Slice> usplit = updated.Split();
  rocksdb::Slice pk = osplit.at(this->pk_column_);
  CHECK(pk == usplit.at(this->pk_column_)) << "Update cannot change PK";

  for (RocksdbIndex &index : this->indices_) {
    bool changed = false;
    std::vector<rocksdb::Slice> ovalues;
    std::vector<rocksdb::Slice> uvalues;
    ovalues.reserve(index.GetColumns().size());
    uvalues.reserve(index.GetColumns().size());
    for (size_t col : index.GetColumns()) {
      ovalues.push_back(osplit.at(col));
      uvalues.push_back(usplit.at(col));
      changed = changed || osplit.at(col) != usplit.at(col);
    }
    if (changed) {
      index.Delete(ovalues, shard, pk);
      index.Add(uvalues, shard, pk);
    }
  }
}

// Query planning (selecting index).
RocksdbTable::IndexChoice RocksdbTable::ChooseIndex(
    sqlast::ValueMapper *value_mapper) const {
  // PK has highest priority.
  if (value_mapper->HasValues(this->pk_column_)) {
    return {IndexChoiceType::PK, false, 0};
  }
  // Unique indices have second highest priority.
  for (size_t column : this->unique_columns_) {
    if (value_mapper->HasValues(column)) {
      for (size_t index = 0; index < this->indices_.size(); index++) {
        const auto &cols = this->indices_.at(index).GetColumns();
        if (cols.size() == 1 && cols.front() == column) {
          return {IndexChoiceType::UNIQUE, false, static_cast<size_t>(index)};
        }
      }
    }
  }
  // Least priorty are regular indices. We try to select the index with the most
  // conditions matched.
  size_t max = 0;
  size_t argmax = 0;
  bool max_complete = false;
  for (size_t i = 0; i < this->indices_.size(); i++) {
    size_t prefix =
        value_mapper->PrefixWithValues(this->indices_.at(i).GetColumns());
    bool complete = (prefix == this->indices_.at(i).GetColumns().size());
    if (prefix > max) {
      max = prefix;
      argmax = i;
      max_complete = complete;
    } else if (prefix == max && !max_complete && complete) {
      max = prefix;
      argmax = i;
      max_complete = true;
    }
  }
  if (max > 0) {
    return {IndexChoiceType::REGULAR, !max_complete, argmax};
  }
  return {IndexChoiceType::SCAN, false, 0};
}
RocksdbTable::IndexChoice RocksdbTable::ChooseIndex(size_t column_index) const {
  sqlast::ValueMapper vm(this->schema_);
  vm.AddValues(column_index, {});
  return this->ChooseIndex(&vm);
}

// Index Lookup.
std::optional<IndexSet> RocksdbTable::IndexLookup(
    sqlast::ValueMapper *value_mapper) const {
  IndexChoice plan = this->ChooseIndex(value_mapper);
  switch (plan.type) {
    // By primary key index.
    case IndexChoiceType::PK: {
      std::vector<sqlast::Value> vals =
          value_mapper->ReleaseValues(this->pk_column_);
      std::vector<std::string> encoded =
          EncodeValues(this->schema_.TypeOf(this->pk_column_), vals);
      return this->pk_index_.Get(std::move(encoded));
    }
    // By unique or regular index.
    case IndexChoiceType::UNIQUE:
    case IndexChoiceType::REGULAR: {
      const RocksdbIndex &index = this->indices_.at(plan.idx);
      return index.Get(index.EncodeComposite(value_mapper, this->schema_),
                       plan.prefix);
    }
    // By scan.
    case IndexChoiceType::SCAN:
      return {};
    default:
      LOG(FATAL) << "Unsupported query plan";
  }
}
std::optional<DedupIndexSet> RocksdbTable::IndexLookupDedup(
    sqlast::ValueMapper *value_mapper) const {
  IndexChoice plan = this->ChooseIndex(value_mapper);
  switch (plan.type) {
    // By primary key index.
    case IndexChoiceType::PK: {
      std::vector<sqlast::Value> vals =
          value_mapper->ReleaseValues(this->pk_column_);
      std::vector<std::string> encoded =
          EncodeValues(this->schema_.TypeOf(this->pk_column_), vals);
      return this->pk_index_.GetDedup(std::move(encoded));
    }
    // By unique or regular index.
    case IndexChoiceType::UNIQUE:
    case IndexChoiceType::REGULAR: {
      const RocksdbIndex &index = this->indices_.at(plan.idx);
      return index.GetDedup(index.EncodeComposite(value_mapper, this->schema_),
                            plan.prefix);
    }
    // By scan.
    case IndexChoiceType::SCAN:
      return {};
    default:
      LOG(FATAL) << "Unsupported query plan";
  }
}

// Check if a record with given PK exists.
bool RocksdbTable::Exists(const rocksdb::Slice &pk_value) const {
  return this->pk_index_.Get({pk_value.ToString()}).size() > 0;
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
  if (keys.size() == 0) {
    return {};
  }

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
