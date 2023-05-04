#include "k9db/sql/rocksdb/table.h"

#include <utility>

#include "glog/logging.h"
#include "k9db/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace k9db {
namespace sql {
namespace rocks {

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

// Rocksdb Options for table column families.
rocksdb::ColumnFamilyOptions RocksdbTable::ColumnFamilyOptions() {
  rocksdb::ColumnFamilyOptions options;
  options.OptimizeLevelStyleCompaction();
  options.prefix_extractor.reset(new K9dbPrefixTransform());
  options.comparator = K9dbComparator();
  return options;
}

// Constructor.
RocksdbTable::RocksdbTable(rocksdb::DB *db, const std::string &table_name,
                           const dataflow::SchemaRef &schema,
                           RocksdbMetadata *metadata)
    : db_(db),
      table_name_(table_name),
      schema_(schema),
      metadata_(metadata),
      pk_column_(),
      unique_columns_(),
      handle_(),
      pk_index_(db, table_name, metadata),
      indices_() {
  const std::vector<dataflow::ColumnID> &keys = this->schema_.keys();
  CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
  this->pk_column_ = keys.front();

  // Has this table been created before and we are reloading as part of
  // restarting k9db?
  std::optional<std::unique_ptr<rocksdb::ColumnFamilyHandle>> handle = {};
  if (metadata != nullptr) {
    handle = metadata->Find(table_name);
  }

  if (!handle.has_value()) {
    // Create rocksdb table.
    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options = ColumnFamilyOptions();
    PANIC(this->db_->CreateColumnFamily(options, table_name, &handle));
    this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
  } else {
    // Table was already created, point to it!
    this->handle_ = std::move(handle.value());
  }
}

// Create an index.
std::string RocksdbTable::CreateIndex(const std::vector<size_t> &cols) {
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
      return index.name();
    }
    if (IsPrefix(cols, index.GetColumns())) {
      LOG(WARNING) << "Index is a prefix of an existing index";
    }
    if (IsPrefix(index.GetColumns(), cols)) {
      LOG(WARNING) << "An existing index is a prefix of index being created";
    }
  }
  this->indices_.emplace_back(this->db_, this->table_name_, cols,
                              this->metadata_);
  return this->indices_.back().name();
}

// Index updating.
void RocksdbTable::IndexAdd(const rocksdb::Slice &shard,
                            const RocksdbSequence &row,
                            RocksdbWriteTransaction *txn, bool update_pk) {
  std::vector<rocksdb::Slice> split = row.Split();
  rocksdb::Slice pk = split.at(this->pk_column_);

  // Update PK index.
  if (update_pk) {
    this->pk_index_.Add(pk, shard, txn);
  }

  // Update other indices.
  for (RocksdbIndex &index : this->indices_) {
    std::vector<rocksdb::Slice> index_values;
    index_values.reserve(index.GetColumns().size());
    for (size_t col : index.GetColumns()) {
      index_values.push_back(split.at(col));
    }
    index.Add(index_values, shard, pk, txn);
  }
}
void RocksdbTable::IndexDelete(const rocksdb::Slice &shard,
                               const RocksdbSequence &row,
                               RocksdbWriteTransaction *txn, bool update_pk) {
  std::vector<rocksdb::Slice> split = row.Split();
  rocksdb::Slice pk = split.at(this->pk_column_);

  // Update PK index.
  if (update_pk) {
    this->pk_index_.Delete(pk, shard, txn);
  }

  // Update other indices.
  for (RocksdbIndex &index : this->indices_) {
    std::vector<rocksdb::Slice> index_values;
    index_values.reserve(index.GetColumns().size());
    for (size_t col : index.GetColumns()) {
      index_values.push_back(split.at(col));
    }
    index.Delete(index_values, shard, pk, txn);
  }
}
void RocksdbTable::IndexUpdate(const rocksdb::Slice &shard,
                               const RocksdbSequence &old,
                               const RocksdbSequence &updated,
                               RocksdbWriteTransaction *txn) {
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
      index.Delete(ovalues, shard, pk, txn);
      index.Add(uvalues, shard, pk, txn);
    }
  }
}

// Query planning (selecting index).
RocksdbPlan RocksdbTable::ChooseIndex(
    const sqlast::ValueMapper *value_mapper) const {
  RocksdbPlan plan(this->table_name_, this->schema_);
  // PK has highest priority.
  if (value_mapper->HasValues(this->pk_column_)) {
    plan.MakePK({this->pk_column_});
    return plan;
  }
  // Unique indices have second highest priority.
  for (size_t column : this->unique_columns_) {
    if (value_mapper->HasValues(column)) {
      for (size_t index = 0; index < this->indices_.size(); index++) {
        const auto &cols = this->indices_.at(index).GetColumns();
        if (cols.size() == 1 && cols.front() == column) {
          plan.MakeIndex(true, {column}, index);
          return plan;
        }
      }
    }
  }
  // Least priorty are regular indices. We try to select the index with the most
  // conditions matched.
  size_t max = 0;
  size_t argmax = 0;
  for (size_t i = 0; i < this->indices_.size(); i++) {
    const std::vector<size_t> &index_cols = this->indices_.at(i).GetColumns();
    if (index_cols.size() > max && value_mapper->HasValues(index_cols)) {
      max = index_cols.size();
      argmax = i;
    }
  }
  if (max > 0) {
    plan.MakeIndex(false, this->indices_.at(argmax).GetColumns(), argmax);
    return plan;
  }
  plan.MakeScan();
  return plan;
}

RocksdbPlan RocksdbTable::ChooseIndex(size_t column_index) const {
  sqlast::ValueMapper vm(this->schema_);
  vm.AddValues(column_index, {});
  return this->ChooseIndex(&vm);
}

// Index Lookup.
std::optional<IndexSet> RocksdbTable::IndexLookup(
    sqlast::ValueMapper *value_mapper, const RocksdbInterface *txn,
    int limit) const {
  RocksdbPlan plan = this->ChooseIndex(value_mapper);
  switch (plan.type()) {
    // By primary key index.
    case RocksdbPlan::IndexChoiceType::PK: {
      std::vector<sqlast::Value> vals =
          value_mapper->ReleaseValues(this->pk_column_);
      std::vector<std::string> encoded =
          EncodeValues(this->schema_.TypeOf(this->pk_column_), vals);
      return this->pk_index_.Get(std::move(encoded), txn);
    }
    // By unique or regular index.
    case RocksdbPlan::IndexChoiceType::UNIQUE:
    case RocksdbPlan::IndexChoiceType::REGULAR: {
      const RocksdbIndex &index = this->indices_.at(plan.idx());
      return index.Get(index.EncodeComposite(value_mapper, this->schema_), txn,
                       limit);
    }
    // By scan.
    case RocksdbPlan::IndexChoiceType::SCAN:
      return {};
    default:
      LOG(FATAL) << "Unsupported query plan";
  }
}
std::optional<DedupIndexSet> RocksdbTable::IndexLookupDedup(
    sqlast::ValueMapper *value_mapper, const RocksdbInterface *txn,
    int limit) const {
  RocksdbPlan plan = this->ChooseIndex(value_mapper);
  switch (plan.type()) {
    // By primary key index.
    case RocksdbPlan::IndexChoiceType::PK: {
      std::vector<sqlast::Value> vals =
          value_mapper->ReleaseValues(this->pk_column_);
      std::vector<std::string> encoded =
          EncodeValues(this->schema_.TypeOf(this->pk_column_), vals);
      return this->pk_index_.GetDedup(std::move(encoded), txn);
    }
    // By unique or regular index.
    case RocksdbPlan::IndexChoiceType::UNIQUE:
    case RocksdbPlan::IndexChoiceType::REGULAR: {
      const RocksdbIndex &index = this->indices_.at(plan.idx());
      return index.GetDedup(index.EncodeComposite(value_mapper, this->schema_),
                            txn, limit);
    }
    // By scan.
    case RocksdbPlan::IndexChoiceType::SCAN:
      return {};
    default:
      LOG(FATAL) << "Unsupported query plan";
  }
}

// Check if a record with given PK exists.
bool RocksdbTable::Exists(const rocksdb::Slice &pk_value,
                          const RocksdbWriteTransaction *txn) const {
  return this->pk_index_.Exists(pk_value, txn);
}

// Write to database.
void RocksdbTable::Put(const EncryptedKey &key, const EncryptedValue &value,
                       RocksdbWriteTransaction *txn) {
  txn->Put(this->handle_.get(), key.Data(), value.Data());
}

// Delete from database.
void RocksdbTable::Delete(const EncryptedKey &key,
                          RocksdbWriteTransaction *txn) {
  txn->Delete(this->handle_.get(), key.Data());
}

// Get from database.
std::optional<EncryptedValue> RocksdbTable::Get(
    const EncryptedKey &key, const RocksdbInterface *txn) const {
  std::optional<std::string> data = txn->Get(this->handle_.get(), key.Data());
  if (data.has_value()) {
    return Cipher::FromDB(std::move(data.value()));
  }
  return {};
}

// MultiGet: faster multi key lookup.
std::vector<std::optional<EncryptedValue>> RocksdbTable::MultiGet(
    const std::vector<EncryptedKey> &keys, const RocksdbInterface *txn) const {
  if (keys.size() == 0) {
    return {};
  }

  // Extract slices.
  std::vector<rocksdb::Slice> slices;
  slices.reserve(keys.size());
  for (const EncryptedKey &key : keys) {
    slices.push_back(key.Data());
  }

  // Use MultiGet.
  std::vector<std::optional<std::string>> data =
      txn->MultiGet(this->handle_.get(), slices);

  // Transform to Ciphers.
  std::vector<std::optional<EncryptedValue>> result;
  result.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    std::optional<std::string> opt = data.at(i);
    if (opt.has_value()) {
      result.emplace_back(Cipher::FromDB(std::move(opt.value())));
    } else {
      result.emplace_back();
    }
  }

  return result;
}

// Get all data in table.
RocksdbStream RocksdbTable::GetAll(const RocksdbInterface *txn) const {
  std::unique_ptr<rocksdb::Iterator> it =
      txn->Iterate(this->handle_.get(), false);
  it->SeekToFirst();
  return RocksdbStream(std::move(it));
}

// Get all data in shard.
// Read all data from shard.
RocksdbStream RocksdbTable::GetShard(const EncryptedPrefix &shard_name,
                                     const RocksdbInterface *txn) const {
  // Iterator that spans the whole shard prefix.
  std::unique_ptr<rocksdb::Iterator> it =
      txn->Iterate(this->handle_.get(), true);
  it->Seek(shard_name.Data());
  return RocksdbStream(std::move(it));
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
