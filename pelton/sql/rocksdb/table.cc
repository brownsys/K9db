#include "pelton/sql/rocksdb/table.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace pelton {
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

// Constructor.
RocksdbTable::RocksdbTable(rocksdb::DB *db, const std::string &table_name,
                           const dataflow::SchemaRef &schema)
    : db_(db),
      table_name_(table_name),
      schema_(schema),
      pk_column_(),
      unique_columns_(),
      handle_(db, table_name),
#ifndef PELTON_PHYSICAL_SEPARATION
      pk_index_(db, table_name),
#endif  // PELTON_PHYSICAL_SEPARATION
      indices_() {
  const std::vector<dataflow::ColumnID> &keys = this->schema_.keys();
  CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
  this->pk_column_ = keys.front();
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
                            const RocksdbSequence &row, RocksdbTransaction *txn,
                            bool update_pk) {
#ifndef PELTON_PHYSICAL_SEPARATION
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
#endif  // PELTON_PHYSICAL_SEPARATION
}
void RocksdbTable::IndexDelete(const rocksdb::Slice &shard,
                               const RocksdbSequence &row,
                               RocksdbTransaction *txn, bool update_pk) {
#ifndef PELTON_PHYSICAL_SEPARATION
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
#endif  // PELTON_PHYSICAL_SEPARATION
}
void RocksdbTable::IndexUpdate(const rocksdb::Slice &shard,
                               const RocksdbSequence &old,
                               const RocksdbSequence &updated,
                               RocksdbTransaction *txn) {
#ifndef PELTON_PHYSICAL_SEPARATION
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
#endif  // PELTON_PHYSICAL_SEPARATION
}

// Query planning (selecting index).
RocksdbTable::IndexChoice RocksdbTable::ChooseIndex(
    sqlast::ValueMapper *value_mapper) const {
#ifdef PELTON_PHYSICAL_SEPARATION
  return {IndexChoiceType::SCAN, 0};
#else
  // PK has highest priority.
  if (value_mapper->HasValues(this->pk_column_)) {
    return {IndexChoiceType::PK, 0};
  }
  // Unique indices have second highest priority.
  for (size_t column : this->unique_columns_) {
    if (value_mapper->HasValues(column)) {
      for (size_t index = 0; index < this->indices_.size(); index++) {
        const auto &cols = this->indices_.at(index).GetColumns();
        if (cols.size() == 1 && cols.front() == column) {
          return {IndexChoiceType::UNIQUE, static_cast<size_t>(index)};
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
    return {IndexChoiceType::REGULAR, argmax};
  }
  return {IndexChoiceType::SCAN, 0};
#endif  // PELTON_PHYSICAL_SEPARATION
}
RocksdbTable::IndexChoice RocksdbTable::ChooseIndex(size_t column_index) const {
  sqlast::ValueMapper vm(this->schema_);
  vm.AddValues(column_index, {});
  return this->ChooseIndex(&vm);
}

// Index Lookup.
std::optional<IndexSet> RocksdbTable::IndexLookup(
    sqlast::ValueMapper *value_mapper, const RocksdbTransaction *txn) const {
#ifdef PELTON_PHYSICAL_SEPARATION
    return {};
#else
  IndexChoice plan = this->ChooseIndex(value_mapper);
  switch (plan.type) {
    // By primary key index.
    case IndexChoiceType::PK: {
      std::vector<sqlast::Value> vals =
          value_mapper->ReleaseValues(this->pk_column_);
      std::vector<std::string> encoded =
          EncodeValues(this->schema_.TypeOf(this->pk_column_), vals);
      return this->pk_index_.Get(std::move(encoded), txn);
    }
    // By unique or regular index.
    case IndexChoiceType::UNIQUE:
    case IndexChoiceType::REGULAR: {
      const RocksdbIndex &index = this->indices_.at(plan.idx);
      return index.Get(index.EncodeComposite(value_mapper, this->schema_), txn);
    }
    // By scan.
    case IndexChoiceType::SCAN:
      return {};
    default:
      LOG(FATAL) << "Unsupported query plan";
  }
#endif  // PELTON_PHYSICAL_SEPARATION
}
std::optional<DedupIndexSet> RocksdbTable::IndexLookupDedup(
    sqlast::ValueMapper *value_mapper, const RocksdbTransaction *txn) const {
#ifdef PELTON_PHYSICAL_SEPARATION
    return {};
#else
  IndexChoice plan = this->ChooseIndex(value_mapper);
  switch (plan.type) {
    // By primary key index.
    case IndexChoiceType::PK: {
      std::vector<sqlast::Value> vals =
          value_mapper->ReleaseValues(this->pk_column_);
      std::vector<std::string> encoded =
          EncodeValues(this->schema_.TypeOf(this->pk_column_), vals);
      return this->pk_index_.GetDedup(std::move(encoded), txn);
    }
    // By unique or regular index.
    case IndexChoiceType::UNIQUE:
    case IndexChoiceType::REGULAR: {
      const RocksdbIndex &index = this->indices_.at(plan.idx);
      return index.GetDedup(index.EncodeComposite(value_mapper, this->schema_),
                            txn);
    }
    // By scan.
    case IndexChoiceType::SCAN:
      return {};
    default:
      LOG(FATAL) << "Unsupported query plan";
  }
#endif  // PELTON_PHYSICAL_SEPARATION
}

#ifndef PELTON_PHYSICAL_SEPARATION
// Check if a record with given PK exists.
bool RocksdbTable::Exists(const rocksdb::Slice &pk_value,
                          const RocksdbTransaction *txn) const {
  return this->pk_index_.CheckUniqueAndLock(pk_value, txn);
}
#endif  // PELTON_PHYSICAL_SEPARATION

// Write to database.
void RocksdbTable::Put(const EncryptedKey &key, const EncryptedValue &value,
                       RocksdbTransaction *txn) {
  this->handle_.Put(key, value, txn);
}

// Get from database.
std::optional<EncryptedValue> RocksdbTable::Get(
    const EncryptedKey &key, bool read, const RocksdbTransaction *txn) const {
  return this->handle_.Get(key, read, txn);
}

// MultiGet: faster multi key lookup.
std::vector<std::optional<EncryptedValue>> RocksdbTable::MultiGet(
    const std::vector<EncryptedKey> &keys, bool read,
    const RocksdbTransaction *txn) const {
  return this->handle_.MultiGet(keys, read, txn);
}

// Delete from database.
void RocksdbTable::Delete(const EncryptedKey &key, RocksdbTransaction *txn) {
  this->handle_.Delete(key, txn);
}

// Get all data in table.
RocksdbStream RocksdbTable::GetAll(const RocksdbTransaction *txn) const {
  return this->handle_.GetAll(txn);
}

// Get all data in shard.
// Read all data from shard.
RocksdbStream RocksdbTable::GetShard(const EncryptedPrefix &shard_name,
                                     const RocksdbTransaction *txn) const {
  return this->handle_.GetShard(shard_name, txn);
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
