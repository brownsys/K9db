// Alternative implemenation of our RocksdbTable class where the table is
// sharded over different column families, one per user.
// This is only used in the drill down experiment.

// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/table.h"
// clang-format on

#include <utility>

#include "glog/logging.h"
#include "k9db/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace k9db {
namespace sql {
namespace rocks {

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
      mtx_(),
      transform_(),
      handles_() {
  const std::vector<dataflow::ColumnID> &keys = this->schema_.keys();
  CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
  this->pk_column_ = keys.front();

  // Are we restarting a previously created database?
  CHECK(this->metadata_ == nullptr || !this->metadata_->IsRestart())
      << "Must use a fresh DB with physical separation";
}

/*
 * (Global, i.e. across shards) indices are not supported in physical separation
 * mode.
 */

void RocksdbTable::IndexAdd(const rocksdb::Slice &shard,
                            const RocksdbSequence &row,
                            RocksdbWriteTransaction *txn, bool update_pk) {}

void RocksdbTable::IndexDelete(const rocksdb::Slice &shard,
                               const RocksdbSequence &row,
                               RocksdbWriteTransaction *txn, bool update_pk) {}

void RocksdbTable::IndexUpdate(const rocksdb::Slice &shard,
                               const RocksdbSequence &old,
                               const RocksdbSequence &updated,
                               RocksdbWriteTransaction *txn) {}

RocksdbPlan RocksdbTable::ChooseIndex(
    const sqlast::ValueMapper *value_mapper) const {
  RocksdbPlan plan(this->table_name_, this->schema_);
  plan.MakeScan();
  return plan;
}
RocksdbPlan RocksdbTable::ChooseIndex(size_t column_index) const {
  RocksdbPlan plan(this->table_name_, this->schema_);
  plan.MakeScan();
  return plan;
}

std::optional<IndexSet> RocksdbTable::IndexLookup(
    sqlast::ValueMapper *value_mapper, const RocksdbInterface *txn,
    int limit) const {
  return {};
}
std::optional<DedupIndexSet> RocksdbTable::IndexLookupDedup(
    sqlast::ValueMapper *value_mapper, const RocksdbInterface *txn,
    int limit) const {
  return {};
}

/*
 * Key-based operations.
 */

// Write to database.
void RocksdbTable::Put(const EncryptedKey &key, const EncryptedValue &value,
                       RocksdbWriteTransaction *txn) {
  rocksdb::ColumnFamilyHandle *cf = this->GetOrCreateCf(key);
  txn->Put(cf, key.Data(), value.Data());
}

// Delete from database.
void RocksdbTable::Delete(const EncryptedKey &key,
                          RocksdbWriteTransaction *txn) {
  std::optional<rocksdb::ColumnFamilyHandle *> cf = this->GetCf(key);
  if (cf.has_value()) {
    txn->Delete(cf.value(), key.Data());
  }
}

// Get from database.
std::optional<EncryptedValue> RocksdbTable::Get(
    const EncryptedKey &key, const RocksdbInterface *txn) const {
  std::optional<rocksdb::ColumnFamilyHandle *> cf = this->GetCf(key);
  if (cf.has_value()) {
    std::optional<std::string> data = txn->Get(cf.value(), key.Data());
    if (data.has_value()) {
      return Cipher::FromDB(std::move(data.value()));
    }
  }
  return {};
}

// MultiGet: faster multi key lookup.
std::vector<std::optional<EncryptedValue>> RocksdbTable::MultiGet(
    const std::vector<EncryptedKey> &keys, const RocksdbInterface *txn) const {
  if (keys.size() == 0) {
    return {};
  }

  // Group keys by cf.
  std::unordered_map<std::string, std::vector<size_t>> map;
  for (size_t i = 0; i < keys.size(); i++) {
    std::string sh = this->transform_.Transform(keys.at(i).Data()).ToString();
    map[sh].push_back(i);
  }

  // MultiGet within each column family.
  std::vector<std::optional<EncryptedValue>> result(
      keys.size(), std::optional<EncryptedValue>());
  for (const auto &[_, v] : map) {
    // Extract slices.
    std::vector<rocksdb::Slice> slices;
    slices.reserve(v.size());
    for (size_t i : v) {
      slices.push_back(keys.at(i).Data());
    }

    // Use MultiGet.
    std::optional<rocksdb::ColumnFamilyHandle *> cf =
        this->GetCf(keys.at(v.front()));
    if (cf.has_value()) {
      std::vector<std::optional<std::string>> data =
          txn->MultiGet(cf.value(), slices);

      // Transform to Ciphers.
      for (size_t i = 0; i < data.size(); i++) {
        std::optional<std::string> opt = data.at(i);
        if (opt.has_value()) {
          result.at(v.at(i)) = Cipher::FromDB(std::move(opt.value()));
        }
      }
    }
  }
  return result;
}

/*
 * Iterator operations.
 */

// Get all data in table.
RocksdbStream RocksdbTable::GetAll(const RocksdbInterface *txn) const {
  RocksdbStream stream;
  for (const auto &[_, cf] : this->handles_) {
    std::unique_ptr<rocksdb::Iterator> it = txn->Iterate(cf.get(), false);
    it->SeekToFirst();
    stream.AddIterator(std::move(it));
  }
  return stream;
}

// Get all data in shard.
// Read all data from shard.
RocksdbStream RocksdbTable::GetShard(const EncryptedPrefix &shard_name,
                                     const RocksdbInterface *txn) const {
  std::optional<rocksdb::ColumnFamilyHandle *> cf = this->GetCf(shard_name);
  if (cf.has_value()) {
    // Iterator that spans the whole shard prefix.
    std::unique_ptr<rocksdb::Iterator> it = txn->Iterate(cf.value(), false);
    it->SeekToFirst();
    return RocksdbStream(std::move(it));
  }
  return RocksdbStream();
}

/*
 * Column family management.
 */

// Get or create a column family if non exist.
template <typename T>
rocksdb::ColumnFamilyHandle *RocksdbTable::GetOrCreateCf(const T &cipher) {
  std::string shard = this->transform_.Transform(cipher.Data()).ToString();
  util::SharedLock lock(&this->mtx_);
  auto &&[upgraded, condition] =
      lock.UpgradeIf([&]() { return this->handles_.count(shard) == 0; });
  if (condition) {
    std::string name = this->table_name_ + "_" + shard;
    // Create rocksdb table.
    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options = this->ColumnFamilyOptions();
    PANIC(this->db_->CreateColumnFamily(options, name, &handle));
    this->handles_.emplace(shard, handle);
    return handle;
  }
  return this->handles_.at(shard).get();
}

// Get or optional.
template <typename T>
std::optional<rocksdb::ColumnFamilyHandle *> RocksdbTable::GetCf(
    const T &cipher) const {
  std::string shard = this->transform_.Transform(cipher.Data()).ToString();
  util::SharedLock lock(&this->mtx_);
  auto it = this->handles_.find(shard);
  if (it != this->handles_.end()) {
    return it->second.get();
  }
  return {};
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
