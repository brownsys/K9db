#include "k9db/sql/rocksdb/index.h"

#include <algorithm>
#include <optional>
#include <utility>

#include "k9db/sqlast/ast.h"
#include "k9db/util/iterator.h"
#include "k9db/util/status.h"

namespace k9db {
namespace sql {
namespace rocks {

namespace {

/*
 * The prefix transform for our indices.
 * Unlike a table's prefix transform which yields <shard,pk> prefixes,
 * An index transform yields <indexed_value> prefixes.
 * This enables lookups/seek based on lookup values without knowing the target
 * shard(s).
 */
class IndexPrefixTransform : public rocksdb::SliceTransform {
 public:
  IndexPrefixTransform() = default;

  const char *Name() const override { return "ShardPrefix"; }
  bool InDomain(const rocksdb::Slice &key) const override { return true; }
  rocksdb::Slice Transform(const rocksdb::Slice &key) const override {
    return ExtractColumn(key, 0);
  }
};

// Helper: avoids writing the Get code twice for regular and dedup lookups.
template <typename T, typename R, bool shards_specified = false>
T GetHelper(rocksdb::DB *db, const RocksdbInterface *txn,
            rocksdb::ColumnFamilyHandle *handle,
            std::vector<std::string> &&values,
            std::vector<std::string> &&shards = {}, int limit = -1) {
  // Open iterator over the database and the txn together.
  std::unique_ptr<rocksdb::Iterator> it = txn->Iterate(handle, true);

  // Make and sort all prefixes.
  for (std::string &val : values) {
    val.push_back(__ROCKSSEP);
  }

  if constexpr (shards_specified) {
    if (shards.size() == values.size()) {
      auto [b, e] = util::Zip(&values, &shards);
      std::sort(b, e);
    } else {
      LOG(FATAL) << "Lookup with shards has bad number of shards";
    }
  } else {
    std::sort(values.begin(), values.end());
  }

  // Holds the result.
  T result;

  // Go through prefixes in order.
  for (size_t i = 0; i < values.size(); i++) {
    const std::string &value = values.at(i);
    rocksdb::Slice pslice(value);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      if (limit != -1 && result.size() == static_cast<size_t>(limit)) {
        return result;
      }

      R record(it->key());
      if constexpr (shards_specified) {
        if (record.GetShard() == shards.at(i)) {
          result.emplace(record.TargetKey());
        }
      } else {
        result.emplace(record.TargetKey());
      }
    }
  }
  return result;
}

}  // namespace

/*
 * RocksdbIndex
 */
// Rocksdb Options for index column families.
std::string RocksdbIndex::ColumnFamilySuffix() { return "__index"; }
rocksdb::ColumnFamilyOptions RocksdbIndex::ColumnFamilyOptions() {
  rocksdb::ColumnFamilyOptions options;
  options.prefix_extractor.reset(new IndexPrefixTransform());
  options.comparator = rocksdb::BytewiseComparator();
  return options;
}

// Constructor.
RocksdbIndex::RocksdbIndex(rocksdb::DB *db, const std::string &table_name,
                           const std::vector<size_t> &columns,
                           RocksdbMetadata *metadata)
    : db_(db), handle_(), columns_(columns), cf_name_() {
  // Create a name for the index.
  std::string name = table_name;
  for (size_t c : this->columns_) {
    name += "_" + std::to_string(c);
  }
  name += ColumnFamilySuffix();

  // Has this index been created before and we are reloading as part of
  // restarting k9db?
  std::optional<std::unique_ptr<rocksdb::ColumnFamilyHandle>> handle = {};
  if (metadata != nullptr) {
    handle = metadata->Find(name);
  }

  if (!handle.has_value()) {
    // Create a column family for this index.
    rocksdb::ColumnFamilyHandle *handle;
    PANIC(this->db_->CreateColumnFamily(ColumnFamilyOptions(), name, &handle));
    this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
  } else {
    // It was already created, point to it!
    this->handle_ = std::move(handle.value());
  }

  // Store the name used for this column family.
  this->cf_name_ = std::move(name);
}

// Adding things to index.
void RocksdbIndex::Add(const std::vector<rocksdb::Slice> &values,
                       const rocksdb::Slice &shard_name,
                       const rocksdb::Slice &pk, RocksdbWriteTransaction *txn) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  IRecord e(values, shard_name, pk);
  txn->Put(handle, e.Data(), "");
}

// Deleting things from index.
void RocksdbIndex::Delete(const std::vector<rocksdb::Slice> &values,
                          const rocksdb::Slice &shard_name,
                          const rocksdb::Slice &pk,
                          RocksdbWriteTransaction *txn) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  IRecord e(values, shard_name, pk);
  txn->Delete(handle, e.Data());
}

// Use to encode condition values for composite index.
std::vector<std::string> RocksdbIndex::EncodeComposite(
    sqlast::ValueMapper *vm, const dataflow::SchemaRef &schema) const {
  std::vector<std::string> result;
  for (size_t col : this->columns_) {
    // Prefix has been consumed.
    if (!vm->HasValues(col)) {
      for (std::string &str : result) {
        str.push_back(__ROCKSCOMP);
      }
      break;
    }
    // More columns still have values.
    std::vector<sqlast::Value> vals = vm->ReleaseValues(col);
    if (result.size() == 0) {
      result = EncodeValues(schema.TypeOf(col), vals);
    } else {
      std::vector<std::string> next;
      std::vector<std::string> encoded = EncodeValues(schema.TypeOf(col), vals);
      for (const std::string &previous : result) {
        for (const std::string &enc : encoded) {
          next.push_back(previous);
          next.back().push_back(__ROCKSCOMP);
          next.back().append(enc);
        }
      }
      result = std::move(next);
    }
  }
  return result;
}

// Get by value.
IndexSet RocksdbIndex::Get(std::vector<std::string> &&v,
                           const RocksdbInterface *txn, int limit) const {
  return GetHelper<IndexSet, IRecord>(this->db_, txn, this->handle_.get(),
                                      std::move(v), {}, limit);
}
DedupIndexSet RocksdbIndex::GetDedup(std::vector<std::string> &&v,
                                     const RocksdbInterface *txn,
                                     int limit) const {
  return GetHelper<DedupIndexSet, IRecord>(this->db_, txn, this->handle_.get(),
                                           std::move(v), {}, limit);
}

IndexSet RocksdbIndex::GetWithShards(std::vector<std::string> &&shards,
                                     std::vector<std::string> &&values,
                                     const RocksdbInterface *txn) const {
  return GetHelper<IndexSet, IRecord, true>(this->db_, txn, this->handle_.get(),
                                            std::move(values),
                                            std::move(shards));
}

/*
 * RocksdbPKIndex
 */

// Rocksdb Options for PK index column families.
std::string RocksdbPKIndex::ColumnFamilySuffix() { return "__pk_index"; }
rocksdb::ColumnFamilyOptions RocksdbPKIndex::ColumnFamilyOptions() {
  rocksdb::ColumnFamilyOptions options;
  options.prefix_extractor.reset(new IndexPrefixTransform());
  options.comparator = rocksdb::BytewiseComparator();
  return options;
}

// Constructor.
RocksdbPKIndex::RocksdbPKIndex(rocksdb::DB *db, const std::string &table_name,
                               RocksdbMetadata *metadata)
    : db_(db) {
  // Create a name for the index.
  std::string name = table_name + ColumnFamilySuffix();

  // Has this index been created before and we are reloading as part of
  // restarting k9db?
  std::optional<std::unique_ptr<rocksdb::ColumnFamilyHandle>> handle = {};
  if (metadata != nullptr) {
    handle = metadata->Find(name);
  }

  if (!handle.has_value()) {
    // Create a column family for this index.
    rocksdb::ColumnFamilyHandle *handle;
    PANIC(this->db_->CreateColumnFamily(ColumnFamilyOptions(), name, &handle));
    this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
  } else {
    // It was already created, point to it!
    this->handle_ = std::move(handle.value());
  }
}

// Adding things to index.
void RocksdbPKIndex::Add(const rocksdb::Slice &pk_value,
                         const rocksdb::Slice &shard_name,
                         RocksdbWriteTransaction *txn) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::optional<std::string> str = txn->Get(handle, pk_value);
  if (str.has_value()) {
    IRecord e(std::move(str.value()));
    e.AppendNewShard(shard_name);
    txn->Put(handle, pk_value, e.Data());
  } else {
    IRecord e;
    e.AppendNewShard(shard_name);
    txn->Put(handle, pk_value, e.Data());
  }
}

// Deleting things from index.
void RocksdbPKIndex::Delete(const rocksdb::Slice &pk_value,
                            const rocksdb::Slice &shard_name,
                            RocksdbWriteTransaction *txn) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::optional<std::string> str = txn->Get(handle, pk_value);
  if (str.has_value()) {
    IRecord e(std::move(str.value()));
    e.RemoveShard(shard_name);
    if (e.Empty()) {
      txn->Delete(handle, pk_value);
    } else {
      txn->Put(handle, pk_value, e.Data());
    }
  }
}

// Check whether PK exists or not and lock it (atomic)!
bool RocksdbPKIndex::Exists(const rocksdb::Slice &pk,
                            const RocksdbWriteTransaction *txn) const {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::optional<std::string> result = txn->Get(handle, pk);
  return result.has_value();
}

// Get by value.
IndexSet RocksdbPKIndex::Get(std::vector<std::string> &&v,
                             const RocksdbInterface *txn) const {
  std::vector<rocksdb::Slice> slices;
  slices.reserve(v.size());
  for (const std::string &str : v) {
    slices.emplace_back(str);
  }

  IndexSet set;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::vector<std::optional<std::string>> records =
      txn->MultiGet(handle, slices);
  for (size_t i = 0; i < records.size(); i++) {
    std::optional<std::string> str = records.at(i);
    if (str.has_value()) {
      IRecord irecord(std::move(str.value()));
      for (rocksdb::Slice shard : irecord) {
        RocksdbSequence sequence;
        sequence.AppendEncoded(shard);
        sequence.AppendEncoded(slices.at(i));
        set.emplace(std::move(sequence));
      }
    }
  }
  return set;
}
DedupIndexSet RocksdbPKIndex::GetDedup(std::vector<std::string> &&v,
                                       const RocksdbInterface *txn) const {
  std::vector<rocksdb::Slice> slices;
  slices.reserve(v.size());
  for (const std::string &str : v) {
    slices.emplace_back(str);
  }

  DedupIndexSet set;
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::vector<std::optional<std::string>> records =
      txn->MultiGet(handle, slices);
  for (size_t i = 0; i < records.size(); i++) {
    std::optional<std::string> str = records.at(i);
    if (str.has_value()) {
      IRecord irecord(std::move(str.value()));
      for (rocksdb::Slice shard : irecord) {
        RocksdbSequence sequence;
        sequence.AppendEncoded(shard);
        sequence.AppendEncoded(slices.at(i));
        set.emplace(std::move(sequence));
        break;
      }
    }
  }
  return set;
}

// Count how many shard each pk value is in.
std::vector<size_t> RocksdbPKIndex::CountShards(
    std::vector<std::string> &&pk_values, const RocksdbInterface *txn) const {
  std::vector<rocksdb::Slice> slices;
  slices.reserve(pk_values.size());
  for (const std::string &str : pk_values) {
    slices.emplace_back(str);
  }

  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  std::vector<std::optional<std::string>> records =
      txn->MultiGet(handle, slices);

  std::vector<size_t> result;
  result.reserve(pk_values.size());
  for (size_t i = 0; i < records.size(); i++) {
    size_t count = 0;
    std::optional<std::string> str = records.at(i);
    if (str.has_value()) {
      IRecord irecord(std::move(str.value()));
      for (auto it = irecord.begin(); it != irecord.end(); ++it) {
        count++;
      }
    }
    result.push_back(count);
  }
  return result;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
