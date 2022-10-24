#include "pelton/sql/rocksdb/index.h"

#include <algorithm>
#include <utility>

#include "pelton/util/iterator.h"
#include "pelton/util/status.h"

namespace pelton {
namespace sql {

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
T GetHelper(rocksdb::DB *db, rocksdb::ColumnFamilyHandle *handle,
            std::vector<std::string> &&values,
            std::vector<std::string> &&shards = {}) {
  // Iterator options.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.total_order_seek = false;
  options.prefix_same_as_start = true;
  options.verify_checksums = false;

  // Open iterator.
  rocksdb::Iterator *ptr = db->NewIterator(options, handle);
  std::unique_ptr<rocksdb::Iterator> it(ptr);

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
// Constructor.
RocksdbIndex::RocksdbIndex(rocksdb::DB *db, const std::string &table_name,
                           size_t column)
    : db_(db) {
  // Create a name for the index.
  std::string name = table_name + "_index" + std::to_string(column);

  // Create a column family for this index.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.prefix_extractor.reset(new IndexPrefixTransform());
  options.comparator = rocksdb::BytewiseComparator();
  PANIC(this->db_->CreateColumnFamily(options, name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbIndex::Add(const rocksdb::Slice &value,
                       const rocksdb::Slice &shard_name,
                       const rocksdb::Slice &pk) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  IRecord e(value, shard_name, pk);
  PANIC(this->db_->Put(rocksdb::WriteOptions(), handle, e.Data(), ""));
}

// Deleting things from index.
void RocksdbIndex::Delete(const rocksdb::Slice &value,
                          const rocksdb::Slice &shard_name,
                          const rocksdb::Slice &pk) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  IRecord e(value, shard_name, pk);
  PANIC(this->db_->Delete(rocksdb::WriteOptions(), handle, e.Data()));
}

// Get by value.
IndexSet RocksdbIndex::Get(std::vector<std::string> &&v) const {
  return GetHelper<IndexSet, IRecord>(this->db_, this->handle_.get(),
                                      std::move(v));
}
DedupIndexSet RocksdbIndex::GetDedup(std::vector<std::string> &&v) const {
  return GetHelper<DedupIndexSet, IRecord>(this->db_, this->handle_.get(),
                                           std::move(v));
}

IndexSet RocksdbIndex::GetWithShards(std::vector<std::string> &&shards,
                                     std::vector<std::string> &&values) const {
  return GetHelper<IndexSet, IRecord, true>(
      this->db_, this->handle_.get(), std::move(values), std::move(shards));
}

/*
 * RocksdbPKIndex
 */
// Constructor.
RocksdbPKIndex::RocksdbPKIndex(rocksdb::DB *db, const std::string &table_name)
    : db_(db) {
  // Create a name for the index.
  std::string name = table_name + "_pk_index";

  // Create a column family for this index.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.prefix_extractor.reset(new IndexPrefixTransform());
  options.comparator = rocksdb::BytewiseComparator();
  PANIC(this->db_->CreateColumnFamily(options, name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Adding things to index.
void RocksdbPKIndex::Add(const rocksdb::Slice &pk_value,
                         const rocksdb::Slice &shard_name) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  IRecord e(pk_value, shard_name);
  PANIC(this->db_->Put(rocksdb::WriteOptions(), handle, e.Data(), ""));
}

// Deleting things from index.
void RocksdbPKIndex::Delete(const rocksdb::Slice &pk_value,
                            const rocksdb::Slice &shard_name) {
  rocksdb::ColumnFamilyHandle *handle = this->handle_.get();
  IRecord e(pk_value, shard_name);
  PANIC(this->db_->Delete(rocksdb::WriteOptions(), handle, e.Data()));
}

// Get by value.
IndexSet RocksdbPKIndex::Get(std::vector<std::string> &&v) const {
  return GetHelper<IndexSet, IRecord>(this->db_, this->handle_.get(),
                                      std::move(v));
}
DedupIndexSet RocksdbPKIndex::GetDedup(std::vector<std::string> &&v) const {
  return GetHelper<DedupIndexSet, IRecord>(this->db_, this->handle_.get(),
                                           std::move(v));
}

// Count how many shard each pk value is in.
std::vector<size_t> RocksdbPKIndex::CountShards(
    std::vector<std::string> &&pk_values) const {
  // Iterator options.
  rocksdb::ReadOptions options;
  options.fill_cache = false;
  options.total_order_seek = false;
  options.prefix_same_as_start = true;
  options.verify_checksums = false;

  // Open iterator.
  rocksdb::Iterator *ptr = this->db_->NewIterator(options, this->handle_.get());
  std::unique_ptr<rocksdb::Iterator> it(ptr);

  // Make and sort all prefixes.
  std::vector<size_t> indices;
  indices.reserve(pk_values.size());
  for (std::string &pk : pk_values) {
    indices.push_back(indices.size());
    pk.push_back(__ROCKSSEP);
  }

  auto [b, e] = util::Zip(&pk_values, &indices);
  std::sort(b, e);

  // Holds the result.
  std::vector<size_t> result(pk_values.size(), 0);

  // Go through prefixes in order.
  for (const auto &[pk_value, idx] : util::Zip(&pk_values, &indices)) {
    rocksdb::Slice pslice(pk_value);
    for (it->Seek(pslice); it->Valid(); it->Next()) {
      result[idx]++;
    }
  }
  return result;
}

}  // namespace sql
}  // namespace pelton
