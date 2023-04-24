// Helper for deduplication.
#ifndef K9DB_SQL_ROCKSDB_DEDUP_H_
#define K9DB_SQL_ROCKSDB_DEDUP_H_

#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "k9db/sql/rocksdb/encode.h"

namespace k9db {
namespace sql {
namespace rocks {

template <typename T>
class DedupSet {
 public:
  DedupSet() = default;

  bool Duplicate(T &&t) { return !this->set_.insert(std::move(t)).second; }

 private:
  std::unordered_set<T> set_;
};

template <typename K>
class DedupMap {
 public:
  DedupMap() = default;

  bool Exists(K &&t) {
    this->ptr_ = &this->set_[t];
    return *this->ptr_ != 0;
  }
  size_t Value() const { return *this->ptr_ - 1; }
  void Assign(size_t val) { *this->ptr_ = val + 1; }

 private:
  std::unordered_map<K, size_t> set_;
  size_t *ptr_;
};

using IndexSet =
    std::unordered_set<RocksdbIndexRecord, RocksdbIndexRecord::Hash,
                       RocksdbIndexRecord::Equal>;

using DedupIndexSet =
    std::unordered_set<RocksdbIndexRecord, RocksdbIndexRecord::DedupHash,
                       RocksdbIndexRecord::DedupEqual>;

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_DEDUP_H_
