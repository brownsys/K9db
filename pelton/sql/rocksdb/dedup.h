// Helper for deduplication.
#ifndef PELTON_SQL_ROCKSDB_DEDUP_H_
#define PELTON_SQL_ROCKSDB_DEDUP_H_

#include <unordered_set>
#include <utility>

#include "pelton/sql/rocksdb/encode.h"

namespace pelton {
namespace sql {

template <typename T>
class DedupSet {
 public:
  DedupSet() = default;

  bool Duplicate(T &&t) { return !this->set_.insert(std::move(t)).second; }

 private:
  std::unordered_set<T> set_;
};

using IndexSet =
    std::unordered_set<RocksdbIndexRecord, RocksdbIndexRecord::Hash,
                       RocksdbIndexRecord::Equal>;

using DedupIndexSet =
    std::unordered_set<RocksdbIndexRecord, RocksdbIndexRecord::DedupHash,
                       RocksdbIndexRecord::DedupEqual>;

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_DEDUP_H_
