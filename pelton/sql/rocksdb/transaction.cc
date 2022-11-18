// Batched writes with RYW.
#include "pelton/sql/rocksdb/transaction.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"

namespace pelton {
namespace sql {
namespace rocks {

// Options for transaction.
rocksdb::WriteOptions WriteOpts() {
  rocksdb::WriteOptions opts;
  opts.sync = true;
  return opts;
}
rocksdb::TransactionOptions TxnOpts() {
  rocksdb::TransactionOptions opts;
  return opts;
}

// While the usual rocksdb::Iterator, used for reading data from the database,
// respects the parameters passed to it in the associated rocksdb::Options, the
// specific child class of Iterator that rocksdb::WriteBatchWithIndex uses
// when reading from both the database and the batch does not respect
// most of them.
// https://github.com/facebook/rocksdb/blob/a8a4ed52a4f4ab57d214ed4984306ea89da32f3f/utilities/write_batch_with_index/write_batch_with_index_internal.cc#L34
// This is because it relies on composing the DB iterator with the WriteBatch
// iterator, and the later doesn't support any ReadOptions.
// Our code uses the prefix_same_as_start option sometimes (when reading from
// a secondary index or iterating over the shard of a user), and relies on
// the iterator correctly enforcing it.
// As a result, we create a dummy wrapper here that enforces this parameter
// after using rocksdb::WriteBatchWithIndex's violating iterator.
namespace {

class PrefixRespectingIterator : public rocksdb::Iterator {
 public:
  PrefixRespectingIterator(rocksdb::Iterator *it,
                           const rocksdb::SliceTransform *transform)
      : it_(it), transform_(transform) {}

  // On seek, we store the prefix, and then compare to it in Valid().
  void Seek(const rocksdb::Slice &target) override {
    CHECK(this->transform_->InDomain(target));
    this->it_->Seek(target);
    this->prefix_ = this->transform_->Transform(target);
  }
  bool Valid() const override {
    if (this->it_->Valid()) {
      // Current key needs to be in the same prefix.
      rocksdb::Slice key = this->it_->key();
      if (this->transform_->InDomain(key)) {
        return this->prefix_ == this->transform_->Transform(key);
      }
    }
    return false;
  }

  // All other functions just forward to the underlying iterator.
  void SeekToFirst() { this->it_->SeekToFirst(); }
  void SeekToLast() { this->it_->SeekToLast(); }
  void SeekForPrev(const rocksdb::Slice &target) {
    this->it_->SeekForPrev(target);
  }
  void Next() { this->it_->Next(); }
  void Prev() { this->it_->Prev(); }
  rocksdb::Slice key() const { return this->it_->key(); }
  rocksdb::Slice value() const { return this->it_->value(); }
  rocksdb::Status status() const { return this->it_->status(); }
  rocksdb::Status Refresh() { return this->it_->Refresh(); }
  rocksdb::Slice timestamp() const { return this->it_->timestamp(); }
  rocksdb::Status GetProperty(std::string prop_name, std::string *prop) {
    return this->it_->GetProperty(prop_name, prop);
  }

 private:
  std::unique_ptr<rocksdb::Iterator> it_;
  const rocksdb::SliceTransform *transform_;
  rocksdb::Slice prefix_;
};

}  // namespace

/*
 * Constructor/destructor.
 */

// A transaction starts as soon as it is created!
RocksdbTransaction::RocksdbTransaction(rocksdb::TransactionDB *db)
    : db_(db),
      txn_(db_->BeginTransaction(WriteOpts(), TxnOpts())),
      snapshot_(db_->GetSnapshot()) {}

// Delete owned pointers on destruction.
RocksdbTransaction::~RocksdbTransaction() {
  this->txn_ = nullptr;
  this->db_->ReleaseSnapshot(this->snapshot_);
  this->snapshot_ = nullptr;
}

/*
 * Atomic Writes.
 */

void RocksdbTransaction::Put(rocksdb::ColumnFamilyHandle *cf,
                             const rocksdb::Slice &key,
                             const rocksdb::Slice &value) {
  PANIC(this->txn_->Put(cf, key, value));
}
void RocksdbTransaction::Delete(rocksdb::ColumnFamilyHandle *cf,
                                const rocksdb::Slice &key) {
  PANIC(this->txn_->Delete(cf, key));
}

/*
 * Reads without locking.
 */

std::optional<std::string> RocksdbTransaction::Get(
    rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key) const {
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  // opts.snapshot = this->snapshot_;

  std::string result;
  rocksdb::Status status = this->txn_->Get(opts, cf, key, &result);
  if (status.ok()) {
    return result;
  } else if (status.IsNotFound()) {
    return {};
  }
  PANIC(status);
  return {};
}
std::vector<std::optional<std::string>> RocksdbTransaction::MultiGet(
    rocksdb::ColumnFamilyHandle *cf,
    const std::vector<rocksdb::Slice> &keys) const {
  // Make C-arrays for rocksdb MultiGet.
  std::unique_ptr<rocksdb::Status[]> statuses =
      std::make_unique<rocksdb::Status[]>(keys.size());
  std::unique_ptr<rocksdb::PinnableSlice[]> pins =
      std::make_unique<rocksdb::PinnableSlice[]>(keys.size());

  // Read from rocksdb.
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  // opts.snapshot = this->snapshot_;

  // Read.
  this->txn_->MultiGet(opts, cf, keys.size(), keys.data(), pins.get(),
                       statuses.get());

  // Move results into vector.
  std::vector<std::optional<std::string>> results;
  results.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    if (statuses[i].ok()) {
      results.emplace_back(std::move(*pins[i].GetSelf()));
    } else if (statuses[i].IsNotFound()) {
      results.emplace_back();
    } else {
      PANIC(statuses[i]);
    }
  }
  return results;
}

/*
 * Locking reads.
 */

std::optional<std::string> RocksdbTransaction::GetForUpdate(
    rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key) const {
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  // opts.snapshot = this->snapshot_;

  std::string result;
  rocksdb::Status status = this->txn_->GetForUpdate(opts, cf, key, &result);
  if (status.ok()) {
    return result;
  } else if (status.IsNotFound()) {
    return {};
  }
  PANIC(status);
  return {};
}
std::vector<std::optional<std::string>> RocksdbTransaction::MultiGetForUpdate(
    rocksdb::ColumnFamilyHandle *cf,
    const std::vector<rocksdb::Slice> &keys) const {
  std::vector<rocksdb::ColumnFamilyHandle *> handles(keys.size(), cf);
  std::vector<std::string> pins;
  pins.reserve(keys.size());

  // Read from rocksdb.
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  // opts.snapshot = this->snapshot_;

  // Read.
  std::vector<rocksdb::Status> statuses =
      this->txn_->MultiGetForUpdate(opts, handles, keys, &pins);

  // Move results into vector.
  std::vector<std::optional<std::string>> results;
  results.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    if (statuses.at(i).ok()) {
      results.emplace_back(std::move(pins.at(i)));
    } else if (statuses.at(i).IsNotFound()) {
      results.emplace_back();
    } else {
      PANIC(statuses.at(i));
    }
  }
  return results;
}

/*
 * Iteration without locking.
 */

std::unique_ptr<rocksdb::Iterator> RocksdbTransaction::Iterate(
    rocksdb::ColumnFamilyHandle *cf, bool same_prefix) const {
  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  opts.verify_checksums = false;
  opts.total_order_seek = !same_prefix;
  opts.prefix_same_as_start = same_prefix;
  // opts.snapshot = this->snapshot_;

  // Get an iterator with the base snapshot from both the underlying DB and
  // the current uncommitted write set of the transaction.
  rocksdb::Iterator *it = this->txn_->GetIterator(opts, cf);
  if (same_prefix) {
    // Wrap around a prefix respecting iterator.
    rocksdb::ColumnFamilyDescriptor descriptor;
    PANIC(cf->GetDescriptor(&descriptor));
    const rocksdb::SliceTransform *transform =
        descriptor.options.prefix_extractor.get();
    return std::make_unique<PrefixRespectingIterator>(it, transform);
  } else {
    return std::unique_ptr<rocksdb::Iterator>(it);
  }
}

/*
 * Commit / rollback.
 */

void RocksdbTransaction::Rollback() { PANIC(this->txn_->Rollback()); }
void RocksdbTransaction::Commit() { PANIC(this->txn_->Commit()); }

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
