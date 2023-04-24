// Batched writes with RYW.
#include "k9db/sql/rocksdb/transaction.h"

#include <utility>

#include "glog/logging.h"
#include "k9db/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"

namespace k9db {
namespace sql {
namespace rocks {

namespace {

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

// This iterator locks every row read and used.
// This iterator doesn't lock the boundry row (e.g. when iterating within a key
// prefix, the first row outside that prefix will be eventually read, which
// is when iteration ends, this last key is not locked).
// Locking occurs on Next().
class LockingIterator : public rocksdb::Iterator {
 public:
  LockingIterator(rocksdb::Iterator *it, rocksdb::Transaction *txn,
                  rocksdb::ColumnFamilyHandle *cf)
      : it_(it), txn_(txn), cf_(cf) {}

  // We lock when moving to a new row.
  void Next() {
    this->Lock();
    this->it_->Next();
  }
  void Prev() { LOG(FATAL) << "Prev() unsupported!"; }

  // All other functions just forward to the underlying iterator.
  void Seek(const rocksdb::Slice &target) override { this->it_->Seek(target); }
  bool Valid() const override { return this->it_->Valid(); }
  void SeekToFirst() { this->it_->SeekToFirst(); }
  void SeekToLast() { this->it_->SeekToLast(); }
  void SeekForPrev(const rocksdb::Slice &target) {
    this->it_->SeekForPrev(target);
  }
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
  rocksdb::Transaction *txn_;
  rocksdb::ColumnFamilyHandle *cf_;

  // Lock current key.
  void Lock() {
    // Options.
    rocksdb::ReadOptions opts;
    opts.total_order_seek = true;
    opts.verify_checksums = false;
    // Call GetForUpdate.
    std::string _;
    auto status = this->txn_->GetForUpdate(opts, this->cf_, this->key(), &_);
    if (!status.ok() && !status.IsNotFound()) {
      PANIC(status);
    }
  }
};

}  // namespace

/*
 * RocksdbWriteTransaction.
 */

// A write transaction starts as soon as it is created!
RocksdbWriteTransaction::RocksdbWriteTransaction(rocksdb::TransactionDB *db)
    : db_(db),
      txn_(db_->BeginTransaction(WriteOpts(), TxnOpts())),
      finalized_(false) {
  // TODO(babman): can we get higher isolation using snapshots like the next
  //               line?
  // SetSnapshot on the next operation call.
  // Ensures that the read set has not been modified after the very first read.
  // this->txn_->SetSnapshotOnNextOperation();
}

RocksdbWriteTransaction::~RocksdbWriteTransaction() {
  if (!this->finalized_) {
    LOG(FATAL) << "Transaction destructed but not committed or rolledback!";
  }
  this->txn_ = nullptr;
}

// Commit / rollback.
void RocksdbWriteTransaction::Rollback() {
  this->finalized_ = true;
  PANIC(this->txn_->Rollback());
}
void RocksdbWriteTransaction::Commit() {
  this->finalized_ = true;
  PANIC(this->txn_->Commit());
}

// Writes.
void RocksdbWriteTransaction::Put(rocksdb::ColumnFamilyHandle *cf,
                                  const rocksdb::Slice &key,
                                  const rocksdb::Slice &value) {
  PANIC(this->txn_->Put(cf, key, value));
}
void RocksdbWriteTransaction::Delete(rocksdb::ColumnFamilyHandle *cf,
                                     const rocksdb::Slice &key) {
  PANIC(this->txn_->Delete(cf, key));
}

// Locking reads.
std::optional<std::string> RocksdbWriteTransaction::Get(
    rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key) const {
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;

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

std::vector<std::optional<std::string>> RocksdbWriteTransaction::MultiGet(
    rocksdb::ColumnFamilyHandle *cf,
    const std::vector<rocksdb::Slice> &keys) const {
  std::vector<rocksdb::ColumnFamilyHandle *> handles(keys.size(), cf);
  std::vector<std::string> pins(keys.size(), "");

  // Read from rocksdb.
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;

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

std::unique_ptr<rocksdb::Iterator> RocksdbWriteTransaction::Iterate(
    rocksdb::ColumnFamilyHandle *cf, bool same_prefix) const {
  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  opts.verify_checksums = false;
  opts.total_order_seek = !same_prefix;
  opts.prefix_same_as_start = same_prefix;

  // Get an iterator from both the underlying DB and
  // the current uncommitted write set of the transaction.
  // This iterator locks the read and used row (using GetForUpdate()).
  std::unique_ptr<rocksdb::Iterator> it = std::make_unique<LockingIterator>(
      this->txn_->GetIterator(opts, cf), this->txn_.get(), cf);
  if (same_prefix) {
    // Wrap around a prefix respecting iterator.
    rocksdb::ColumnFamilyDescriptor descriptor;
    PANIC(cf->GetDescriptor(&descriptor));
    const rocksdb::SliceTransform *transform =
        descriptor.options.prefix_extractor.get();
    return std::make_unique<PrefixRespectingIterator>(it.release(), transform);
  }
  return it;
}

/*
 * RocksdbReadSnapshot.
 */

// Acquire snapshot when the first read is issued, not when transaction is
// created to match MyRock behavior.
RocksdbReadSnapshot::RocksdbReadSnapshot(rocksdb::TransactionDB *db)
    : db_(db), snapshot_(nullptr) {}

// Delete owned pointers on destruction.
RocksdbReadSnapshot::~RocksdbReadSnapshot() {
  if (this->snapshot_ != nullptr) {
    this->db_->ReleaseSnapshot(this->snapshot_);
  }
  this->snapshot_ = nullptr;
}

// Initialize the snapshot if not already initialized.
void RocksdbReadSnapshot::InitializeSnapshot() const {
  if (this->snapshot_ == nullptr) {
    this->snapshot_ = this->db_->GetSnapshot();
  }
}

// Non-locking reads from snapshot.
std::optional<std::string> RocksdbReadSnapshot::Get(
    rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key) const {
  this->InitializeSnapshot();

  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  opts.snapshot = this->snapshot_;

  std::string result;
  rocksdb::Status status = this->db_->Get(opts, cf, key, &result);
  if (status.ok()) {
    return result;
  } else if (status.IsNotFound()) {
    return {};
  }
  PANIC(status);
  return {};
}

std::vector<std::optional<std::string>> RocksdbReadSnapshot::MultiGet(
    rocksdb::ColumnFamilyHandle *cf,
    const std::vector<rocksdb::Slice> &keys) const {
  this->InitializeSnapshot();

  // Make C-arrays for rocksdb MultiGet.
  std::unique_ptr<rocksdb::Status[]> statuses =
      std::make_unique<rocksdb::Status[]>(keys.size());
  std::unique_ptr<rocksdb::PinnableSlice[]> pins =
      std::make_unique<rocksdb::PinnableSlice[]>(keys.size());

  // Read from rocksdb.
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  opts.snapshot = this->snapshot_;

  // Read.
  this->db_->MultiGet(opts, cf, keys.size(), keys.data(), pins.get(),
                      statuses.get());

  // Move results into vector.
  std::vector<std::optional<std::string>> results;
  results.reserve(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    if (statuses[i].ok()) {
      results.emplace_back(pins[i].ToString());
    } else if (statuses[i].IsNotFound()) {
      results.emplace_back();
    } else {
      PANIC(statuses[i]);
    }
  }
  return results;
}

std::unique_ptr<rocksdb::Iterator> RocksdbReadSnapshot::Iterate(
    rocksdb::ColumnFamilyHandle *cf, bool same_prefix) const {
  this->InitializeSnapshot();

  rocksdb::ReadOptions opts;
  opts.fill_cache = false;
  opts.verify_checksums = false;
  opts.total_order_seek = !same_prefix;
  opts.prefix_same_as_start = same_prefix;
  opts.snapshot = this->snapshot_;

  // Get an iterator with the base snapshot from both the underlying DB.
  return std::unique_ptr<rocksdb::Iterator>(this->db_->NewIterator(opts, cf));
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
