// Transactions with RYW and REPEATABLE READS isolation.
#ifndef PELTON_SQL_ROCKSDB_TRANSACTION_H_
#define PELTON_SQL_ROCKSDB_TRANSACTION_H_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/utilities/transaction_db.h"

namespace pelton {
namespace sql {
namespace rocks {

// All the reads and writes to rocksdb go via this interface or subclasses.
// The interface is either instantiated as a transaction (for writes),
// or over a consistent snapshot (for reads).
class RocksdbInterface {
 public:
  virtual ~RocksdbInterface() = default;

  // Reads: for transaction, these reads lock the rows as if they are writes,
  //        for consistent snapshot, these reads return consistent data with the
  //        snapshot.
  virtual std::optional<std::string> Get(rocksdb::ColumnFamilyHandle *cf,
                                         const rocksdb::Slice &key) const = 0;

  virtual std::vector<std::optional<std::string>> MultiGet(
      rocksdb::ColumnFamilyHandle *cf,
      const std::vector<rocksdb::Slice> &keys) const = 0;

  virtual std::unique_ptr<rocksdb::Iterator> Iterate(
      rocksdb::ColumnFamilyHandle *cf, bool same_prefix) const = 0;
};

// A single statement transaction containing reads and writes to several rows
// in potentially several column families.
// Writes in the batch are not applied to the DB, which remains unchanged,
// until the transaction is committed.
// Reads are escalated to writes so they similiarly acquire a lock.
// Further, we use the SetSnapshot() API in rocksdb to ensure that the
// transaction won't commit if concurrent changes were made to the read set
// after the first read.
class RocksdbWriteTransaction : public RocksdbInterface {
 public:
  explicit RocksdbWriteTransaction(rocksdb::TransactionDB *db);
  ~RocksdbWriteTransaction();

  // Commit / Rollback.
  void Commit();
  void Rollback();

  // Only defined in RocksdbWriteTransaction.
  // Writes: these are batched and staged but not comitted. Writes acquire a
  // lock to what gets written, including new previously-unexisting keys.
  void Put(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key,
           const rocksdb::Slice &value);
  void Delete(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key);

  // Read current (i.e. latest comitted data), acquire write lock for the read
  // data.
  std::optional<std::string> Get(rocksdb::ColumnFamilyHandle *cf,
                                 const rocksdb::Slice &key) const override;

  std::vector<std::optional<std::string>> MultiGet(
      rocksdb::ColumnFamilyHandle *cf,
      const std::vector<rocksdb::Slice> &keys) const override;

  std::unique_ptr<rocksdb::Iterator> Iterate(rocksdb::ColumnFamilyHandle *cf,
                                             bool same_prefix) const override;

 private:
  rocksdb::TransactionDB *db_;
  std::unique_ptr<rocksdb::Transaction> txn_;
  bool finalized_;
};

class RocksdbReadSnapshot : public RocksdbInterface {
 public:
  explicit RocksdbReadSnapshot(rocksdb::TransactionDB *db);
  ~RocksdbReadSnapshot();

  // Read from snapshot and don't acquire locks for read data.
  std::optional<std::string> Get(rocksdb::ColumnFamilyHandle *cf,
                                 const rocksdb::Slice &key) const override;

  std::vector<std::optional<std::string>> MultiGet(
      rocksdb::ColumnFamilyHandle *cf,
      const std::vector<rocksdb::Slice> &keys) const override;

  std::unique_ptr<rocksdb::Iterator> Iterate(rocksdb::ColumnFamilyHandle *cf,
                                             bool same_prefix) const override;

 private:
  rocksdb::TransactionDB *db_;
  mutable const rocksdb::Snapshot *snapshot_;

  // Initialize snapshot if it is not initialized already.
  void InitializeSnapshot() const;
};

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_TRANSACTION_H_
