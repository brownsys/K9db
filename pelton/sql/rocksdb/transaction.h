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

// A batch of writes.
// Writes in the batch are not applied to the DB, which remains unchanged.
// The batch can include writes/deletes to different column families.
// We provide a read API below so that sessions can read from the conjunction of
// the database and the batch, so that a session can read writes it issued but
// did not commit (or rollback).
class RocksdbTransaction {
 public:
  explicit RocksdbTransaction(rocksdb::TransactionDB *db);
  ~RocksdbTransaction();

  // Initialize / start the transaction.
  void Start();

  // Writes.
  void Put(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key,
           const rocksdb::Slice &value);
  void Delete(rocksdb::ColumnFamilyHandle *cf, const rocksdb::Slice &key);

  // Reads.
  std::optional<std::string> Get(rocksdb::ColumnFamilyHandle *cf,
                                 const rocksdb::Slice &key) const;

  std::vector<std::optional<std::string>> MultiGet(
      rocksdb::ColumnFamilyHandle *cf,
      const std::vector<rocksdb::Slice> &keys) const;

  std::optional<std::string> GetForUpdate(rocksdb::ColumnFamilyHandle *cf,
                                          const rocksdb::Slice &key) const;

  std::vector<std::optional<std::string>> MultiGetForUpdate(
      rocksdb::ColumnFamilyHandle *cf,
      const std::vector<rocksdb::Slice> &keys) const;

  std::unique_ptr<rocksdb::Iterator> Iterate(rocksdb::ColumnFamilyHandle *cf,
                                             bool same_prefix) const;

  // Start/Commit/Rollback.
  void Commit();
  void Rollback();

 private:
  rocksdb::TransactionDB *db_;
  std::unique_ptr<rocksdb::Transaction> txn_;
  const rocksdb::Snapshot *snapshot_ = nullptr;
};

}  // namespace rocks
}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ROCKSDB_TRANSACTION_H_
