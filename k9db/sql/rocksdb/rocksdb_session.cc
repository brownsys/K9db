// clang-format off
// NOLINTNEXTLINE
#include "k9db/sql/rocksdb/rocksdb_connection.h"
// clang-format on

namespace k9db {
namespace sql {
namespace rocks {

/*
 * Opening, committing or rolling back a session.
 */

std::unique_ptr<Session> RocksdbConnection::OpenSession() {
  return std::make_unique<RocksdbSession>(this);
}

void RocksdbSession::BeginTransaction(bool write) {
  this->write_txn_ = write;
  if (this->txn_ == nullptr) {
    rocksdb::TransactionDB *db = this->conn_->db_.get();
    if (write) {
      this->txn_ = std::make_unique<RocksdbWriteTransaction>(db);
    } else {
      this->txn_ = std::make_unique<RocksdbReadSnapshot>(db);
    }
  }
}
void RocksdbSession::CommitTransaction() {
  if (this->write_txn_) {
    reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get())->Commit();
  }
  this->txn_ = nullptr;
}
void RocksdbSession::RollbackTransaction() {
  if (this->write_txn_) {
    reinterpret_cast<RocksdbWriteTransaction *>(this->txn_.get())->Rollback();
  }
  this->txn_ = nullptr;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
