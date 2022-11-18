// clang-format off
// NOLINTNEXTLINE
#include "pelton/sql/rocksdb/rocksdb_connection.h"
// clang-format on

namespace pelton {
namespace sql {
namespace rocks {

/*
 * Opening, committing or rolling back a session.
 */

std::unique_ptr<Session> RocksdbConnection::OpenSession() {
  return std::make_unique<RocksdbSession>(this);
}

void RocksdbSession::BeginTransaction() {
  this->txn_ = std::make_unique<RocksdbTransaction>(this->conn_->db_.get());
}
void RocksdbSession::CommitTransaction() {
  this->txn_->Commit();
  this->txn_ = nullptr;
}
void RocksdbSession::RollbackTransaction() {
  this->txn_->Rollback();
  this->txn_ = nullptr;
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
