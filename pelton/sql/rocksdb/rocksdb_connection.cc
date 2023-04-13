#include "pelton/sql/rocksdb/rocksdb_connection.h"

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"

namespace pelton {
namespace sql {
namespace rocks {

// Open a database connection (on pelton start).
void RocksdbConnection::Open(const std::string &db_name) {
#ifdef PELTON_OPT
  std::string path = "/mnt/disks/my-ssd/pelton/" + db_name;
#else
  std::string path = "/tmp/pelton/rocksdb/" + db_name;
#endif  // PELTON_OPT

  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();

  // Transaction options.
  rocksdb::TransactionDBOptions txn_opts;
  txn_opts.transaction_lock_timeout = 10000;
  txn_opts.write_policy = rocksdb::TxnDBWritePolicy::WRITE_COMMITTED;
  txn_opts.default_write_batch_flush_threshold = 0;

  // Open the database.
  rocksdb::TransactionDB *db;
  PANIC(rocksdb::TransactionDB::Open(opts, txn_opts, path, &db));
  this->db_ = std::unique_ptr<rocksdb::TransactionDB>(db);
}

// Close database connection (on pelton shutdown).
void RocksdbConnection::Close() {
  this->tables_.clear();
  this->db_ = nullptr;
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
