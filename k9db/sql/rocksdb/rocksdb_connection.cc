#include "k9db/sql/rocksdb/rocksdb_connection.h"

#include "glog/logging.h"
#include "k9db/util/status.h"
#include "rocksdb/options.h"

#define DEFAULT_K9DB_DB_PATH "/tmp/k9db_"

namespace k9db {
namespace sql {
namespace rocks {

namespace {
inline bool EndsWith(const std::string &str, const std::string &suffix) {
  if (str.size() < suffix.size()) {
    return false;
  }
  return std::equal(suffix.rbegin(), suffix.rend(), str.rbegin());
}
}  // namespace

/*
 * Opening/closing a rocksdb connection.
 */

std::vector<std::string> RocksdbConnection::Open(const std::string &db_name,
                                                 const std::string &db_path) {
  // Make path for database.
  std::string path = DEFAULT_K9DB_DB_PATH;
  if (db_path.size() > 0) {
    path = db_path;
    if (path.back() != '/') {
      path.push_back('/');
    }
  }
  path = path + db_name;

  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();

  // Transaction options.
  rocksdb::TransactionDBOptions txn_opts;
  txn_opts.transaction_lock_timeout = 10000;
  txn_opts.write_policy = rocksdb::TxnDBWritePolicy::WRITE_COMMITTED;
  txn_opts.default_write_batch_flush_threshold = 0;

  // Check if database already exists in that path, and whether it has some
  // column families.
  std::vector<std::string> existing_cfs;
  rocksdb::Status status =
      rocksdb::DB::ListColumnFamilies(opts, path, &existing_cfs);

  // ListColumnFamilies may succeed, or may fail if there is no rocksdatabase
  // in that path.
  if (!status.ok() && !status.IsIOError() && !status.IsNotFound()) {
    PANIC(status);
  }

  // Specify existing column families to load when DB is created.
  std::vector<rocksdb::ColumnFamilyHandle *> handles;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  for (const std::string &name : existing_cfs) {
    if (name == "default") {
      // Default column family.
      descriptors.emplace_back(name, rocksdb::ColumnFamilyOptions());
    } else if (EndsWith(name, RocksdbIndex::ColumnFamilySuffix())) {
      // Secondary index.
      descriptors.emplace_back(name, RocksdbIndex::ColumnFamilyOptions());
    } else if (EndsWith(name, RocksdbPKIndex::ColumnFamilySuffix())) {
      // PK index.
      descriptors.emplace_back(name, RocksdbPKIndex::ColumnFamilyOptions());
    } else if (name == RocksdbMetadata::KeysColumnFamily()) {
      // User encryption keys persistent column family.
      descriptors.emplace_back(name,
                               RocksdbMetadata::KeysColumnFamilyOptions());
    } else if (name == RocksdbMetadata::StatementsColumnFamily()) {
      // Statements persistent column family.
      descriptors.emplace_back(
          name, RocksdbMetadata::StatementsColumnFamilyOptions());
    } else {
      // Column family corresponds to a regular table.
      descriptors.emplace_back(name, RocksdbTable::ColumnFamilyOptions());
    }
  }

  // Open database.
  rocksdb::TransactionDB *db;
  if (descriptors.size() == 0) {
    PANIC(rocksdb::TransactionDB::Open(opts, txn_opts, path, &db));
  } else {
    PANIC(rocksdb::TransactionDB::Open(opts, txn_opts, path, descriptors,
                                       &handles, &db));
  }
  this->db_ = std::unique_ptr<rocksdb::TransactionDB>(db);

  // Figure out what to do with any opened pre-existing column families.
  std::vector<std::string> result = this->metadata_.Initialize(
      this->db_.get(), std::move(handles), std::move(descriptors));

  // Initialize encryption manager.
  this->encryption_.Initialize(&this->metadata_);
  return result;
}

// Close database connection (on k9db shutdown).
void RocksdbConnection::Close() {
  this->tables_.clear();
  this->metadata_.Clear();
  this->db_ = nullptr;
}

}  // namespace rocks
}  // namespace sql
}  // namespace k9db
