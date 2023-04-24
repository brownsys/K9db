#ifndef K9DB_SQL_ROCKSDB_METADATA_H__
#define K9DB_SQL_ROCKSDB_METADATA_H__

// This class is responsible for persisting and reloading schema and metadata
// information to disk.
// Every time a table, index, or view is created. This class provides an
// interface to persist the description of what is created to disk, so that
// when k9db is restarted, it can read that description and reload the table,
// index, or view properly.
// This class is also responsible for storing the required encryption keys.

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "k9db/sqlast/ast.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"

namespace k9db {
namespace sql {
namespace rocks {

class RocksdbMetadata {
 public:
  // Column family options for the families used to store keys and persisted
  // statements.
  static rocksdb::ColumnFamilyOptions KeysColumnFamilyOptions() {
    return rocksdb::ColumnFamilyOptions();
  }
  static rocksdb::ColumnFamilyOptions StatementsColumnFamilyOptions() {
    return rocksdb::ColumnFamilyOptions();
  }
  static std::string KeysColumnFamily() { return "__keys__"; }
  static std::string StatementsColumnFamily() { return "__statements__"; }

  // Constructor.
  RocksdbMetadata() = default;

  // Need to destruct early to ensure safe destruction of RocksdbConnection.
  void Clear() {
    this->statements_cf_ = nullptr;
    this->keys_cf_ = nullptr;
    this->cf_map_.clear();
  }

  // Initialize when the db is opened.
  // Loads the metadata from disk, if any exist.
  // Returns all the previously persisted statements from metadata DB.
  std::vector<std::string> Initialize(
      rocksdb::TransactionDB *db,
      std::vector<rocksdb::ColumnFamilyHandle *> &&handles,
      std::vector<rocksdb::ColumnFamilyDescriptor> &&descriptors);

  // Given a column family name, see if such a column family was already created
  // by previous runs of k9db, and return it if found.
  std::optional<std::unique_ptr<rocksdb::ColumnFamilyHandle>> Find(
      const std::string &column_family_name);

  // Persist statements to disk.
  void Persist(const sqlast::CreateTable &sql);
  void Persist(std::string cf_name, const sqlast::CreateIndex &sql);
  void Persist(const sqlast::CreateView &sql);

  // Load encryption keys.
  std::optional<std::string> LoadGlobalKey();
  std::optional<std::string> LoadGlobalNonce();
  std::unordered_map<std::string, std::string> LoadUserKeys();

  // Persist encryption keys.
  void PersistGlobalKey(const std::string &key);
  void PersistGlobalNonce(const std::string &nonce);
  void PersistUserKey(const std::string &user_id, const std::string &key);

 private:
  rocksdb::TransactionDB *db_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> statements_cf_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> keys_cf_;
  std::unordered_map<std::string, std::unique_ptr<rocksdb::ColumnFamilyHandle>>
      cf_map_;

  // Track persisted statements.
  std::unordered_set<std::string> persisted_views_;
  std::unordered_set<std::string> persisted_cfs_;

  // Counter ensures persist and reload occur in the same order.
  std::atomic<size_t> counter_;
};

}  // namespace rocks
}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_ROCKSDB_METADATA_H__
