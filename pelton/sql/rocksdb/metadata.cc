// This class is responsible for persisting and reloading schema and metadata
// information to disk.
// Every time a table, index, or view is created. This class provides an
// interface to persist the description of what is created to disk, so that
// when pelton is restarted, it can read that description and reload the table,
// index, or view properly.
// This class is also responsible for storing the required encryption keys.

#include "pelton/sql/rocksdb/metadata.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"

#define GLOBAL_KEY "__global__key"
#define GLOBAL_NONCE "__global__nonce"

namespace pelton {
namespace sql {
namespace rocks {

namespace {
std::string FixedSizeInt(int i) {
  std::string str = std::to_string(i);
  while (str.size() < 4) {
    str = "0" + str;
  }
  return str;
}
inline bool StartsWith(const std::string &str, const std::string &prefix,
                       size_t idx = 0) {
  if (str.size() < prefix.size() + idx) {
    return false;
  }
  return std::equal(prefix.begin(), prefix.end(), str.begin() + idx);
}
}  // namespace

// Initialize when the db is opened.
// Loads the metadata from disk, if any exist.
std::vector<std::string> RocksdbMetadata::Initialize(
    rocksdb::TransactionDB *db,
    std::vector<rocksdb::ColumnFamilyHandle *> &&handles,
    std::vector<rocksdb::ColumnFamilyDescriptor> &&descriptors) {
  // Store db pointer.
  this->db_ = db;

  // Store loaded pre-existing column families.
  CHECK_EQ(handles.size(), descriptors.size());
  for (size_t i = 0; i < handles.size(); i++) {
    const std::string &name = descriptors.at(i).name;
    if (name == "default") {
      delete handles.at(i);
      continue;
    } else if (name == KeysColumnFamily()) {
      this->keys_cf_ =
          std::unique_ptr<rocksdb::ColumnFamilyHandle>(handles.at(i));
    } else if (name == StatementsColumnFamily()) {
      this->statements_cf_ =
          std::unique_ptr<rocksdb::ColumnFamilyHandle>(handles.at(i));
    } else {
      this->cf_map_.emplace(name, handles.at(i));
      this->persisted_cfs_.insert(name);
    }
  }

  // In case pelton is just being started for the first time, we need to create
  // the column families for statements and keys.
  if (this->keys_cf_ == nullptr) {
    std::string cf_name = KeysColumnFamily();
    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options = KeysColumnFamilyOptions();
    PANIC(this->db_->CreateColumnFamily(options, cf_name, &handle));
    this->keys_cf_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
  }
  if (this->statements_cf_ == nullptr) {
    std::string cf_name = StatementsColumnFamily();
    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options = StatementsColumnFamilyOptions();
    PANIC(this->db_->CreateColumnFamily(options, cf_name, &handle));
    this->statements_cf_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
  }

  // Read all persisted statements and find views.
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  rocksdb::Iterator *ptr =
      this->db_->NewIterator(opts, this->statements_cf_.get());

  std::vector<std::string> result;
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string stmt = it->value().ToString();
    if (StartsWith(stmt, "CREATE VIEW ")) {
      std::string view_name = "";
      for (size_t i = 12; i < stmt.size(); i++) {
        if (StartsWith(stmt, " AS ", i)) {
          view_name = stmt.substr(12, i);
        }
      }
      this->persisted_views_.insert(view_name);
    }
    result.push_back(std::move(stmt));
  }
  this->counter_ = result.size();
  return result;
}

// Given a column family name, see if such a column family was already created
// by previous runs of pelton, and return it if found.
std::optional<std::unique_ptr<rocksdb::ColumnFamilyHandle>>
RocksdbMetadata::Find(const std::string &column_family_name) {
  auto it = this->cf_map_.find(column_family_name);
  if (it == this->cf_map_.end()) {
    return {};
  } else {
    return std::move(it->second);
  }
}

// Persist statements to disk.
void RocksdbMetadata::Persist(const sqlast::CreateTable &sql) {
  const std::string &cf_name = sql.table_name();
  if (this->persisted_cfs_.count(cf_name) == 0) {
    rocksdb::WriteOptions opts;
    opts.sync = true;
    sqlast::Stringifier stringifier;
    std::string key = FixedSizeInt(this->counter_++);
    std::string stmt = sql.Visit(&stringifier);
    PANIC(this->db_->Put(opts, this->statements_cf_.get(), key, stmt));
    this->persisted_cfs_.insert(cf_name);
  }
}
void RocksdbMetadata::Persist(std::string cf_name,
                              const sqlast::CreateIndex &sql) {
  if (this->persisted_cfs_.count(cf_name) == 0) {
    rocksdb::WriteOptions opts;
    opts.sync = true;
    sqlast::Stringifier stringifier;
    std::string key = FixedSizeInt(this->counter_++);
    std::string stmt = sql.Visit(&stringifier);
    PANIC(this->db_->Put(opts, this->statements_cf_.get(), key, stmt));
    this->persisted_cfs_.insert(cf_name);
  }
}
void RocksdbMetadata::Persist(const sqlast::CreateView &sql) {
  std::string view_name = sql.view_name();
  if (this->persisted_views_.count(view_name) == 0) {
    rocksdb::WriteOptions opts;
    opts.sync = true;
    sqlast::Stringifier stringifier;
    std::string key = FixedSizeInt(this->counter_++);
    std::string stmt = sql.Visit(&stringifier);
    PANIC(this->db_->Put(opts, this->statements_cf_.get(), key, stmt));
    this->persisted_views_.insert(view_name);
  }
}

// Load encryption keys.
std::optional<std::string> RocksdbMetadata::LoadGlobalKey() {
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;

  std::string result;
  rocksdb::Status status =
      this->db_->Get(opts, this->keys_cf_.get(), GLOBAL_KEY, &result);
  if (status.ok()) {
    return result;
  } else if (!status.IsNotFound()) {
    PANIC(status);
  }
  return {};
}

std::optional<std::string> RocksdbMetadata::LoadGlobalNonce() {
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;

  std::string result;
  rocksdb::Status status =
      this->db_->Get(opts, this->keys_cf_.get(), GLOBAL_NONCE, &result);
  if (status.ok()) {
    return result;
  } else if (!status.IsNotFound()) {
    PANIC(status);
  }
  return {};
}

std::unordered_map<std::string, std::string> RocksdbMetadata::LoadUserKeys() {
  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  rocksdb::Iterator *ptr = this->db_->NewIterator(opts, this->keys_cf_.get());

  std::unordered_map<std::string, std::string> result;
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string key = it->key().ToString();
    if (key == GLOBAL_KEY || key == GLOBAL_NONCE) {
      continue;
    }
    result.emplace(key, it->value().ToString());
  }
  return result;
}

// Persist encryption keys.
void RocksdbMetadata::PersistGlobalKey(const std::string &key) {
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(this->db_->Put(opts, this->keys_cf_.get(), GLOBAL_KEY, key));
}
void RocksdbMetadata::PersistGlobalNonce(const std::string &nonce) {
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(this->db_->Put(opts, this->keys_cf_.get(), GLOBAL_NONCE, nonce));
}
void RocksdbMetadata::PersistUserKey(const std::string &user_id,
                                     const std::string &key) {
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(this->db_->Put(opts, this->keys_cf_.get(), user_id, key));
}

}  // namespace rocks
}  // namespace sql
}  // namespace pelton
