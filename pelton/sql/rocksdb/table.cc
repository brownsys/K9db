#include "pelton/sql/rocksdb/table.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

// Constructor.
RocksdbTable::RocksdbTable(rocksdb::DB *db, const std::string &table_name,
                           const dataflow::SchemaRef &schema)
    : db_(db),
      table_name_(table_name),
      schema_(schema),
      pk_column_(),
      handle_(),
      pk_index_(db, table_name),
      indices_(schema.size()) {
  const std::vector<dataflow::ColumnID> &keys = this->schema_.keys();
  CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
  this->pk_column_ = keys.front();

  // Create rocksdb table.
  rocksdb::ColumnFamilyHandle *handle;
  rocksdb::ColumnFamilyOptions options;
  options.OptimizeLevelStyleCompaction();
  options.prefix_extractor.reset(new PeltonPrefixTransform());
  options.comparator = PeltonComparator();
  PANIC(this->db_->CreateColumnFamily(options, table_name, &handle));
  this->handle_ = std::unique_ptr<rocksdb::ColumnFamilyHandle>(handle);
}

// Create an index.
void RocksdbTable::CreateIndex(size_t column_index) {
  if (column_index != this->pk_column_ &&
      !this->indices_.at(column_index).has_value()) {
    this->indices_.at(column_index) =
        RocksdbIndex(this->db_, this->table_name_, column_index);
  }
}

// Index updating.
void RocksdbTable::IndexAdd(const rocksdb::Slice &shard,
                            const RocksdbSequence &row) {
  rocksdb::Slice pk = row.At(this->pk_column_);

  // Update PK index.
  this->pk_index_.Add(pk, shard);

  // Update other indices.
  size_t i = 0;
  for (rocksdb::Slice value : row) {
    std::optional<RocksdbIndex> &index = this->indices_.at(i++);
    if (index.has_value()) {
      index->Add(value, shard, pk);
    }
  }
}
void RocksdbTable::IndexDelete(const rocksdb::Slice &shard,
                               const RocksdbSequence &row) {
  rocksdb::Slice pk = row.At(this->pk_column_);

  // Update PK index.
  this->pk_index_.Delete(pk, shard);

  // Update other indices.
  size_t i = 0;
  for (rocksdb::Slice value : row) {
    std::optional<RocksdbIndex> &index = this->indices_.at(i++);
    if (index.has_value()) {
      index->Delete(value, shard, pk);
    }
  }
}

// Check if a record with given PK exists.
bool RocksdbTable::Exists(const rocksdb::Slice &pk_value) const {
  return this->pk_index_.Get({pk_value}).size() > 0;
}

// Write to database.
void RocksdbTable::Put(const EncryptedKey &key, const EncryptedValue &value) {
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(this->db_->Put(opts, this->handle_.get(), key.Data(), value.Data()));
}

// Get from database.
std::optional<EncryptedValue> RocksdbTable::Get(const EncryptedKey &key) const {
  std::string data;

  rocksdb::ReadOptions opts;
  opts.total_order_seek = true;
  opts.verify_checksums = false;
  auto status = this->db_->Get(opts, this->handle_.get(), key.Data(), &data);
  if (status.ok()) {
    return Cipher::FromDB(std::move(data));
  } else if (status.IsNotFound()) {
    return {};
  }
  PANIC(status);
}

// Delete from database.
void RocksdbTable::Delete(const EncryptedKey &key) {
  rocksdb::WriteOptions opts;
  opts.sync = true;  // Maybe not necessary? Check MYROCKS.
  PANIC(this->db_->Delete(opts, this->handle_.get(), key.Data()));
}

}  // namespace sql
}  // namespace pelton
