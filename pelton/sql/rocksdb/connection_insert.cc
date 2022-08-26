#include "glog/logging.h"
#include "pelton/sql/rocksdb/connection.h"
#include "pelton/sql/rocksdb/encode.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

namespace pelton {
namespace sql {

/*
 * INSERT STATEMENTS.
 */
InsertResult RocksdbConnection::ExecuteInsert(const sqlast::Insert &stmt,
                                              const std::string &shard_name) {
  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(table_name).get();
  std::vector<std::optional<RocksdbIndex>> &indices =
      this->indices_.at(table_name);

  // Encode row.
  RocksdbRecord record = RocksdbRecord::FromInsert(stmt, schema, shard_name);
  rocksdb::Slice pk = record.GetPK();

  // Check PK is unique.
  std::optional<RocksdbIndex> &pk_index = indices.at(schema.keys().front());
  CHECK(pk_index.has_value()) << "No PK index!";
  CHECK_EQ(pk_index->Get({pk}).size(), 0) << "Integrity error: PK exists";

  // Update indices.
  RocksdbRecord::Iterator iterator = record.IterateValue();
  for (size_t i = 0; iterator.Next(); i++) {
    std::optional<RocksdbIndex> &index = indices.at(i);
    if (index.has_value()) {
      index->Add(iterator.Value(), shard_name, pk);
    }
  }

  // Encrypt.
  EncryptedRecord encrypted =
      this->encryption_manager_.EncryptRecord(shard_name, std::move(record));

  // Write to DB.
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(this->db_->Put(opts, handler, encrypted.Key(), encrypted.Value()));

  return {1, 0};
}

InsertResult RocksdbConnection::ExecuteReplace(const sqlast::Insert &stmt,
                                               const std::string &shard_name) {
  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(table_name).get();
  std::vector<std::optional<RocksdbIndex>> &indices =
      this->indices_.at(table_name);

  // Encode row.
  RocksdbRecord record = RocksdbRecord::FromInsert(stmt, schema, shard_name);
  rocksdb::Slice pk = record.GetPK();

  // Check if row exists.
  std::optional<RocksdbIndex> &pk_index = indices.at(schema.keys().front());
  CHECK(pk_index.has_value()) << "No PK index!";
  std::vector<RocksdbIndexRecord> lookup = pk_index->Get({pk});
  CHECK_LE(lookup.size(), 1u);

  // Handle any existing rows that need to be replaced (i.e. deleted).
  if (lookup.size() > 0) {
    // TODO(babman): rewrite this as a delete?
    // Encrypt lookup key that was read from index.
    auto lookup_key =
        this->encryption_manager_.EncryptKey(lookup.front().TargetKey());
    std::string old_shard_name = lookup.front().ExtractShardName().ToString();

    // Read from table.
    std::string str;
    rocksdb::ReadOptions opts;
    opts.total_order_seek = true;
    opts.verify_checksums = false;
    PANIC(this->db_->Get(opts, handler, lookup_key, &str));

    // Decrypt read record.
    EncryptedRecord encrypted("", std::move(str));
    RocksdbRecord old_record = this->encryption_manager_.DecryptRecord(
        old_shard_name, std::move(encrypted));

    // Update indices.
    RocksdbRecord::Iterator iterator = old_record.IterateValue();
    for (size_t i = 0; iterator.Next(); i++) {
      std::optional<RocksdbIndex> &index = indices.at(i);
      if (index.has_value()) {
        index->Delete(iterator.Value(), lookup.front().ExtractShardName(), pk);
      }
    }

    // Delete record.
    PANIC(this->db_->Delete(rocksdb::WriteOptions(), handler, lookup_key));
  }

  // Update indices.
  RocksdbRecord::Iterator iterator = record.IterateValue();
  for (size_t i = 0; iterator.Next(); i++) {
    std::optional<RocksdbIndex> &index = indices.at(i);
    if (index.has_value()) {
      index->Add(iterator.Value(), shard_name, pk);
    }
  }

  // Encrypt.
  EncryptedRecord encrypted =
      this->encryption_manager_.EncryptRecord(shard_name, std::move(record));

  // Write to DB.
  rocksdb::WriteOptions opts;
  opts.sync = true;
  PANIC(this->db_->Put(opts, handler, encrypted.Key(), encrypted.Value()));

  return {1, 0};
}

}  // namespace sql
}  // namespace pelton
