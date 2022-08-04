#include "pelton/sql/rocksdb/connection.h"

#include <unordered_set>
#include <utility>

#include "glog/logging.h"
#include "pelton/sql/rocksdb/encryption.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

#define PANIC PANIC_STATUS

namespace pelton {
namespace sql {

namespace {

#define __ROCKSSEP static_cast<char>(30)

bool HasWhereClause(const sqlast::AbstractStatement *stmt) {
  switch (stmt->type()) {
    case sqlast::AbstractStatement::Type::UPDATE: {
      return static_cast<const sqlast::Update *>(stmt)->HasWhereClause();
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      return static_cast<const sqlast::Delete *>(stmt)->HasWhereClause();
    }
    case sqlast::AbstractStatement::Type::SELECT: {
      return static_cast<const sqlast::Select *>(stmt)->HasWhereClause();
    }
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
}

}  // namespace

// Open a connection.
void RocksdbConnection::Open(const std::string &db_name) {
  std::string path = "/tmp/pelton/rocksdb/" + db_name;

  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();
  opts.prefix_extractor.reset(new EncryptedPrefixTransform());
  opts.comparator = EncryptedComparator::Instance();

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  this->db_ = std::unique_ptr<rocksdb::DB>(db);

  // Initialize libsodium.
  InitializeEncryption();

  // Generate global key.
  this->global_key_ = KeyGen();
}

// Close the connection.
void RocksdbConnection::Close() {
  if (this->db_ != nullptr) {
    this->indices_.clear();
    this->handlers_.clear();
    this->db_ = nullptr;

#ifdef PELTON_ENCRYPTION
    // delete encryption keys
    delete[] this->global_key_;
    for (const auto &[user, user_key] : this->user_keys_) {
      delete[] user_key;
    }
#endif  // PELTON_ENCRYPTION
  }
}

/*
 * CREATE STATEMENTS.
 */
bool RocksdbConnection::ExecuteCreateTable(const sqlast::CreateTable &stmt) {
  const std::string &table_name = stmt.table_name();
  if (this->tables_.count(table_name) == 0) {
    // Schema and PK.
    dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(stmt);
    const std::vector<dataflow::ColumnID> &keys = schema.keys();
    CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
    size_t pk = keys.at(0);

    rocksdb::ColumnFamilyHandle *handle;
    rocksdb::ColumnFamilyOptions options;
    options.OptimizeLevelStyleCompaction();
    options.prefix_extractor.reset(new EncryptedPrefixTransform());
    options.comparator = EncryptedComparator::Instance();
    PANIC(this->db_->CreateColumnFamily(options, table_name, &handle));

    // Fill in table metadata.
    TableID tid = this->tables_.size();
    this->tables_.emplace(table_name, tid);
    this->handlers_.emplace(tid, handle);
    this->schemas_.emplace(tid, schema);
    this->primary_keys_.emplace(tid, pk);
    this->indexed_columns_.emplace(tid, std::vector<size_t>{});
    this->indices_.emplace(tid, std::vector<RocksdbIndex>{});
    if (stmt.GetColumns().at(pk).auto_increment()) {
      this->auto_increment_counters_.emplace(tid, 0);
    }

    // Create indices for table.
    for (size_t i = 0; i < stmt.GetColumns().size(); i++) {
      const auto &column = stmt.GetColumns().at(i);
      bool has_constraint = false;
      for (const auto &cnstr : column.GetConstraints()) {
        if (cnstr.type() == sqlast::ColumnConstraint::Type::FOREIGN_KEY) {
          has_constraint = true;
          break;
        } else if (cnstr.type() == sqlast::ColumnConstraint::Type::UNIQUE) {
          has_constraint = true;
          break;
        }
      }
      if (has_constraint) {
        this->indexed_columns_.at(tid).push_back(i);
        this->indices_.at(tid).emplace_back(this->db_.get(), tid, i);
      }
    }
  }
  return true;
}

// Execute Create Index Statement
bool RocksdbConnection::ExecuteCreateIndex(const sqlast::CreateIndex &stmt) {
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  size_t cid = this->schemas_.at(tid).IndexOf(stmt.column_name());
  std::vector<size_t> &indexed_columns = this->indexed_columns_.at(tid);
  auto it = std::find(indexed_columns.begin(), indexed_columns.end(), cid);
  if (it == indexed_columns.end()) {
    std::vector<RocksdbIndex> &indices = this->indices_.at(tid);
    indexed_columns.push_back(cid);
    indices.emplace_back(this->db_.get(), tid, cid);
  }
  return true;
}

/*
 * UPDATE STATEMENTS.
 */
InsertResult RocksdbConnection::ExecuteInsert(const sqlast::Insert &stmt,
                                              const std::string &shard_name) {
  ShardID sid = shard_name + __ROCKSSEP;

  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.VisitInsert(stmt);
  if (stmt.replace() && indexed_columns.size() > 0) {
    // replacing insert with some indices, we need to find existing rows
    // to update indices correctly, might as well use the returning
    // code.
    LOG(FATAL) << "Unsupported";
  }

  // if we are not replacing, then we will need to update indices for
  // the insert only (nothing is removed).
  // if no indices, then we do not need to know what rows got replaced.
  std::string pk_value = "";
  uint64_t last_insert_id = 0;
  if (value_mapper.HasAfter(db_schema.NameOf(pk))) {
    if (this->auto_increment_counters_.count(tid) == 1) {
      LOG(FATAL) << "Insert has value for PK but it is auto increment!";
    }
    const auto &vals = value_mapper.After(db_schema.NameOf(pk));
    CHECK_EQ(vals.size(), 1u) << "Insert has no value for PK!";
    pk_value = vals.at(0).ToString();
    CHECK(!IsNull(pk_value.data(), pk_value.size())) << "Insert has NULL PK!";
  } else {
    if (this->auto_increment_counters_.count(tid) == 0) {
      LOG(FATAL) << "Insert has no value for PK and not auto increment!";
    }
    last_insert_id = ++(this->auto_increment_counters_.at(tid));
    pk_value = std::to_string(last_insert_id);
  }

  // Insert/replace new row.
  std::string skey = sid + pk_value + __ROCKSSEP;
  std::string row = EncodeInsert(stmt, db_schema, pk_value);
  auto opts = rocksdb::WriteOptions();
  opts.sync = true;
  PANIC(this->db_->Put(opts, handler, EncryptKey(this->global_key_, skey),
                       Encrypt(this->GetUserKey(shard_name), row)));

  // Update indices.
  rocksdb::Slice pkslice(pk_value.data(), pk_value.size());
  rocksdb::Slice rslice(row);
  for (size_t i = 0; i < indexed_columns.size(); i++) {
    RocksdbIndex &index = indices.at(i);
    size_t indexed_column = indexed_columns.at(i);
    index.Add(ExtractColumn(rslice, indexed_column), shard_name, pkslice);
  }
  return {1, last_insert_id};
}

// Execute Update statement
SqlResult RocksdbConnection::ExecuteUpdate(const sqlast::Update &stmt) {
  // ShardID sid = shard_name + __ROCKSSEP;

  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.VisitUpdate(stmt);

  // Turn update effects into an easy to process form.
  std::unordered_map<std::string, std::string> update;
  auto &columns = stmt.GetColumns();
  auto &values = stmt.GetValues();
  for (size_t i = 0; i < columns.size(); i++) {
    update.emplace(columns.at(i), values.at(i));
  }

  // Look up all affected rows.
  std::pair<std::vector<std::string>, std::vector<std::string>> result =
      this->GetRecords(tid, value_mapper, true);
  std::vector<std::string> shards = result.second;
  std::vector<std::string> rows = result.first;

  // Result components
  std::vector<dataflow::Record> records;
  std::vector<std::string> okeys;
  rows = this->Filter(db_schema, &stmt, std::move(rows));
  if (rows.size() > 5) {
    LOG(WARNING) << "Perf Warning: " << rows.size()
                 << " rocksdb updates in a loop " << stmt;
  }
  for (int i = 0; i < rows.size(); i++) {
    std::string &row = rows[i];
    std::string &shard_name = shards[i];
    // Compute updated row.
    std::string nrow = Update(update, db_schema, row);
    if (nrow == row) {  // No-op.
      continue;
    }
    rocksdb::Slice oslice(row);
    rocksdb::Slice nslice(nrow);
    rocksdb::Slice opk = ExtractColumn(oslice, pk);
    if (stmt.returning()) {
      okeys.push_back(opk.ToString());
      records.push_back(DecodeRecord(oslice, db_schema, {}, {}));
    }

    rocksdb::Slice npk = ExtractColumn(nslice, pk);
    // std::shard_name = ExtractColumn(shard, 0);
    ShardID sid = shard_name + __ROCKSSEP;
    std::string okey = sid + opk.ToString() + __ROCKSSEP;
    std::string nkey = sid + npk.ToString() + __ROCKSSEP;
    bool keychanged = !SlicesEq(opk, npk);
    // If the PK is unchanged, we do not need to delete, and can replace.
    if (keychanged) {
      PANIC(db_->Delete(rocksdb::WriteOptions(), handler,
                        EncryptKey(this->global_key_, okey)));
    }
    PANIC(db_->Put(rocksdb::WriteOptions(), handler,
                   EncryptKey(this->global_key_, nkey),
                   Encrypt(this->GetUserKey(shard_name), nrow)));
    // Update indices.
    for (size_t i = 0; i < indexed_columns.size(); i++) {
      RocksdbIndex &index = indices.at(i);
      size_t indexed_column = indexed_columns.at(i);
      rocksdb::Slice oval = ExtractColumn(oslice, indexed_column);
      rocksdb::Slice nval = ExtractColumn(nslice, indexed_column);
      if (!keychanged && SlicesEq(oval, nval)) {
        continue;
      }
      index.Delete(oval, shard_name, opk);
      index.Add(nval, shard_name, npk);
    }
  }
  // TODO(babman): need to handle returning updates.
  if (stmt.returning()) {
    return sql::SqlResult(
        sql::SqlResultSet(db_schema, std::move(records), std::move(okeys)));
  } else {
    return SqlResult(rows.size());
  }
}

InsertResult RocksdbConnection::ExecuteReplace(const sqlast::Insert &stmt,
                                               const std::string &shard_name) {
  ShardID sid = shard_name + __ROCKSSEP;

  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.VisitInsert(stmt);

  std::string pk_value = "";
  uint64_t last_insert_id = 0;
  if (value_mapper.HasAfter(db_schema.NameOf(pk))) {
    if (this->auto_increment_counters_.count(tid) == 1) {
      LOG(FATAL) << "Insert has value for PK but it is auto increment!";
    }
    const auto &vals = value_mapper.After(db_schema.NameOf(pk));
    CHECK_EQ(vals.size(), 1u) << "Insert has no value for PK!";
    pk_value = vals.at(0).ToString();
    CHECK(!IsNull(pk_value.data(), pk_value.size())) << "Insert has NULL PK!";
  } else {
    if (this->auto_increment_counters_.count(tid) == 0) {
      LOG(FATAL) << "Insert has no value for PK and not auto increment!";
    }
    last_insert_id = ++(this->auto_increment_counters_.at(tid));
    pk_value = std::to_string(last_insert_id);
  }
  // Delete old columns with same pk value
  auto result = this->GetRecords(tid, value_mapper, false);
  auto rows = result.first;
  auto shards = result.second;
  // TO-DO: senity check, remove this line
  assert(rows.size() == 1);
  rocksdb::Slice keyslice = ExtractColumn(rows[0], pk);
  rocksdb::Slice slice(rows[0]);
  if (shards[0] != shard_name) {  // need to delete
    auto skey = shards[0] + __ROCKSSEP + keyslice.ToString() + __ROCKSSEP;
    PANIC(this->db_->Delete(rocksdb::WriteOptions(), handler,
                            EncryptKey(this->global_key_, skey)));
  }
  // Update Indexes
  for (size_t i = 0; i < indexed_columns.size(); i++) {
    RocksdbIndex &index = indices.at(i);
    size_t indexed_column = indexed_columns.at(i);
    index.Delete(ExtractColumn(slice, indexed_column), shards[0], keyslice);
  }

  // Insert new row.

  std::string skey = sid + pk_value + __ROCKSSEP;
  std::string row = EncodeInsert(stmt, db_schema, pk_value);
  auto opts = rocksdb::WriteOptions();
  opts.sync = true;
  PANIC(this->db_->Put(opts, handler, EncryptKey(this->global_key_, skey),
                       Encrypt(this->GetUserKey(shard_name), row)));

  // Update indices.
  rocksdb::Slice pkslice(pk_value.data(), pk_value.size());
  rocksdb::Slice rslice(row);
  for (size_t i = 0; i < indexed_columns.size(); i++) {
    RocksdbIndex &index = indices.at(i);
    size_t indexed_column = indexed_columns.at(i);
    index.Add(ExtractColumn(rslice, indexed_column), shard_name, pkslice);
  }
  return {1, last_insert_id};
}

// Execute Delete statements
SqlResult RocksdbConnection::ExecuteDelete(const sqlast::Delete &stmt) {
  // Read table metadata.
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.VisitDelete(stmt);
  // Look up all affected rows.
  std::pair<std::vector<std::string>, std::vector<std::string>> result =
      this->GetRecords(tid, value_mapper, true);
  std::vector<std::string> shards = result.second;
  std::vector<std::string> rows = result.first;

  // Result components
  std::vector<dataflow::Record> records;
  std::vector<std::string> keys;

  rows = this->Filter(db_schema, &stmt, std::move(rows));
  auto rsize = rows.size();
  if (rsize > 5) {
    LOG(WARNING) << "Perf Warning: " << rsize << " rocksdb deletes in a loop "
                 << stmt;
  }
  for (int i = 0; i < rsize; i++) {
    std::string &row = rows[i];
    std::string &shard_name = shards[i];

    rocksdb::Slice slice(row);
    // Get the key of the existing row.
    rocksdb::Slice keyslice = ExtractColumn(row, pk);
    if (stmt.returning()) {
      keys.push_back(keyslice.ToString());
      records.push_back(DecodeRecord(slice, db_schema, {}, {}));
    }
    // Delete the existing row by key.
    std::string skey =
        shard_name + __ROCKSSEP + keyslice.ToString() + __ROCKSSEP;
    PANIC(this->db_->Delete(rocksdb::WriteOptions(), handler,
                            EncryptKey(this->global_key_, skey)));

    // Update indices.
    for (size_t i = 0; i < indexed_columns.size(); i++) {
      RocksdbIndex &index = indices.at(i);
      size_t indexed_column = indexed_columns.at(i);
      index.Delete(ExtractColumn(slice, indexed_column), shard_name, keyslice);
    }
  }
  if (stmt.returning()) {
    return sql::SqlResult(
        sql::SqlResultSet(db_schema, std::move(records), std::move(keys)));
  } else {
    return SqlResult(rsize);
  }
  // TODO(babman): can do this better.
  // TODO(babman): need to handle returning deletes.
}

/*
 * SELECT STATEMENTS.
 */

SqlResultSet RocksdbConnection::ExecuteSelect(
    const sqlast::Select &stmt, const dataflow::SchemaRef &out_schema,
    const std::vector<AugInfo> &augments) {
  CHECK_LE(augments.size(), 1u) << "Too many augmentations";

  // Read table metadata.
  // TO-DO: check for redundancy with Get
  const std::string &table_name = stmt.table_name();
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.VisitSelect(stmt);

  // Result components.
  std::vector<dataflow::Record> records;
  std::vector<std::string> pkeys;

  // Figure out the projections (including order).
  bool has_projection = stmt.GetColumns().at(0) != "*";
  std::vector<int> projections;
  if (has_projection) {
    projections = ProjectionSchema(db_schema, out_schema, augments);
  }

  // Look up all the rows.
  auto result = this->GetRecords(tid, value_mapper, false).first;
  result = this->Filter(db_schema, &stmt, std::move(result));
  for (std::string &row : result) {
    rocksdb::Slice slice(row);
    rocksdb::Slice key = ExtractColumn(row, pk);
    pkeys.push_back(key.ToString());
    records.push_back(DecodeRecord(slice, out_schema, augments, projections));
  }
  return SqlResultSet(out_schema, std::move(records), std::move(pkeys));
}

// Helpers.
// Get record matching values in a value mapper (either by key, index, or it).
std::vector<std::string> RocksdbConnection::GetShard(
    const sqlast::AbstractStatement *stmt, TableID table_id,
    const std::string &shard_name, const ValueMapper &value_mapper) {
  ShardID shard_id = shard_name + __ROCKSSEP;
  // Read Metadata.
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(table_id).get();
  const dataflow::SchemaRef &schema = this->schemas_.at(table_id);
  size_t pk = this->primary_keys_.at(table_id);
  const auto &indexed_columns = this->indexed_columns_.at(table_id);
  std::vector<RocksdbIndex> &indices = this->indices_.at(table_id);
  const std::string &pkname = schema.NameOf(pk);

  // Lookup either by PK or index.
  bool found = false;
  std::vector<std::string> keys;
  if (value_mapper.HasBefore(pkname)) {
    // Use the PK value(s).
    const std::vector<rocksdb::Slice> &slices = value_mapper.Before(pkname);
    for (const auto &slice : slices) {
      keys.push_back(shard_id + slice.ToString() + __ROCKSSEP);
    }
    found = true;
  } else {
    // No PK, use any index.
    for (size_t i = 0; i < indexed_columns.size(); i++) {
      RocksdbIndex &index = indices.at(i);
      const std::string &idxcolumn = schema.NameOf(indexed_columns.at(i));
      if (value_mapper.HasBefore(idxcolumn)) {
        keys = index.GetShard(value_mapper.Before(idxcolumn), shard_name);
        for (size_t i = 0; i < keys.size(); i++) {
          keys[i] = shard_id + keys[i] + __ROCKSSEP;
        }
        found = true;
        break;
      }
    }
  }

  // Look in rocksdb.
  std::vector<std::string> result;
  if (found) {
    if (keys.size() == 1) {
      std::string str;
      rocksdb::ReadOptions opts;
      opts.total_order_seek = true;
      opts.verify_checksums = false;
      rocksdb::Status status = this->db_->Get(
          opts, handler, EncryptKey(this->global_key_, keys.front()), &str);
      if (status.ok()) {
        result.push_back(std::move(Decrypt(this->GetUserKey(shard_name), str)));
      } else if (!status.IsNotFound()) {
        PANIC(status);
      }
    } else if (keys.size() > 1) {
      // Make slices for keys.
      rocksdb::Slice *slices = new rocksdb::Slice[keys.size()];
      std::string *tmp = new std::string[keys.size()];
      rocksdb::PinnableSlice *pins = new rocksdb::PinnableSlice[keys.size()];
      rocksdb::Status *statuses = new rocksdb::Status[keys.size()];
      for (size_t i = 0; i < keys.size(); i++) {
        keys.at(i) = EncryptKey(this->global_key_, keys.at(i));
        slices[i] = rocksdb::Slice(keys.at(i));
        pins[i] = rocksdb::PinnableSlice(tmp + i);
      }
      // Use MultiGet.
      rocksdb::ReadOptions opts;
      opts.total_order_seek = true;
      opts.verify_checksums = false;
      this->db_->MultiGet(opts, handler, keys.size(), slices, pins, statuses);
      // Read values that were found.
      for (size_t i = 0; i < keys.size(); i++) {
        rocksdb::Status &status = statuses[i];
        if (status.ok()) {
          if (pins[i].IsPinned()) {
            tmp[i].assign(pins[i].data(), pins[i].size());
          }
          result.push_back(
              std::move(Decrypt(this->GetUserKey(shard_name), tmp[i])));
        } else if (!status.IsNotFound()) {
          PANIC(status);
        }
      }
      // Cleanup memory.
      delete[] statuses;
      delete[] pins;
      delete[] tmp;
      delete[] slices;
    }
  } else {
    // Need to lookup all records.
    if (HasWhereClause(stmt)) {
      LOG(WARNING) << "Perf Warning: Query has no rocksdb index " << *stmt;
    }
    rocksdb::ReadOptions options;
    options.fill_cache = false;
    options.total_order_seek = false;
    options.prefix_same_as_start = true;
    options.verify_checksums = false;
    auto ptr = this->db_->NewIterator(options, handler);
    std::unique_ptr<rocksdb::Iterator> it(ptr);
    for (it->Seek(EncryptSeek(this->global_key_, shard_id)); it->Valid();
         it->Next()) {
      result.push_back(
          Decrypt(this->GetUserKey(shard_name), it->value().ToString()));
    }
  }
  return result;
}

// Get record matching values in a value mapper FROM ALL SHARDS(either by key,
// index, or it).
std::pair<std::vector<std::string>, std::vector<std::string>>
RocksdbConnection::GetRecords(TableID table_id, const ValueMapper &value_mapper,
                              bool return_shards) {
  // Read Metadata.
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(table_id).get();
  const dataflow::SchemaRef &schema = this->schemas_.at(table_id);
  size_t pk = this->primary_keys_.at(table_id);
  const auto &indexed_columns = this->indexed_columns_.at(table_id);
  std::vector<RocksdbIndex> &indices = this->indices_.at(table_id);
  const std::string &pkname = schema.NameOf(pk);

  // Calling KeyFinder
  auto key_info = KeyFinder(table_id, value_mapper);
  auto keys_raw = key_info.second;
  std::vector<std::string> keys;
  std::vector<std::string> shards;
  std::vector<std::string> result;
  std::cout << "keyinfo: " << key_info.first << std::endl;
  if (key_info.first) {
    if (return_shards) {
      for (size_t i = 0; i < keys_raw.size(); i++) {
        // TO-DO: does repeated access slow performance
        keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
                       __ROCKSSEP);
        shards.push_back(keys_raw[i].first);
      }
    } else {
      for (size_t i = 0; i < keys_raw.size(); i++) {
        keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
                       __ROCKSSEP);
      }
    }

    // Look in rocksdb.
    if (keys.size() == 1) {
      std::string str;
      rocksdb::ReadOptions opts;
      opts.total_order_seek = true;
      opts.verify_checksums = false;

      rocksdb::Status status = this->db_->Get(
          opts, handler, EncryptKey(this->global_key_, keys.front()), &str);
      if (status.ok()) {
        // TO-DO - could be optimised
        result.push_back(std::move(Decrypt(
            this->GetUserKey(keys[0].substr(0, keys[0].find(__ROCKSSEP))),
            str)));
      } else if (!status.IsNotFound()) {
        PANIC(status);
      }
    } else if (keys.size() > 1) {
      // Make slices for keys.
      rocksdb::Slice *slices = new rocksdb::Slice[keys.size()];
      std::string *tmp = new std::string[keys.size()];
      rocksdb::PinnableSlice *pins = new rocksdb::PinnableSlice[keys.size()];
      rocksdb::Status *statuses = new rocksdb::Status[keys.size()];
      for (size_t i = 0; i < keys.size(); i++) {
        keys.at(i) = EncryptKey(this->global_key_, keys.at(i));
        slices[i] = rocksdb::Slice(keys.at(i));
        pins[i] = rocksdb::PinnableSlice(tmp + i);
      }
      // Use MultiGet.
      rocksdb::ReadOptions opts;
      opts.total_order_seek = true;
      opts.verify_checksums = false;
      this->db_->MultiGet(opts, handler, keys.size(), slices, pins, statuses);
      // Read values that were found.
      for (size_t i = 0; i < keys.size(); i++) {
        rocksdb::Status &status = statuses[i];
        if (status.ok()) {
          if (pins[i].IsPinned()) {
            tmp[i].assign(pins[i].data(), pins[i].size());
          }
          result.push_back(std::move(Decrypt(
              this->GetUserKey(keys[i].substr(0, keys[i].find(__ROCKSSEP))),
              tmp[i])));
        } else if (!status.IsNotFound()) {
          PANIC(status);
        }
      }
      // Cleanup memory.
      delete[] statuses;
      delete[] pins;
      delete[] tmp;
      delete[] slices;
    }
  } else {
    LOG(FATAL) << "case not implemented!";
  }

  return {result, shards};
}

// Filter records by where clause in abstract statement.
std::vector<std::string> RocksdbConnection::Filter(
    const dataflow::SchemaRef &schema, const sqlast::AbstractStatement *sql,
    std::vector<std::string> &&rows) {
  std::vector<std::string> filtered;
  filtered.reserve(rows.size());
  for (std::string &row : rows) {
    FilterVisitor filter(rocksdb::Slice(row), schema);
    if (filter.Visit(sql)) {
      filtered.emplace_back(std::move(row));
    }
  }
  return filtered;
}

std::pair<bool, std::vector<std::pair<std::string, std::string>>>
RocksdbConnection::KeyFinder(TableID table_id,
                             const ValueMapper &value_mapper) {
  // Read Metadata.
  const dataflow::SchemaRef &schema = this->schemas_.at(table_id);
  size_t pk = this->primary_keys_.at(table_id);
  const auto &indexed_columns = this->indexed_columns_.at(table_id);
  std::vector<RocksdbIndex> &indices = this->indices_.at(table_id);
  const std::string &pkname = schema.NameOf(pk);

  // Lookup either by PK or index.
  std::vector<std::pair<std::string, std::string>> keys;
  // std::vector<std::string> keys_raw;

  // TO-DO (Vedant) this is expensive, will that effect performance?
  auto it = std::find(indexed_columns.begin(), indexed_columns.end(), pk);
  std::cout << "hasbefore pk: " << value_mapper.HasBefore(pkname) << std::endl;
  if (it != indexed_columns.end() && value_mapper.HasBefore(pkname)) {
    auto i = it - indexed_columns.begin();
    RocksdbIndex &index = indices.at(i);
    keys = index.Get(value_mapper.Before(pkname));
    // for (size_t i = 0; i < keys_raw.size(); i++) {
    //   keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
    //                  __ROCKSSEP);
    // }
    return {true, keys};
  }
  for (size_t i = 0; i < indexed_columns.size(); i++) {
    RocksdbIndex &index = indices.at(i);
    const std::string &idxcolumn = schema.NameOf(indexed_columns.at(i));
    std::cout << "hasbefore other: " << value_mapper.HasBefore(idxcolumn)
              << std::endl;
    if (value_mapper.HasBefore(idxcolumn)) {
      keys = index.Get(value_mapper.Before(idxcolumn));
      // for (size_t i = 0; i < keys_raw.size(); i++) {
      //   keys.push_back(keys_raw[i].first + __ROCKSSEP + keys_raw[i].second +
      //                  __ROCKSSEP);
      // }
      return {true, keys};
    }
  }
  return {false, {}};
}

// Gets key corresponding to input user. Creates key if does not exist.
unsigned char *RocksdbConnection::GetUserKey(const std::string &shard_name) {
#ifdef PELTON_ENCRYPTION
  shards::SharedLock lock(&this->user_keys_mtx_);
  std::unordered_map<std::string, unsigned char *>::const_iterator it =
      this->user_keys_.find(shard_name);
  if (it == this->user_keys_.cend()) {
    shards::UniqueLock upgraded(std::move(lock));
    it = this->user_keys_.find(shard_name);
    if (it == this->user_keys_.cend()) {
      unsigned char *key = KeyGen();
      this->user_keys_.emplace(shard_name, key);
      return key;
    }
    return it->second;
  }
  return it->second;
#else
  return nullptr;
#endif  // PELTON_ENCRYPTION
}

}  // namespace sql
}  // namespace pelton
