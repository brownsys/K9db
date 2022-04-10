#include "pelton/sql/connections/rocksdb_connection.h"

#include <unordered_set>
#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"
#include "encryption.h"

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

const std::string &GetTableName(const sqlast::AbstractStatement *stmt) {
  switch (stmt->type()) {
    case sqlast::AbstractStatement::Type::CREATE_TABLE: {
      auto ptr = static_cast<const sqlast::CreateTable *>(stmt);
      return ptr->table_name();
    }
    case sqlast::AbstractStatement::Type::CREATE_INDEX: {
      auto ptr = static_cast<const sqlast::CreateIndex *>(stmt);
      return ptr->table_name();
    }
    case sqlast::AbstractStatement::Type::INSERT: {
      auto ptr = static_cast<const sqlast::Insert *>(stmt);
      return ptr->table_name();
    }
    case sqlast::AbstractStatement::Type::UPDATE: {
      auto ptr = static_cast<const sqlast::Update *>(stmt);
      return ptr->table_name();
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      auto ptr = static_cast<const sqlast::Delete *>(stmt);
      return ptr->table_name();
    }
    case sqlast::AbstractStatement::Type::SELECT: {
      auto ptr = static_cast<const sqlast::Select *>(stmt);
      return ptr->table_name();
    }
    default:
      LOG(FATAL) << "UNREACHABLE";
  }
}

}  // namespace

// SingletonRocksdbConnection.
SingletonRocksdbConnection::SingletonRocksdbConnection(
    const std::string &db_name) {
  std::string path = "/tmp/pelton/rocksdb/" + db_name;

  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;
  opts.IncreaseParallelism();
  opts.OptimizeLevelStyleCompaction();
  opts.prefix_extractor.reset(new ShardPrefixTransform(1));
  opts.comparator = rocksdb::BytewiseComparator();

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
void SingletonRocksdbConnection::Close() {
  if (this->db_ != nullptr) {
    this->indices_.clear();
    this->handlers_.clear();
    this->db_ = nullptr;
    delete[] this->global_key_;
  }
}

// Execute statement by type.
bool SingletonRocksdbConnection::ExecuteStatement(
    const sqlast::AbstractStatement *sql, const std::string &shard_name) {
  switch (sql->type()) {
    // Create Table.
    case sqlast::AbstractStatement::Type::CREATE_TABLE: {
      const sqlast::CreateTable *stmt =
          static_cast<const sqlast::CreateTable *>(sql);

      const std::string &table_name = stmt->table_name();
      if (this->tables_.count(table_name) == 0) {
        // Schema and PK.
        dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(*stmt);
        const std::vector<dataflow::ColumnID> &keys = schema.keys();
        CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
        size_t pk = keys.at(0);

        rocksdb::ColumnFamilyHandle *handle;
        rocksdb::ColumnFamilyOptions options;
        options.OptimizeLevelStyleCompaction();
        options.prefix_extractor.reset(new ShardPrefixTransform(1));
        options.comparator = rocksdb::BytewiseComparator();
        PANIC(this->db_->CreateColumnFamily(options, table_name, &handle));

        // Fill in table metadata.
        TableID tid = this->tables_.size();
        this->tables_.emplace(table_name, tid);
        this->handlers_.emplace(tid, handle);
        this->schemas_.emplace(tid, schema);
        this->primary_keys_.emplace(tid, pk);
        this->indexed_columns_.emplace(tid, std::vector<size_t>{});
        this->indices_.emplace(tid, std::vector<RocksdbIndex>{});
        if (stmt->GetColumns().at(pk).auto_increment()) {
          this->auto_increment_counters_.emplace(tid, 0);
        }

        // Create indices for table.
        for (size_t i = 0; i < stmt->GetColumns().size(); i++) {
          const auto &column = stmt->GetColumns().at(i);
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
      break;
    }

    // Create Index.
    case sqlast::AbstractStatement::Type::CREATE_INDEX: {
      const sqlast::CreateIndex *stmt =
          static_cast<const sqlast::CreateIndex *>(sql);

      const std::string &table_name = stmt->table_name();
      TableID tid = this->tables_.at(table_name);
      size_t cid = this->schemas_.at(tid).IndexOf(stmt->column_name());
      std::vector<size_t> &indexed_columns = this->indexed_columns_.at(tid);
      auto it = std::find(indexed_columns.begin(), indexed_columns.end(), cid);
      if (it == indexed_columns.end()) {
        std::vector<RocksdbIndex> &indices = this->indices_.at(tid);
        indexed_columns.push_back(cid);
        indices.emplace_back(this->db_.get(), tid, cid);
      }
      break;
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteStatement";
  }
  return true;
}

int SingletonRocksdbConnection::ExecuteUpdate(
    const sqlast::AbstractStatement *sql, const std::string &shard_name) {
  ShardID sid = shard_name + __ROCKSSEP;
  // Read table metadata.
  const std::string &table_name = GetTableName(sql);
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.Visit(sql);

  // Process update per type.
  switch (sql->type()) {
    case sqlast::AbstractStatement::Type::INSERT: {
      const sqlast::Insert *stmt = static_cast<const sqlast::Insert *>(sql);
      if (stmt->replace() && indexed_columns.size() > 0) {
        // replacing insert with some indices, we need to find existing rows
        // to update indices correctly, might as well use the returning
        // code.
        this->ExecuteQuery(sql, db_schema, {}, shard_name);
        return 1;
      }
      // if we are not replacing, then we will need to update indices for
      // the insert only (nothing is removed).
      // if no indices, then we do not need to know what rows got replaced.
      std::string pk_value = "";
      if (value_mapper.HasAfter(db_schema.NameOf(pk))) {
        if (this->auto_increment_counters_.count(tid) == 1) {
          LOG(FATAL) << "Insert has value for PK but it is auto increment!";
        }
        const auto &vals = value_mapper.After(db_schema.NameOf(pk));
        CHECK_EQ(vals.size(), 1u) << "Insert has no value for PK!";
        pk_value = vals.at(0).ToString();
        CHECK(!IsNull(pk_value.data(), pk_value.size()))
            << "Insert has NULL PK!";
      } else {
        if (this->auto_increment_counters_.count(tid) == 0) {
          LOG(FATAL) << "Insert has no value for PK and not auto increment!";
        }
        pk_value = std::to_string(++(this->auto_increment_counters_.at(tid)));
      }

      // Insert/replace new row.
      std::string skey = sid + pk_value + __ROCKSSEP;
      std::string row = EncodeInsert(*stmt, db_schema, pk_value);
      auto opts = rocksdb::WriteOptions();
      opts.sync = true;
      PANIC(this->db_->Put(opts, handler, skey, 
                            Encrypt(this->global_key_, row)));

      // Update indices.
      rocksdb::Slice pkslice(pk_value.data(), pk_value.size());
      rocksdb::Slice rslice(row);
      for (size_t i = 0; i < indexed_columns.size(); i++) {
        RocksdbIndex &index = indices.at(i);
        size_t indexed_column = indexed_columns.at(i);
        index.Add(shard_name, ExtractColumn(rslice, indexed_column), pkslice);
      }
      return 1;
    }
    case sqlast::AbstractStatement::Type::UPDATE: {
      const sqlast::Update *stmt = static_cast<const sqlast::Update *>(sql);

      // Turn update effects into an easy to process form.
      std::unordered_map<std::string, std::string> update;
      auto &columns = stmt->GetColumns();
      auto &values = stmt->GetValues();
      for (size_t i = 0; i < columns.size(); i++) {
        update.emplace(columns.at(i), values.at(i));
      }

      // Look up all affected rows.
      std::vector<std::string> rows =
          this->Get(sql, tid, shard_name, value_mapper);
      rows = this->Filter(db_schema, sql, std::move(rows));
      if (rows.size() > 5) {
        LOG(WARNING) << "Perf Warning: " << rows.size()
                     << " rocksdb updates in a loop" << *sql;
      }
      for (std::string &row : rows) {
        // Compute updated row.
        std::string nrow = Update(update, db_schema, row);
        if (nrow == row) {  // No-op.
          continue;
        }
        rocksdb::Slice oslice(row);
        rocksdb::Slice nslice(nrow);
        rocksdb::Slice opk = ExtractColumn(oslice, pk);
        rocksdb::Slice npk = ExtractColumn(nslice, pk);
        std::string okey = sid + opk.ToString() + __ROCKSSEP;
        std::string nkey = sid + npk.ToString() + __ROCKSSEP;
        bool keychanged = !SlicesEq(opk, npk);
        // If the PK is unchanged, we do not need to delete, and can replace.
        if (keychanged) {
          PANIC(db_->Delete(rocksdb::WriteOptions(), handler, okey));
        }
        PANIC(db_->Put(rocksdb::WriteOptions(), handler, nkey, 
                        Encrypt(this->global_key_, nrow)));
        // Update indices.
        for (size_t i = 0; i < indexed_columns.size(); i++) {
          RocksdbIndex &index = indices.at(i);
          size_t indexed_column = indexed_columns.at(i);
          rocksdb::Slice oval = ExtractColumn(oslice, indexed_column);
          rocksdb::Slice nval = ExtractColumn(nslice, indexed_column);
          if (!keychanged && SlicesEq(oval, nval)) {
            continue;
          }
          index.Delete(shard_name, oval, opk);
          index.Add(shard_name, nval, npk);
        }
      }
      return rows.size();
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      auto resultset = this->ExecuteQuery(sql, db_schema, {}, shard_name);
      return resultset.Vec().size();
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteUpdate";
  }
  return -1;
}

SqlResultSet SingletonRocksdbConnection::ExecuteQuery(
    const sqlast::AbstractStatement *sql, const dataflow::SchemaRef &out_schema,
    const std::vector<AugInfo> &augments, const std::string &shard_name) {
  CHECK_LE(augments.size(), 1u) << "Too many augmentations";
  ShardID sid = shard_name + __ROCKSSEP;

  // Read table metadata.
  const std::string &table_name = GetTableName(sql);
  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);
  const auto &indexed_columns = this->indexed_columns_.at(tid);
  std::vector<RocksdbIndex> &indices = this->indices_.at(tid);

  ValueMapper value_mapper(pk, indexed_columns, db_schema);
  value_mapper.Visit(sql);

  // Result components.
  std::vector<dataflow::Record> records;
  std::vector<std::string> keys;

  switch (sql->type()) {
    case sqlast::AbstractStatement::Type::SELECT: {
      const sqlast::Select *stmt = static_cast<const sqlast::Select *>(sql);

      // Figure out the projections (including order).
      bool has_projection = stmt->GetColumns().at(0) != "*";
      std::vector<int> projections;
      if (has_projection) {
        projections = ProjectionSchema(db_schema, out_schema, augments);
      }

      // Look up all the rows.
      std::vector<std::string> rows =
          this->Get(sql, tid, shard_name, value_mapper);
      rows = this->Filter(db_schema, sql, std::move(rows));
      for (std::string &row : rows) {
        rocksdb::Slice slice(row);
        rocksdb::Slice key = ExtractColumn(row, pk);
        keys.push_back(key.ToString());
        records.push_back(
            DecodeRecord(slice, out_schema, augments, projections));
      }
      break;
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      // Look up all the rows.
      std::vector<std::string> rows =
          this->Get(sql, tid, shard_name, value_mapper);
      rows = this->Filter(db_schema, sql, std::move(rows));
      if (rows.size() > 5) {
        LOG(WARNING) << "Perf Warning: " << rows.size()
                     << " rocksdb deletes in a loop" << *sql;
      }
      for (std::string &row : rows) {
        rocksdb::Slice slice(row);
        // Get the key of the existing row.
        rocksdb::Slice keyslice = ExtractColumn(row, pk);
        keys.push_back(keyslice.ToString());
        records.push_back(DecodeRecord(slice, out_schema, augments, {}));
        // Delete the existing row by key.
        std::string skey = sid + keyslice.ToString() + __ROCKSSEP;
        PANIC(this->db_->Delete(rocksdb::WriteOptions(), handler, skey));
        // Update indices.
        for (size_t i = 0; i < indexed_columns.size(); i++) {
          RocksdbIndex &index = indices.at(i);
          size_t indexed_column = indexed_columns.at(i);
          index.Delete(shard_name, ExtractColumn(slice, indexed_column), skey);
        }
      }
      break;
    }
    case sqlast::AbstractStatement::Type::INSERT: {
      const sqlast::Insert *stmt = static_cast<const sqlast::Insert *>(sql);

      // A replacing AND returning INSERT.
      // Make sure insert provides a correct PK.
      const auto &vals = value_mapper.After(db_schema.NameOf(pk));
      CHECK_EQ(vals.size(), 1u) << "Insert has no value for PK!";
      const rocksdb::Slice &pkslice = vals.at(0);
      CHECK(!IsNull(pkslice.data(), pkslice.size())) << "Insert has NULL PK!";

      // Lookup existing row.
      std::string skey = sid + pkslice.ToString() + __ROCKSSEP;
      std::string orow;
      rocksdb::Slice oslice(nullptr, 0);

      rocksdb::ReadOptions opts;
      opts.total_order_seek = true;
      opts.verify_checksums = false;
      auto st = this->db_->Get(opts, handler, skey, &orow);
      if (st.ok()) {
        // There is a pre-existing row with that PK (that will get replaced).
        orow = Decrypt(this->global_key_, orow);
        oslice = rocksdb::Slice(orow.data(), orow.size());
        keys.push_back(pkslice.ToString());
        records.push_back(DecodeRecord(oslice, out_schema, augments, {}));
      } else if (!st.IsNotFound()) {
        PANIC(st);
      }

      // Insert/replace new row.
      std::string nrow = EncodeInsert(*stmt, db_schema, "");
      rocksdb::Slice nslice(nrow);
      if (SlicesEq(oslice, nslice)) {  // No-op.
        break;
      }
      PANIC(this->db_->Put(rocksdb::WriteOptions(), handler, skey, 
                            Encrypt(this->global_key_, nrow)));
      // Update indices.
      for (size_t i = 0; i < indexed_columns.size(); i++) {
        RocksdbIndex &index = indices.at(i);
        size_t indexed_column = indexed_columns.at(i);
        rocksdb::Slice nval = ExtractColumn(nslice, indexed_column);
        if (oslice.data() != nullptr) {
          rocksdb::Slice oval = ExtractColumn(oslice, indexed_column);
          if (SlicesEq(oval, nval)) {
            continue;
          }
          index.Delete(shard_name, oval, pkslice);
        }
        index.Add(shard_name, nval, pkslice);
      }
      break;
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteQuery";
  }
  return SqlResultSet(out_schema, std::move(records), std::move(keys));
}

SqlResultSet SingletonRocksdbConnection::ExecuteQueryAll(
    const sqlast::AbstractStatement *sql, const dataflow::SchemaRef &out_schema,
    const std::vector<AugInfo> &augments) {
  CHECK_LE(augments.size(), 1u) << "Too many augmentations";
  std::vector<AugInfo> augments_copy = augments;
  bool must_augment = augments_copy.size() > 0;

  // Read table metadata.
  const std::string &table_name = GetTableName(sql);
  if (this->tables_.count(table_name) == 0) {
    // The table has not been concretely created yet (lazy on insert)!
    return SqlResultSet(out_schema, {}, {});
  }

  TableID tid = this->tables_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(tid).get();
  const dataflow::SchemaRef &db_schema = this->schemas_.at(tid);
  size_t pk = this->primary_keys_.at(tid);

  // Result components.
  std::vector<dataflow::Record> records;
  std::vector<std::string> keys;
  std::unordered_set<std::string> duplicates;
  switch (sql->type()) {
    case sqlast::AbstractStatement::Type::SELECT: {
      const sqlast::Select *stmt = static_cast<const sqlast::Select *>(sql);

      // Figure out the projections (including order).
      bool has_projection = stmt->GetColumns().at(0) != "*";
      std::vector<int> projections;
      if (has_projection) {
        projections = ProjectionSchema(db_schema, out_schema, augments);
      }

      // Look up all the rows.
      rocksdb::ReadOptions options;
      options.fill_cache = false;
      options.verify_checksums = false;
      auto ptr = this->db_->NewIterator(options, handler);
      std::unique_ptr<rocksdb::Iterator> it(ptr);
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        std::string rowstr = Decrypt(this->global_key_, it->value().ToString());
        rocksdb::Slice rowslice = rocksdb::Slice(rowstr.data(), rowstr.size());
        // Extract PK.
        rocksdb::Slice pk_value = ExtractColumn(rowslice, pk);
        std::string keystr = pk_value.ToString();
        if (duplicates.count(keystr) == 0) {
          keys.push_back(std::move(keystr));
          duplicates.emplace(pk_value.ToString());
          // Figure out the shard_by column if needed to augment into the
          // result.
          if (must_augment) {
            rocksdb::Slice keyslice = it->key();
            rocksdb::Slice aug_value = ExtractColumn(keyslice, 0);
            augments_copy.at(0).value = aug_value.ToString();
          }
          records.push_back(
              DecodeRecord(rowslice, out_schema, augments_copy, projections));
        }
      }
      break;
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteQueryAll";
  }
  return SqlResultSet(out_schema, std::move(records), std::move(keys));
}

// Helpers.
// Get record matching values in a value mapper (either by key, index, or it).
std::vector<std::string> SingletonRocksdbConnection::Get(
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
        keys = index.Get(shard_name, value_mapper.Before(idxcolumn));
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
      rocksdb::Status status =
          this->db_->Get(opts, handler, keys.front(), &str);
      if (status.ok()) {
        result.push_back(std::move(Decrypt(this->global_key_, str)));
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
          result.push_back(std::move(Decrypt(this->global_key_, tmp[i])));
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
    rocksdb::Slice pslice(shard_id);
    for (it->Seek(shard_id); it->Valid(); it->Next()) {
      result.push_back(Decrypt(this->global_key_, it->value().ToString()));
    }
  }
  return result;
}

// Filter records by where clause in abstract statement.
std::vector<std::string> SingletonRocksdbConnection::Filter(
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

// Static singleton.
std::unique_ptr<SingletonRocksdbConnection> RocksdbConnection::INSTANCE =
    nullptr;

}  // namespace sql
}  // namespace pelton
