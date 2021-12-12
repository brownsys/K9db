#include "pelton/sql/connections/rocksdb_connection.h"

#include <utility>

#include "glog/logging.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

#define PANIC PANIC_STATUS

namespace pelton {
namespace sql {

namespace {

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
  std::string path = "/var/pelton/rocksdb/" + db_name;
  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC(rocksdb::DB::Open(opts, path, &db));
  this->db_ = std::unique_ptr<rocksdb::DB>(db);
}

// Close the connection.
void SingletonRocksdbConnection::Close() {
  if (this->db_ != nullptr) {
    this->indices_.clear();
    this->handlers_.clear();
    this->db_ = nullptr;
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

        // Creating the table's column family.
        rocksdb::ColumnFamilyHandle *handle;
        PANIC(this->db_->CreateColumnFamily(rocksdb::ColumnFamilyOptions(),
                                            table_name, &handle));

        // Fill in table metadata.
        TableID tid = this->tables_.size();
        this->tables_.emplace(table_name, tid);
        this->handlers_.emplace(tid, handle);
        this->schemas_.emplace(tid, schema);
        this->primary_keys_.emplace(tid, pk);
        this->indexed_columns_.emplace(tid, std::vector<size_t>{});
        this->indices_.emplace(tid, std::vector<RocksdbIndex>{});

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
      ShardID sid = this->GetOrCreateShardID(shard_name);
      // if we are not replacing, then we will need to update indices for
      // the insert only (nothing is removed).
      // if no indices, then we do not need to know what rows got replaced.
      const auto &vals = value_mapper.After(db_schema.NameOf(pk));
      CHECK_EQ(vals.size(), 1u) << "Insert has no value for PK!";
      const rocksdb::Slice &pkslice = vals.at(0);
      CHECK(!IsNull(pkslice.data(), pkslice.size())) << "Insert has NULL PK!";
      std::string skey = sid + pkslice.ToString();

      // Insert/replace new row.
      std::string row = EncodeInsert(*stmt, db_schema);
      PANIC(this->db_->Put(rocksdb::WriteOptions(), handler, skey, row));

      // Update indices.
      rocksdb::Slice rslice(row);
      for (size_t i = 0; i < indexed_columns.size(); i++) {
        RocksdbIndex &index = indices.at(i);
        size_t indexed_column = indexed_columns.at(i);
        std::string index_value =
            sid + ExtractColumn(rslice, indexed_column).ToString();
        index.Add(index_value, rocksdb::Slice(skey));
      }
      return 1;
    }
    case sqlast::AbstractStatement::Type::UPDATE: {
      const sqlast::Update *stmt = static_cast<const sqlast::Update *>(sql);

      // Get the shard ID.
      std::optional<ShardID> sid = this->GetShardID(shard_name);
      if (!sid) {
        return 0;
      }

      // Turn update effects into an easy to process form.
      std::unordered_map<std::string, std::string> update;
      auto &columns = stmt->GetColumns();
      auto &values = stmt->GetValues();
      for (size_t i = 0; i < columns.size(); i++) {
        update.emplace(columns.at(i), values.at(i));
      }

      // Look up all affected rows.
      std::vector<std::string> rows = this->Get(tid, *sid, value_mapper);
      rows = this->Filter(db_schema, sql, std::move(rows));
      for (std::string &row : rows) {
        // Compute updated row.
        std::string nrow = Update(update, db_schema, row);
        if (nrow == row) {  // No-op.
          continue;
        }
        rocksdb::Slice oslice(row);
        rocksdb::Slice nslice(nrow);
        std::string okey = *sid + ExtractColumn(oslice, pk).ToString();
        std::string nkey = *sid + ExtractColumn(nslice, pk).ToString();
        bool keychanged = okey != nkey;
        // If the PK is unchanged, we do not need to delete, and can replace.
        if (keychanged) {
          PANIC(db_->Delete(rocksdb::WriteOptions(), handler, okey));
        }
        PANIC(db_->Put(rocksdb::WriteOptions(), handler, nkey, nrow));
        // Update indices.
        for (size_t i = 0; i < indexed_columns.size(); i++) {
          RocksdbIndex &index = indices.at(i);
          size_t indexed_column = indexed_columns.at(i);
          rocksdb::Slice oval = ExtractColumn(oslice, indexed_column);
          rocksdb::Slice nval = ExtractColumn(nslice, indexed_column);
          if (!keychanged && SlicesEq(oval, nval)) {
            continue;
          }
          std::string ovalstr = *sid + oval.ToString();
          std::string nvalstr = *sid + nval.ToString();
          index.Delete(ovalstr, okey);
          index.Add(nvalstr, nkey);
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

      // Get the shard ID.
      std::optional<ShardID> sid = this->GetShardID(shard_name);
      if (!sid) {
        break;
      }

      // Figure out the projections (including order).
      bool has_projection = stmt->GetColumns().at(0) != "*";
      std::vector<int> projections;
      if (has_projection) {
        projections = ProjectionSchema(db_schema, out_schema, augments);
      }

      // Look up all the rows.
      std::vector<std::string> rows = this->Get(tid, *sid, value_mapper);
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
      // Get the shard ID.
      std::optional<ShardID> sid = this->GetShardID(shard_name);
      if (!sid) {
        break;
      }
      // Look up all the rows.
      std::vector<std::string> rows = this->Get(tid, *sid, value_mapper);
      rows = this->Filter(db_schema, sql, std::move(rows));
      for (std::string &row : rows) {
        rocksdb::Slice slice(row);
        // Get the key of the existing row.
        rocksdb::Slice keyslice = ExtractColumn(row, pk);
        keys.push_back(keyslice.ToString());
        records.push_back(DecodeRecord(slice, out_schema, augments, {}));
        // Delete the existing row by key.
        std::string skey = *sid + keyslice.ToString();
        PANIC(this->db_->Delete(rocksdb::WriteOptions(), handler, skey));
        // Update indices.
        for (size_t i = 0; i < indexed_columns.size(); i++) {
          RocksdbIndex &index = indices.at(i);
          size_t indexed_column = indexed_columns.at(i);
          std::string index_value =
              *sid + ExtractColumn(slice, indexed_column).ToString();
          index.Delete(*sid, skey);
        }
      }
      break;
    }
    case sqlast::AbstractStatement::Type::INSERT: {
      const sqlast::Insert *stmt = static_cast<const sqlast::Insert *>(sql);

      // Get the shard id.
      std::optional<ShardID> opt = this->GetShardID(shard_name);
      ShardID sid = opt ? *opt : this->GetOrCreateShardID(shard_name);

      // A replacing AND returning INSERT.
      // Make sure insert provides a correct PK.
      const auto &vals = value_mapper.After(db_schema.NameOf(pk));
      CHECK_EQ(vals.size(), 1u) << "Insert has no value for PK!";
      const rocksdb::Slice &pkslice = vals.at(0);
      CHECK(!IsNull(pkslice.data(), pkslice.size())) << "Insert has NULL PK!";

      // Lookup existing row.
      std::string skey = sid + pkslice.ToString();
      std::string orow;
      rocksdb::Slice oslice(nullptr, 0);
      if (opt) {
        auto st = this->db_->Get(rocksdb::ReadOptions(), handler, skey, &orow);
        if (st.ok()) {
          // There is a pre-existing row with that PK (that will get replaced).
          oslice = rocksdb::Slice(orow);
          keys.push_back(pkslice.ToString());
          records.push_back(DecodeRecord(oslice, out_schema, augments, {}));
        } else if (!st.IsNotFound()) {
          PANIC(st);
        }
      }

      // Insert/replace new row.
      std::string nrow = EncodeInsert(*stmt, db_schema);
      rocksdb::Slice nslice(nrow);
      if (SlicesEq(oslice, nslice)) {  // No-op.
        break;
      }
      PANIC(this->db_->Put(rocksdb::WriteOptions(), handler, skey, nrow));
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
          index.Delete(sid + oval.ToString(), rocksdb::Slice(skey));
        }
        index.Add(sid + nval.ToString(), rocksdb::Slice(skey));
      }
      break;
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteQuery";
  }
  return SqlResultSet(out_schema, std::move(records), std::move(keys));
}

// Helpers.
// Get record matching values in a value mapper (either by key, index, or it).
std::vector<std::string> SingletonRocksdbConnection::Get(
    TableID table_id, ShardID shard_id, const ValueMapper &value_mapper) {
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
      keys.push_back(shard_id + slice.ToString());
    }
    found = true;
  } else {
    // No PK, use any index.
    for (size_t i = 0; i < indexed_columns.size(); i++) {
      RocksdbIndex &index = indices.at(i);
      const std::string &idxcolumn = schema.NameOf(indexed_columns.at(i));
      if (value_mapper.HasBefore(idxcolumn)) {
        keys = index.Get(shard_id, value_mapper.Before(idxcolumn));
        found = true;
        break;
      }
    }
  }

  // Look in rocksdb.
  std::vector<std::string> result;
  if (found) {
    // Can look up by keys directly.
    for (const std::string &skey : keys) {
      std::string str;
      rocksdb::Status status =
          this->db_->Get(rocksdb::ReadOptions(), handler, skey, &str);
      if (status.ok()) {
        result.push_back(std::move(str));
      } else if (!status.IsNotFound()) {
        PANIC(status);
      }
    }
  } else {
    // Need to lookup all records.
    LOG(INFO) << "rocksdb: Query has no rocksdb index";
    auto ptr = this->db_->NewIterator(rocksdb::ReadOptions(), handler);
    std::unique_ptr<rocksdb::Iterator> it(ptr);
    rocksdb::Slice pslice(shard_id);
    for (it->Seek(shard_id); it->Valid(); it->Next()) {
      rocksdb::Slice keyslice = it->key();
      if (keyslice.size() < pslice.size()) {
        break;
      }
      if (!SlicesEq(pslice, rocksdb::Slice(keyslice.data(), pslice.size()))) {
        break;
      }
      result.push_back(it->value().ToString());
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
