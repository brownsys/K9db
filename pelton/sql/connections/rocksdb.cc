#include "pelton/sql/connections/rocksdb.h"

#include "glog/logging.h"
#include "pelton/sql/connections/rocksdb_util.h"
#include "pelton/util/status.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

namespace pelton {
namespace sql {

// SingletonRocksdbConnection.
SingletonRocksdbConnection::SingletonRocksdbConnection(
    const std::string &path) {
  // Options.
  rocksdb::Options opts;
  opts.create_if_missing = true;
  opts.error_if_exists = true;

  // Open the database.
  rocksdb::DB *db;
  PANIC_STATUS(rocksdb::DB::Open(opts, path, &db));
  this->db_ = std::unique_ptr<rocksdb::DB>(db);
}

// Close the connection.
void SingletonRocksdbConnection::Close() {
  if (this->db_ != nullptr) {
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

      // Schema and PK.
      dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(*stmt);
      const std::vector<dataflow::ColumnID> &keys = schema.keys();
      CHECK_EQ(keys.size(), 1u) << "BAD PK ROCKSDB TABLE";
      size_t pk = keys.at(0);

      // Create table.
      std::string table_name = shard_name + stmt->table_name();
      rocksdb::ColumnFamilyOptions options;
      rocksdb::ColumnFamilyHandle *handle;
      PANIC_STATUS(this->db_->CreateColumnFamily(options, table_name, &handle));
      this->handlers_.emplace(table_name, handle);
      this->schemas_.emplace(table_name, schema);
      this->primary_keys_.emplace(table_name, pk);
      this->indicies_.emplace(table_name, std::unordered_set<size_t>{});
      break;
    }

    // Create Index.
    case sqlast::AbstractStatement::Type::CREATE_INDEX: {
      const sqlast::CreateIndex *stmt =
          static_cast<const sqlast::CreateIndex *>(sql);

      std::string table_name = shard_name + stmt->table_name();
      const std::string &col_name = stmt->column_name();
      size_t idx = this->schemas_.at(table_name).IndexOf(col_name);
      if (idx != this->primary_keys_.at(table_name)) {
        // this->indicies_.at(table_name).insert(idx);
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
  size_t rowcount = 0;
  switch (sql->type()) {
    case sqlast::AbstractStatement::Type::INSERT: {
      const sqlast::Insert *stmt = static_cast<const sqlast::Insert *>(sql);

      // Read indices and schema.
      std::string table_name = shard_name + stmt->table_name();
      size_t pk = this->primary_keys_.at(table_name);
      const auto &indices = this->indicies_.at(table_name);
      const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
      rocksdb::ColumnFamilyHandle *handler =
          this->handlers_.at(table_name).get();

      // Find values assigned to pk and indices.
      ValueMapper mapper(pk, indices, schema);
      stmt->Visit(&mapper);

      // Find primary key value.
      std::vector<rocksdb::Slice> &vals = mapper.After(schema.NameOf(pk));
      CHECK_EQ(vals.size(), 1u) << "Insert has no value for PK!";
      rocksdb::Slice &key = vals.at(0);
      CHECK(!IsNull(key.data(), key.size())) << "Insert has NULL for PK!";
      std::string row = EncodeInsert(*stmt, schema);
      PANIC_STATUS(this->db_->Put(rocksdb::WriteOptions(), handler, key, row));
      return 1;
    }
    case sqlast::AbstractStatement::Type::UPDATE: {
      const sqlast::Update *stmt = static_cast<const sqlast::Update *>(sql);
      std::string table_name = shard_name + stmt->table_name();
      size_t pk = this->primary_keys_.at(table_name);
      const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
      rocksdb::ColumnFamilyHandle *handler =
          this->handlers_.at(table_name).get();

      // Turn update effects into an easy to process form.
      std::unordered_map<std::string, std::string> update;
      auto &columns = stmt->GetColumns();
      auto &values = stmt->GetValues();
      for (size_t i = 0; i < columns.size(); i++) {
        update.emplace(columns.at(i), values.at(i));
      }

      std::vector<std::string> rows = this->LookupAndFilter(table_name, sql);
      size_t rowcount = rows.size();
      for (std::string &row : rows) {
        rocksdb::Slice r(row);
        rocksdb::Slice oldkey = ExtractColumn(r, pk);
        std::string updated = Update(update, schema, row);
        rocksdb::Slice v(updated);
        rocksdb::Slice key = ExtractColumn(v, pk);
        if (SlicesEq(oldkey, key)) {
          PANIC_STATUS(db_->Put(rocksdb::WriteOptions(), handler, key, v));
        } else {
          PANIC_STATUS(db_->Delete(rocksdb::WriteOptions(), handler, oldkey));
          PANIC_STATUS(db_->Put(rocksdb::WriteOptions(), handler, key, v));
        }
      }
      return rowcount;
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      const sqlast::Delete *stmt = static_cast<const sqlast::Delete *>(sql);
      std::string table_name = shard_name + stmt->table_name();
      const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
      RecordKeyVecs rows = this->ExecuteQuery(sql, schema, {}, shard_name);
      return rows.records.size();
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteUpdate";
  }
  return rowcount;
}

RecordKeyVecs SingletonRocksdbConnection::ExecuteQuery(
    const sqlast::AbstractStatement *sql, const dataflow::SchemaRef &out_schema,
    const std::vector<AugInfo> &augments, const std::string &shard_name) {
  CHECK_LE(augments.size(), 1) << "Too many augmentations";
  switch (sql->type()) {
    case sqlast::AbstractStatement::Type::SELECT: {
      const sqlast::Select *stmt = static_cast<const sqlast::Select *>(sql);
      std::string table_name = shard_name + stmt->table_name();
      size_t pk = this->primary_keys_.at(table_name);

      // Get projection schema.
      bool has_projection = stmt->GetColumns().at(0) != "*";
      const dataflow::SchemaRef &db_schema = this->schemas_.at(table_name);
      dataflow::SchemaRef projection_schema = out_schema;
      std::vector<int> projections;
      if (has_projection) {
        projections = ProjectionSchema(db_schema, out_schema, augments);
      }

      RecordKeyVecs result;
      std::vector<std::string> rows = this->LookupAndFilter(table_name, sql);
      for (std::string &row : rows) {
        rocksdb::Slice slice(row);
        rocksdb::Slice key = ExtractColumn(row, pk);
        result.keys.push_back(key.ToString());
        result.records.push_back(DecodeRecord(slice, projection_schema, augments, projections));
      }
      return result;
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      const sqlast::Delete *stmt = static_cast<const sqlast::Delete *>(sql);
      std::string table_name = shard_name + stmt->table_name();
      size_t pk = this->primary_keys_.at(table_name);
      rocksdb::ColumnFamilyHandle *handler =
          this->handlers_.at(table_name).get();

      RecordKeyVecs result;
      std::vector<std::string> rows = this->LookupAndFilter(table_name, sql);
      for (std::string &row : rows) {
        rocksdb::Slice slice(row);
        rocksdb::Slice key = ExtractColumn(row, pk);
        // Add record before deleting to returned vector.
        result.keys.push_back(key.ToString());
        result.records.push_back(DecodeRecord(slice, out_schema, augments, {}));
        // Delete record by key.
        PANIC_STATUS(this->db_->Delete(rocksdb::WriteOptions(), handler, key));
      }
      return result;
    }
    case sqlast::AbstractStatement::Type::INSERT: {
      const sqlast::Insert *stmt = static_cast<const sqlast::Insert *>(sql);
      std::string table_name = shard_name + stmt->table_name();
      size_t pk = this->primary_keys_.at(table_name);

      RecordKeyVecs result;
      std::optional<std::string> row = this->ReplacedRow(table_name, sql);
      if (row) {
        rocksdb::Slice slice(*row);
        rocksdb::Slice key = ExtractColumn(slice, pk);
        result.keys.push_back(key.ToString());
        result.records.push_back(DecodeRecord(slice, out_schema, augments, {}));
      }
      this->ExecuteUpdate(sql, shard_name);
      return result;
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteQuery";
  }
  return {};
}

// Helpers.
// Look up all slices and apply filter.
std::vector<std::string> SingletonRocksdbConnection::LookupAndFilter(
    const std::string table_name, const sqlast::AbstractStatement *stmt) {
  // Store the output here.
  std::vector<std::string> result;

  // Find indices and other info about the table.
  const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(table_name).get();
  size_t pk = this->primary_keys_.at(table_name);
  const std::string &pkname = schema.NameOf(pk);
  const auto &indices = this->indicies_.at(table_name);

  // Find values assigned to pk and indices.
  ValueMapper mapper(pk, indices, schema);
  mapper.Visit(stmt);

  // Case 1: look up by primary key.
  std::vector<rocksdb::Slice> &keys = mapper.Before(pkname);
  if (keys.size() > 0) {
    if (keys.size() > 1) {
      LOG(FATAL) << "Multi-values for primary key";
    }
    for (rocksdb::Slice &key : keys) {
      std::string str;
      rocksdb::Status status =
          this->db_->Get(rocksdb::ReadOptions(), handler, key, &str);
      if (status.ok()) {
        FilterVisitor filter(rocksdb::Slice(str), schema);
        if (filter.Visit(stmt)) {
          result.push_back(std::move(str));
        }
      } else if (!status.IsNotFound()) {
        PANIC_STATUS(status);
      }
    }
    return result;
  }

  // Case 2: look up by secondary index.
  for (size_t index : indices) {
    const std::string &idxname = schema.NameOf(index);
    std::vector<rocksdb::Slice> &keys = mapper.Before(idxname);
    if (keys.size() > 0) {
      if (keys.size() > 1) {
        LOG(FATAL) << "Multi-value for index key";
      }
      LOG(FATAL) << "Looking up by index not yet supported!";
    }
  }

  // Case 3: look all up, filter here.
  auto ptr = this->db_->NewIterator(rocksdb::ReadOptions(), handler);
  std::unique_ptr<rocksdb::Iterator> it(ptr);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    rocksdb::Slice row = it->value();
    FilterVisitor filter(row, schema);
    if (filter.Visit(stmt)) {
      result.push_back(std::string(row.data(), row.size()));
    }
  }
  return result;
}
// A replacing insert!
std::optional<std::string> SingletonRocksdbConnection::ReplacedRow(
    const std::string table_name, const sqlast::AbstractStatement *stmt) {
  // Find indices and other info about the table.
  const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
  rocksdb::ColumnFamilyHandle *handler = this->handlers_.at(table_name).get();
  size_t pk = this->primary_keys_.at(table_name);
  const std::string &pkname = schema.NameOf(pk);

  // Find values assigned to pk and indices.
  ValueMapper mapper(pk, {}, schema);
  mapper.Visit(stmt);

  // Case 1: look up by primary key.
  std::vector<rocksdb::Slice> &keys = mapper.After(pkname);
  if (keys.size() != 1) {
    LOG(FATAL) << "Replacing insert has bad assignment to PK";
  }
  std::string str;
  rocksdb::Status status =
      this->db_->Get(rocksdb::ReadOptions(), handler, keys.at(0), &str);
  if (status.ok()) {
    return str;
  } else if (status.IsNotFound()) {
    return {};
  } else {
    PANIC_STATUS(status);
  }
}

// Static singleton.
std::unique_ptr<SingletonRocksdbConnection> RocksdbConnection::INSTANCE =
    nullptr;

}  // namespace sql
}  // namespace pelton
