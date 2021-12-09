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
    const sqlast::AbstractStatement *sql) {
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
      const std::string &table_name = stmt->table_name();
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

      const std::string &table_name = stmt->table_name();
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
    const sqlast::AbstractStatement *sql) {
  size_t rowcount = 0;
  switch (sql->type()) {
    case sqlast::AbstractStatement::Type::INSERT: {
      const sqlast::Insert *stmt = static_cast<const sqlast::Insert *>(sql);

      // Read indices and schema.
      const std::string &table_name = stmt->table_name();
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
      this->db_->Put(rocksdb::WriteOptions(), handler, key, row);
      return 1;
    }
    case sqlast::AbstractStatement::Type::UPDATE: {
      break;
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      break;
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteUpdate";
  }
  return rowcount;
}

std::vector<dataflow::Record> SingletonRocksdbConnection::ExecuteQuery(
    const sqlast::AbstractStatement *sql, const dataflow::SchemaRef &out_schema,
    const std::vector<AugInfo> &augments) {
  CHECK_LE(augments.size(), 1) << "Too many augmentations";
  switch (sql->type()) {
    case sqlast::AbstractStatement::Type::SELECT: {
      const sqlast::Select *stmt = static_cast<const sqlast::Select *>(sql);

      // Read indices and schema.
      const std::string &table_name = stmt->table_name();
      size_t pk = this->primary_keys_.at(table_name);
      const auto &indices = this->indicies_.at(table_name);
      const dataflow::SchemaRef &schema = this->schemas_.at(table_name);
      rocksdb::ColumnFamilyHandle *handler =
          this->handlers_.at(table_name).get();

      // Find values assigned to pk and indices.
      ValueMapper mapper(pk, indices, schema);
      stmt->Visit(&mapper);

      // Lookup by Primary Key.
      std::vector<dataflow::Record> records;
      std::vector<rocksdb::Slice> &keys = mapper.Before(schema.NameOf(pk));
      if (keys.size() > 1) {
        for (rocksdb::Slice &key : keys) {
          std::string str;
          PANIC_STATUS(db_->Get(rocksdb::ReadOptions(), handler, key, &str));
          rocksdb::Slice row{str};
          FilterVisitor filter(rocksdb::Slice(row), schema);
          if (stmt->Visit(&filter)) {
            records.push_back(
                DecodeRecord(rocksdb::Slice(row), out_schema, augments));
          }
        }
        return records;
      }

      // TODO(babman): Lookup by secondary indices

      // Look all up.
      std::unique_ptr<rocksdb::Iterator> it{
          this->db_->NewIterator(rocksdb::ReadOptions(), handler)};
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
        rocksdb::Slice row = it->value();
        FilterVisitor filter(row, schema);
        if (stmt->Visit(&filter)) {
          records.push_back(DecodeRecord(row, out_schema, augments));
        }
      }
      return records;
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      break;
    }
    case sqlast::AbstractStatement::Type::INSERT: {
      break;
    }
    default:
      LOG(FATAL) << "Illegal statement type in ExecuteQuery";
  }
  return {};
}

// Static singleton.
std::unique_ptr<SingletonRocksdbConnection> RocksdbConnection::INSTANCE =
    nullptr;

}  // namespace sql
}  // namespace pelton
