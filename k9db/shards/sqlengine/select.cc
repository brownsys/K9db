// SELECT statements sharding and rewriting.
#include "k9db/shards/sqlengine/select.h"

#include <utility>

#include "glog/logging.h"
#include "k9db/shards/sqlengine/index.h"
#include "k9db/sql/rocksdb/filter.h"
#include "k9db/sql/rocksdb/project.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

#define __DEF util::ShardName(DEFAULT_SHARD, DEFAULT_SHARD)

/*
 * Main entry point for select: Executes the statement against the shards.
 */
absl::StatusOr<sql::SqlResult> SelectContext::Exec() {
  // Start transaction.
  this->db_->BeginTransaction(false);

  auto result = this->ExecWithinTransaction();

  // Nothing to commit; read only.
  this->db_->RollbackTransaction();

  return result;
}

// Speedup query using analysis of the WHERE condition and in-memory indicies.
std::optional<std::vector<sql::KeyPair>> SelectContext::FindDirectKeys(
    sqlast::ValueMapper *value_mapper) {
  // Optimization in-applicable for scans by definition.
  if (value_mapper->Empty()) {
    return {};
  }
  // Cannot apply optimization if owned in multiple ways.
  // We can improve this later.
  if (this->table_.owners.size() > 1) {
    return {};
  }

  // If the PK is unconstrained, we cannot apply optimization.
  CHECK_EQ(this->schema_.keys().size(), 1u) << "Too many PKs";
  size_t pkcol = this->schema_.keys().front();
  const std::string &pkname = this->schema_.NameOf(pkcol);
  if (!value_mapper->HasValues(pkcol)) {
    return {};
  }

  // Seems like we can apply the optimization. Proceed.
  std::vector<sql::KeyPair> direct_keys;
  std::vector<sqlast::Value> pkvals = value_mapper->ReleaseValues(pkcol);

  // Case 1: not owned.
  if (this->table_.owners.size() == 0) {
    for (sqlast::Value &pkval : pkvals) {
      direct_keys.emplace_back(__DEF, std::move(pkval));
    }
    return direct_keys;
  }

  // Case 2: We have the shard by column specified in the query!
  auto &desc = this->table_.owners.at(0);
  const std::string &shard_kind = desc->shard_kind;
  size_t colidx = EXTRACT_VARIANT(column_index, desc->info);
  if (value_mapper->HasValues(colidx)) {
    std::vector<sqlast::Value> users = value_mapper->ReleaseValues(colidx);
    // Cross product of shards and PKs.
    for (sqlast::Value &user_id : users) {
      std::string uid = user_id.AsUnquotedString();
      for (sqlast::Value &pkval : pkvals) {
        direct_keys.emplace_back(util::ShardName(shard_kind, uid),
                                 sqlast::Value(pkval));
      }
    }
    return direct_keys;
  }

  // Case 3: We have an in-memory index over the PK column.
  auto it = this->table_.indices.find(pkname);
  if (it != this->table_.indices.end()) {
    auto it2 = it->second.find(shard_kind);
    if (it2 != it->second.end()) {
      const IndexDescriptor &index = *it2->second;
      for (sqlast::Value &pk : pkvals) {
        std::vector<dataflow::Record> r = index::LookupIndex(
            index, sqlast::Value(pk), this->conn_, this->lock_);
        if (r.size() > 0) {
          sqlast::Value uid = r.at(0).GetValue(1);
          direct_keys.emplace_back(
              util::ShardName(shard_kind, uid.AsUnquotedString()),
              std::move(pk));
        }
      }
      return direct_keys;
    }
  }

  // No index: cannot apply optimization!
  return {};
}

absl::StatusOr<sql::SqlResult> SelectContext::ExecWithinTransaction() {
  // Make sure table exists in the schema first.
  ASSERT_RET(this->sstate_.TableExists(this->table_name_), InvalidArgument,
             "Table does not exist");

  // Map where conditions to columns.
  sqlast::ValueMapper value_mapper(this->schema_);
  if (this->stmt_.HasWhereClause()) {
    value_mapper.VisitBinaryExpression(*this->stmt_.GetWhereClause());
  }
  std::optional<std::vector<sql::KeyPair>> direct_keys =
      this->FindDirectKeys(&value_mapper);
  if (direct_keys.has_value()) {
    // Lookup direct in the DB.
    size_t pkcol = this->schema_.keys().front();
    std::vector<dataflow::Record> records =
        this->db_->GetDirect(this->table_name_, pkcol, direct_keys.value());
    // Apply any remaining filters.
    if (!value_mapper.Empty()) {
      std::vector<dataflow::Record> filtered;
      for (dataflow::Record &record : records) {
        if (sql::rocks::InMemoryFilter(value_mapper, record)) {
          filtered.push_back(std::move(record));
        }
      }
      records = std::move(filtered);
    }
    // Apply projection, if any.
    sql::rocks::Projection proj =
        sql::rocks::ProjectionSchema(this->schema_, this->stmt_.GetColumns());
    if (proj.schema != this->schema_) {
      std::vector<dataflow::Record> vec;
      for (const dataflow::Record &record : records) {
        vec.push_back(sql::rocks::Project(proj, record));
      }
      return sql::SqlResult(sql::SqlResultSet(proj.schema, std::move(vec)));
    }

    return sql::SqlResult(sql::SqlResultSet(this->schema_, std::move(records)));
  }

  return sql::SqlResult(this->db_->ExecuteSelect(this->stmt_));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
