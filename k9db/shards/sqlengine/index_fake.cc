// Implements an identical API to index.cc that simulate the known ownCloud
// experiment indices using database queries without creating any actual
// indices.
// We use this when views are turned off to run the drill-down experiment.

// clang-format off
// NOLINTNEXTLINE
#include "k9db/shards/sqlengine/index.h"
// clang-format on

#include <unordered_set>
#include <utility>

#include "k9db/dataflow/schema.h"
#include "k9db/shards/state.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "k9db/util/shard_name.h"

namespace k9db {
namespace shards {
namespace sqlengine {
namespace index {

// Fake index creation
absl::StatusOr<IndexDescriptor> Create(const std::string &table_name,
                                       const std::string &shard_kind,
                                       const std::string &column_name,
                                       Connection *connection,
                                       util::UniqueLock *lock) {
  // Create a unique name for the index.
  uint64_t idx = connection->state->SharderState().IncrementIndexCount();
  std::string index_name = "_index_" + std::to_string(idx);

  // Return index descriptor.
  return IndexDescriptor{index_name, table_name, shard_kind, column_name};
}

/*
 * Helpers for simulating indices.
 */

namespace {

using EType = sqlast::Expression::Type;
using CType = sqlast::ColumnDefinition::Type;

// Find the members of a group.
std::vector<dataflow::Record> FindUsersInGroup(sqlast::Value &&gid,
                                               Connection *connection,
                                               util::SharedLock *lock) {
  // SELECT uid from oc_group_user.
  sqlast::Select select("oc_group_user");
  select.AddColumn(sqlast::Select::ResultColumn("uid"));
  // WHERE gid = <gid>.
  auto ptr = std::make_unique<sqlast::BinaryExpression>(EType::EQ);
  ptr->SetLeft(std::make_unique<sqlast::ColumnExpression>("gid"));
  ptr->SetRight(std::make_unique<sqlast::LiteralExpression>(gid));
  select.SetWhereClause(std::move(ptr));

  // Execute query against database.
  sql::Session *db = connection->session.get();
  sql::SqlResultSet resultset = db->ExecuteSelect(select);

  // Create the schema.
  std::vector<std::string> colnames = {"_col", "_shard", "_count"};
  std::vector<CType> coltypes = {gid.ConvertType(), CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> colkeys = {0u, 1u};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(colnames, coltypes, colkeys);

  // Format results as if returned by an index.
  std::vector<dataflow::Record> records;
  for (const dataflow::Record &record : resultset.rows()) {
    records.emplace_back(schema, true);
    records.back().SetValue(gid, 0);
    records.back().SetValue(record.GetValue(0), 1);
    records.back().SetInt(1, 2);
  }
  return records;
}

// Find the owners of a file given its ID.
// Works independently of whether the schema has the shares as owners or
// or only accessors.
std::vector<dataflow::Record> FindOwnersOfFile(sqlast::Value &&fileid,
                                               Connection *connection,
                                               util::SharedLock *lock) {
  // Find owners using DB.
  sql::Session *db = connection->session.get();
  std::unordered_set<util::ShardName> shards =
      db->FindShards("oc_files", 0, fileid);

  // Create the schema.
  std::vector<std::string> colnames = {"_col", "_shard", "_count"};
  std::vector<CType> coltypes = {fileid.ConvertType(), CType::TEXT, CType::INT};
  std::vector<dataflow::ColumnID> colkeys = {0u, 1u};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(colnames, coltypes, colkeys);

  // Return result formatted as if it is an index.
  std::vector<dataflow::Record> result;
  for (const util::ShardName &shard : shards) {
    std::string_view userid = shard.UserID();
    if (userid != DEFAULT_SHARD) {
      result.emplace_back(schema, true);
      result.back().SetValue(fileid, 0);
      result.back().SetString(std::make_unique<std::string>(userid), 1);
      result.back().SetInt(1, 2);
    }
  }
  return result;
}

}  // namespace

// Fake index query: results in some queries against the database.
std::vector<dataflow::Record> LookupIndex(const IndexDescriptor &index,
                                          sqlast::Value &&value,
                                          Connection *connection,
                                          util::SharedLock *lock) {
  const std::string &table_name = index.table_name;
  const std::string &column_name = index.column_name;
  if (table_name == "oc_groups") {
    // Find users in group with gid = value.
    CHECK_EQ(column_name, "gid");
    return FindUsersInGroup(std::move(value), connection, lock);
  }
  if (table_name == "oc_files") {
    // Find owners of file whose id = value.
    CHECK_EQ(column_name, "id");
    return FindOwnersOfFile(std::move(value), connection, lock);
  }
  LOG(FATAL) << "Unrecognized fake index";
  return std::vector<dataflow::Record>();
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
