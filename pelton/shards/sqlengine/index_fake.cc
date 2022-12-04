// Fake indices that actually query the underlying database (for owncloud
// experiment).
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "glog/logging.h"
#include "pelton/connection.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/state.h"
#include "pelton/shards/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"
#include "pelton/util/upgradable_lock.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

namespace {

/*
 * Create SQL statements.
 */
using EType = sqlast::Expression::Type;
using CType = sqlast::ColumnDefinition::Type;
sqlast::Select Select(const std::string &table,
                      const std::vector<std::string> &columns) {
  sqlast::Select select(table);
  for (const std::string &col : columns) {
    select.AddColumn(sqlast::Select::ResultColumn(col));
  }
  return select;
}
std::unique_ptr<sqlast::BinaryExpression> Where(
    const std::string &column, const std::vector<sqlast::Value> &values) {
  auto ptr = std::make_unique<sqlast::BinaryExpression>(EType::IN);
  ptr->SetLeft(std::make_unique<sqlast::ColumnExpression>(column));
  ptr->SetRight(std::make_unique<sqlast::LiteralListExpression>(values));
  return ptr;
}
std::unique_ptr<sqlast::BinaryExpression> And(
    std::unique_ptr<sqlast::BinaryExpression> &&e1,
    std::unique_ptr<sqlast::BinaryExpression> &&e2) {
  auto ptr = std::make_unique<sqlast::BinaryExpression>(EType::AND);
  ptr->SetLeft(std::move(e1));
  ptr->SetRight(std::move(e2));
  return ptr;
}

/*
 * Execute SQL statement.
 */
std::unordered_set<sqlast::Value> Execute(const sqlast::Select &stmt,
                                          Connection *connection,
                                          util::SharedLock *lock) {
  SelectContext context(stmt, connection, lock);
  MOVE_OR_PANIC(sql::SqlResult result, context.ExecWithinTransaction());
  std::unordered_set<sqlast::Value> set;
  for (const auto &r : result.ResultSets().at(0).rows()) {
    set.insert(r.GetValue(0));
  }
  return set;
}
dataflow::SchemaRef CreateSchema(const sqlast::Value &col,
                                 const sqlast::Value &user) {
  CType col1 = col.ConvertType();
  CType col2 = user.ConvertType();
  return dataflow::SchemaFactory::Create({"_col", "_shard", "_count"},
                                         {col1, col2, CType::INT}, {0, 1});
}

/*
 * Simulate an index by querying the DB.
 */
std::vector<dataflow::Record> FindUsersInGroup(sqlast::Value &&value,
                                               Connection *connection,
                                               util::SharedLock *lock) {
  sqlast::Select select = Select("oc_group_user", {"uid"});
  select.SetWhereClause(Where("gid", {value}));
  std::unordered_set<sqlast::Value> lookup = Execute(select, connection, lock);
  CHECK_GT(lookup.size(), 0u) << "Fake index found no records";
  // Make records.
  std::vector<dataflow::Record> records;
  dataflow::SchemaRef schema = CreateSchema(value, *lookup.begin());
  for (const sqlast::Value &val : lookup) {
    records.emplace_back(schema, true);
    records.back().SetValue(value, 0);
    records.back().SetValue(val, 1);
    records.back().SetInt(1, 2);
  }
  return records;
}

std::vector<dataflow::Record> FindOwnersOfFile(sqlast::Value &&fileid,
                                               Connection *connection,
                                               util::SharedLock *lock) {
  // Select 1.
  sqlast::Select select1 = Select("oc_share", {"uid_owner"});
  select1.SetWhereClause(Where("item_source", {fileid}));
  std::unordered_set<sqlast::Value> lookup = Execute(select1, connection, lock);
  CHECK_GT(lookup.size(), 0u) << "Fake index found no records";

  size_t size = lookup.size();
  // Other lookups only if we are using owners, not accessors.
  const Table &table = connection->state->SharderState().GetTable("oc_share");
  if (table.owners.size() > 1) {
    // Select 2.
    sqlast::Select select2 = Select("oc_share", {"share_with"});
    select2.SetWhereClause(And(Where("item_source", {fileid}),
                               Where("share_type", {sqlast::Value(0_s)})));
    std::unordered_set<sqlast::Value> set = Execute(select2, connection, lock);
    lookup.insert(set.begin(), set.end());

    // Select 3.
    sqlast::Select select3 = Select("oc_share", {"share_with_group"});
    select3.SetWhereClause(And(Where("item_source", {fileid}),
                               Where("share_type", {sqlast::Value(1_s)})));
    std::unordered_set<sqlast::Value> grps = Execute(select3, connection, lock);
    if (grps.size() > 0) {
      std::vector<sqlast::Value> grps_vec(grps.begin(), grps.end());
      select3 = Select("oc_group_user", {"uid"});
      select3.SetWhereClause(Where("gid", grps_vec));
      set = Execute(select3, connection, lock);
      lookup.insert(set.begin(), set.end());
    }
  }

  // Make records.
  std::vector<dataflow::Record> records;
  dataflow::SchemaRef schema = CreateSchema(fileid, *lookup.begin());
  for (const sqlast::Value &val : lookup) {
    records.emplace_back(schema, true);
    records.back().SetValue(fileid, 0);
    records.back().SetValue(val, 1);
    records.back().SetInt(1, 2);
  }
  return records;
}

}  // namespace

std::vector<dataflow::Record> FakeLookup(const IndexDescriptor &index,
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
  if (table_name == "oc_share") {
    // Find owners of file whose id = value.
    CHECK_EQ(column_name, "item_source");
    return FindOwnersOfFile(std::move(value), connection, lock);
  }
  LOG(FATAL) << "Unrecognized fake index";
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
