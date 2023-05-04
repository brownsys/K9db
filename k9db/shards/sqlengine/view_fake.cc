// Implements an identical API to view.cc that simulate the known ownCloud
// experiment views using database queries without creating any actual
// views.
// We use this when views are turned off to run the drill-down experiment.

// clang-format off
// NOLINTNEXTLINE
#include "k9db/shards/sqlengine/view.h"
// clang-format on

#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "k9db/dataflow/schema.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "glog/logging.h"

namespace k9db {
namespace shards {
namespace sqlengine {
namespace view {

using EType = sqlast::Expression::Type;
using CType = sqlast::ColumnDefinition::Type;

// View creation is entirely fake.
absl::StatusOr<sql::SqlResult> CreateView(const sqlast::CreateView &stmt,
                                          Connection *connection,
                                          util::UniqueLock *lock) {
  // View creation is entirely fake.
  dataflow::DataFlowState &dstate = connection->state->DataflowState();
  dstate.AddFakeFlow(stmt.view_name());
  return sql::SqlResult(true);
}

// Querying the file_view results in running the equivalent query against
// the database.
absl::StatusOr<sql::SqlResult> SelectView(const sqlast::Select &stmt,
                                          Connection *connection,
                                          util::SharedLock *lock) {
  // Ensure we are ready from ownCloud's file_view.
  const std::string &view_name = stmt.table_name();
  if (view_name != "file_view") {
    LOG(FATAL) << "Views disabled; queried unknown view: " << view_name;
  }

  // Extract query arguments.
  std::vector<sqlast::Value> params;
  const sqlast::Expression *const expr = stmt.GetWhereClause()->GetRight();
  if (stmt.GetWhereClause()->type() == EType::IN) {
    const sqlast::LiteralListExpression *const list =
        static_cast<const sqlast::LiteralListExpression *const>(expr);
    params = list->values();
  } else if (stmt.GetWhereClause()->type() == EType::EQ) {
    const sqlast::LiteralExpression *const literal =
        static_cast<const sqlast::LiteralExpression *const>(expr);
    params.push_back(literal->value());
  } else {
    sqlast::Stringifier stringifer;
    LOG(FATAL) << "Unrecognized where clause in query "
               << stringifer.VisitSelect(stmt);
  }

  // Begin a read transaction to read query results.
  sql::Session *db = connection->session.get();
  db->BeginTransaction(false);

  // ========== SELECT 1 BEGIN ==========
  // SELECT * FROM oc_share
  // WHERE (share_type = 0) AND (share_with IN [params])

  sqlast::Select select_1{"oc_share"};
  select_1.AddColumn(sqlast::Select::ResultColumn("id"));
  select_1.AddColumn(sqlast::Select::ResultColumn("item_source"));
  select_1.AddColumn(sqlast::Select::ResultColumn("share_type"));
  select_1.AddColumn(sqlast::Select::ResultColumn("file_source"));
  select_1.AddColumn(sqlast::Select::ResultColumn("share_with"));

  std::unique_ptr<sqlast::BinaryExpression> select_1_where_clause =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::AND);

  std::unique_ptr<sqlast::BinaryExpression> select_1_left_expr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);

  std::unique_ptr<sqlast::BinaryExpression> select_1_right_expr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);

  select_1_left_expr->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("share_type"));
  select_1_left_expr->SetRight(std::make_unique<sqlast::LiteralExpression>(
      sqlast::Value(static_cast<int64_t>(0))));

  select_1_right_expr->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("share_with"));
  select_1_right_expr->SetRight(
      std::make_unique<sqlast::LiteralListExpression>(params));

  select_1_where_clause->SetLeft(std::move(select_1_left_expr));
  select_1_where_clause->SetRight(std::move(select_1_right_expr));

  select_1.SetWhereClause(std::move(select_1_where_clause));
  sql::SqlResultSet select_1_result = db->ExecuteSelect(select_1);

  // ========== SELECT 1 END ==========

  // ========== SELECT 2 BEGIN ==========
  // SELECT uid, gid FROM oc_group_user
  // WHERE uid IN [params]

  sqlast::Select select_2{"oc_group_user"};
  select_2.AddColumn(sqlast::Select::ResultColumn("gid"));
  select_2.AddColumn(sqlast::Select::ResultColumn("uid"));

  std::unique_ptr<sqlast::BinaryExpression> select_2_where_clause =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);

  select_2_where_clause->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("uid"));
  select_2_where_clause->SetRight(
      std::make_unique<sqlast::LiteralListExpression>(params));

  select_2.SetWhereClause(std::move(select_2_where_clause));
  sql::SqlResultSet select_2_result = db->ExecuteSelect(select_2);

  // ========== SELECT 2 END ==========

  // Convert the results of SELECT 2 to vector
  std::vector<sqlast::Value> select_2_result_vec;

  // Convert the results of SELECT 2 to a map from [gid] to [uids]
  size_t index_of_gid = select_2_result.schema().IndexOf("gid");
  size_t index_of_uid = select_2_result.schema().IndexOf("uid");
  std::unordered_map<std::string, std::unordered_set<std::string>>
      select_2_result_map;
  for (const auto &rec : select_2_result.rows()) {
    std::string gid_str = rec.GetValue(index_of_gid).GetString();
    std::string uid_str = rec.GetValue(index_of_uid).GetString();
    select_2_result_map[gid_str].insert(uid_str);
    select_2_result_vec.push_back(rec.GetValue(index_of_gid));
  }

  // ========== SELECT 3 BEGIN ==========
  // SELECT * FROM oc_share
  // WHERE (share_type = 1) AND (share_with_group IN [select_2_result_vec])

  sqlast::Select select_3{"oc_share"};
  select_3.AddColumn(sqlast::Select::ResultColumn("id"));
  select_3.AddColumn(sqlast::Select::ResultColumn("item_source"));
  select_3.AddColumn(sqlast::Select::ResultColumn("share_type"));
  select_3.AddColumn(sqlast::Select::ResultColumn("file_source"));
  select_3.AddColumn(sqlast::Select::ResultColumn("share_with_group"));

  std::unique_ptr<sqlast::BinaryExpression> select_3_where_clause =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::AND);

  std::unique_ptr<sqlast::BinaryExpression> select_3_left_expr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);

  std::unique_ptr<sqlast::BinaryExpression> select_3_right_expr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);

  select_3_left_expr->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("share_type"));
  select_3_left_expr->SetRight(std::make_unique<sqlast::LiteralExpression>(
      sqlast::Value(static_cast<int64_t>(1))));

  select_3_right_expr->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("share_with_group"));
  select_3_right_expr->SetRight(
      std::make_unique<sqlast::LiteralListExpression>(select_2_result_vec));

  select_3_where_clause->SetLeft(std::move(select_3_left_expr));
  select_3_where_clause->SetRight(std::move(select_3_right_expr));

  select_3.SetWhereClause(std::move(select_3_where_clause));
  sql::SqlResultSet select_3_result = db->ExecuteSelect(select_3);

  // ========== SELECT 3 END ==========

  // Check nullity of the other joins

  // ========== INNER JOIN CHECK STARTS ==========

  std::vector<sqlast::Value> file_source_vec;
  size_t index_of_file_source_1 =
      select_1_result.schema().IndexOf("file_source");
  for (const auto &rec : select_1_result.rows()) {
    file_source_vec.push_back(rec.GetValue(index_of_file_source_1));
  }

  size_t index_of_file_source_2 =
      select_3_result.schema().IndexOf("file_source");
  for (const auto &rec : select_3_result.rows()) {
    file_source_vec.push_back(rec.GetValue(index_of_file_source_2));
  }

  // SELECT fileid, path FROM oc_filecache
  // WHERE fileid IN [file_source_vec]
  sqlast::Select select_join{"oc_filecache"};
  select_join.AddColumn(sqlast::Select::ResultColumn("fileid"));
  select_join.AddColumn(sqlast::Select::ResultColumn("path"));

  std::unique_ptr<sqlast::BinaryExpression> select_join_where_clause =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);

  select_join_where_clause->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("fileid"));
  select_join_where_clause->SetRight(
      std::make_unique<sqlast::LiteralListExpression>(file_source_vec));

  select_join.SetWhereClause(std::move(select_join_where_clause));
  sql::SqlResultSet select_join_result = db->ExecuteSelect(select_join);
  CHECK_EQ(select_join_result.rows().size(), 0u);

  // ========== INNER JOIN CHECK ENDS ==========

  // ========== Perform a "union" ==========
  std::vector<dataflow::Record> union_vec = select_1_result.Vec();
  std::vector<dataflow::Record> select_3_result_vec = select_3_result.Vec();

  size_t select_1_size = union_vec.size();
  for (auto &rec : select_3_result_vec) {
    union_vec.push_back(std::move(rec));
  }

  // ========== Perform a projection ==========
  std::vector<std::string> names = {
      "sid",  "item_source",       "share_type",  "fileid",
      "path", "storage_string_id", "share_target"};
  std::vector<CType> types = {CType::INT,  CType::INT,  CType::INT, CType::INT,
                              CType::TEXT, CType::TEXT, CType::TEXT};
  std::vector<dataflow::ColumnID> keys = {0};
  dataflow::SchemaRef schema =
      dataflow::SchemaFactory::Create(names, types, keys);

  std::vector<dataflow::Record> projection_vec;
  for (const auto &rec : union_vec) {
    dataflow::Record projected{schema};
    projected.SetInt(rec.GetInt(0), 0);
    projected.SetInt(rec.GetInt(1), 1);
    projected.SetInt(rec.GetInt(2), 2);
    projected.SetInt(rec.GetInt(3), 3);
    projected.SetNull(true, 4);
    projected.SetNull(true, 5);
    // Last column is the user_id, either from the direct share_with or via
    // the group.
    if (projection_vec.size() < select_1_size) {
      projected.SetString(std::make_unique<std::string>(rec.GetString(4)), 6);
      projection_vec.push_back(std::move(projected));
    } else {
      for (const std::string &uid : select_2_result_map.at(rec.GetString(4))) {
        projected.SetString(std::make_unique<std::string>(uid), 6);
        projection_vec.push_back(projected.Copy());
      }
    }
  }

  // Nothing to commit.
  db->RollbackTransaction();

  // Return projection as an SqlResultSet.
  return sql::SqlResult(sql::SqlResultSet(schema, std::move(projection_vec)));
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
