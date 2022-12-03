// Hardcoded owncloud query.
#include "pelton/shards/sqlengine/view_query.h"

#include <memory>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "glog/logging.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace view {

using CType = sqlast::ColumnDefinition::Type;

absl::StatusOr<sql::SqlResult> PerformViewQuery(const std::string question_mark,
                                                Connection *conn,
                                                util::SharedLock *lock) {
  // ========== SELECT 1 BEGIN ==========
  // SELECT * FROM oc_share
  // WHERE (share_type = 0) AND (share_with = [question_mark])

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
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);

  select_1_left_expr->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("share_type"));
  select_1_left_expr->SetRight(std::make_unique<sqlast::LiteralExpression>(
      sqlast::Value(static_cast<int64_t>(0))));

  select_1_right_expr->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("share_with"));
  select_1_right_expr->SetRight(std::make_unique<sqlast::LiteralExpression>(
      sqlast::Value(question_mark)));

  select_1_where_clause->SetLeft(std::move(select_1_left_expr));
  select_1_where_clause->SetRight(std::move(select_1_right_expr));

  select_1.SetWhereClause(std::move(select_1_where_clause));

  SelectContext select_1_context(select_1, conn, lock);
  MOVE_OR_PANIC(sql::SqlResult select_1_result, select_1_context.Exec());

  // ========== SELECT 1 END ==========

  // ========== SELECT 2 BEGIN ==========
  // SELECT gid FROM oc_group_user
  // WHERE uid = [question_mark]

  sqlast::Select select_2{"oc_group_user"};
  select_2.AddColumn(sqlast::Select::ResultColumn("gid"));

  std::unique_ptr<sqlast::BinaryExpression> select_2_where_clause =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);

  select_2_where_clause->SetLeft(
      std::make_unique<sqlast::ColumnExpression>("uid"));
  select_2_where_clause->SetRight(std::make_unique<sqlast::LiteralExpression>(
      sqlast::Value(question_mark)));

  select_2.SetWhereClause(std::move(select_2_where_clause));

  SelectContext select_2_context(select_2, conn, lock);
  MOVE_OR_PANIC(sql::SqlResult select_2_result, select_2_context.Exec());

  // ========== SELECT 2 END ==========

  // Convert the results of SELECT 2 to vector
  std::vector<sqlast::Value> select_2_result_vec;

  CHECK_EQ(select_2_result.ResultSets().size(), 1);
  size_t index_of_gid =
      select_2_result.ResultSets().at(0).schema().IndexOf("gid");
  for (const auto &rec : select_2_result.ResultSets().at(0).rows()) {
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

  SelectContext select_3_context(select_3, conn, lock);
  MOVE_OR_PANIC(sql::SqlResult select_3_result, select_3_context.Exec());

  // ========== SELECT 3 END ==========

  // Check nullity of the other joins

  // ========== INNER JOIN CHECK STARTS ==========

  std::vector<sqlast::Value> file_source_vec;

  CHECK_EQ(select_1_result.ResultSets().size(), 1);
  size_t index_of_file_source_1 =
      select_1_result.ResultSets().at(0).schema().IndexOf("file_source");

  for (const auto &rec : select_1_result.ResultSets().at(0).rows()) {
    file_source_vec.push_back(rec.GetValue(index_of_file_source_1));
  }

  CHECK_EQ(select_3_result.ResultSets().size(), 1);
  size_t index_of_file_source_2 =
      select_3_result.ResultSets().at(0).schema().IndexOf("file_source");

  for (const auto &rec : select_3_result.ResultSets().at(0).rows()) {
    file_source_vec.push_back(rec.GetValue(index_of_file_source_2));
  }

  // SELECT fileid FROM oc_filecache
  // WHERE file_source IN [file_source_vec]

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

  SelectContext select_join_context(select_join, conn, lock);
  MOVE_OR_PANIC(sql::SqlResult select_join_result, select_join_context.Exec());

  CHECK_EQ(select_join_result.ResultSets().size(), 1);
  CHECK_EQ(select_join_result.ResultSets().at(0).rows().size(), 0);

  // ========== INNER JOIN CHECK ENDS ==========

  // Perform a "union"
  std::vector<dataflow::Record> union_vec;

  std::vector<dataflow::Record> select_1_result_vec =
      select_1_result.ResultSets().at(0).Vec();
  std::vector<dataflow::Record> select_3_result_vec =
      select_3_result.ResultSets().at(0).Vec();

  for (auto &rec : select_1_result_vec) {
    union_vec.push_back(std::move(rec));
  }

  for (auto &rec : select_3_result_vec) {
    union_vec.push_back(std::move(rec));
  }

  // Perform a projection

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
    dataflow::Record projected_rec{schema};

    projected_rec.SetInt(rec.GetInt(rec.schema().IndexOf("id")), 0);
    projected_rec.SetInt(rec.GetInt(rec.schema().IndexOf("item_source")), 1);
    projected_rec.SetInt(rec.GetInt(rec.schema().IndexOf("share_type")), 2);
    projected_rec.SetNull(true, 3);
    projected_rec.SetNull(true, 4);
    projected_rec.SetNull(true, 5);
    projected_rec.SetString(std::make_unique<std::string>(question_mark), 6);

    projection_vec.push_back(std::move(projected_rec));
  }

  sql::SqlResultSet projection_result_set(schema, std::move(projection_vec));

  return sql::SqlResult({std::move(projection_result_set)});
}

}  // namespace view
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
