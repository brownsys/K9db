// EXPLAIN QUERY statements handling.
#include "k9db/shards/sqlengine/explain.h"

#include <algorithm>

#include "k9db/shards/types.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

/*
 * Helpers.
 */

void ExplainContext::RecurseInsert(const std::string &table_name, bool first) {
  const Table &tbl = this->sstate_.GetTable(table_name);
  for (const auto &[dep_tbl, dep_desc] : tbl.dependents) {
    if (dep_desc->type == InfoType::DIRECT) {
      continue;
    }
    if (first && dep_desc->type == InfoType::TRANSITIVE) {
      continue;
    }
    const std::string &shard_kind = dep_desc->shard_kind;
    std::string column = EXTRACT_VARIANT(column, dep_desc->info);
    this->AddExplanation("INSERT [" + shard_kind + "#" + column + "]", dep_tbl);
    // On disk index updates.
    for (const std::string &index : this->db_->GetIndices(dep_tbl)) {
      this->AddExplanation("INDEX UPDATE", dep_tbl + " ON (" + index + ")");
    }
    this->RecurseInsert(dep_tbl, false);
  }
}

void ExplainContext::RecurseDelete(const std::string &shard_kind,
                                   const std::string &table_name, bool first) {
  const Table &tbl = this->sstate_.GetTable(table_name);
  for (const auto &[dep_tbl, dep_desc] : tbl.dependents) {
    if (shard_kind != "*" && dep_desc->shard_kind != shard_kind) {
      continue;
    }
    if (dep_desc->type == InfoType::DIRECT) {
      continue;
    }
    if (first && dep_desc->type == InfoType::TRANSITIVE) {
      continue;
    }
    std::string column = EXTRACT_VARIANT(column, dep_desc->info);
    // Is the delete by an index?
    std::vector<std::string> indices = this->db_->GetIndices(dep_tbl);
    auto it = std::find(indices.begin(), indices.end(), column);
    std::string index = (it == indices.end() ? "SCAN" : column);
    this->AddExplanation("DELETE [" + shard_kind + "#" + column + "]",
                         dep_tbl + " BY " + index);
    // On disk index updates.
    for (const std::string &index : indices) {
      this->AddExplanation("INDEX UPDATE", dep_tbl + " ON (" + index + ")");
    }
    this->RecurseDelete(dep_desc->shard_kind, dep_tbl, false);
  }
}

/*
 * Insert and Delete.
 */
void ExplainContext::Explain(const sqlast::Insert &query) {
  const std::string &table_name = query.table_name();
  const Table &tbl = this->sstate_.GetTable(table_name);
  for (const auto &desc : tbl.owners) {
    const std::string &shard_kind = desc->shard_kind;
    std::string column = EXTRACT_VARIANT(column, desc->info);
    if (desc->type == InfoType::DIRECT) {
      this->AddExplanation("INSERT [" + shard_kind + "#" + column + "]",
                           table_name);
    } else if (desc->type == InfoType::TRANSITIVE) {
      const std::string &index =
          std::get<TransitiveInfo>(desc->info).index->index_name;
      this->AddExplanation("INSERT [" + shard_kind + "#" + column + "]",
                           table_name + " USING " + index);
    } else if (desc->type == InfoType::VARIABLE) {
      std::string fk = desc->next_table() + "." + desc->upcolumn();
      this->AddExplanation("INSERT [" + shard_kind + "#" + column + "]",
                           table_name + " USING WHERE ON" + fk);
    }
    // On disk index updates.
    for (const std::string &index : this->db_->GetIndices(table_name)) {
      this->AddExplanation("INDEX UPDATE", table_name + " ON (" + index + ")");
    }
  }
  this->RecurseInsert(table_name, true);
  for (const std::string &view : this->dstate_.GetFlowsAffectBy(table_name)) {
    this->AddExplanation("VIEW UPDATE", view);
  }
}

void ExplainContext::Explain(const sqlast::Delete &query) {
  const std::string &table_name = query.table_name();
  const Table &tbl = this->sstate_.GetTable(table_name);
  for (const auto &desc : tbl.owners) {
    const std::string &shard_kind = desc->shard_kind;
    std::string column = EXTRACT_VARIANT(column, desc->info);
    std::string index = this->db_->GetIndex(table_name, query.GetWhereClause());
    this->AddExplanation("DELETE [" + shard_kind + "#" + column + "]",
                         table_name + " USING " + index);
    // On disk index updates.
    for (const std::string &index : this->db_->GetIndices(table_name)) {
      this->AddExplanation("INDEX UPDATE", table_name + " ON (" + index + ")");
    }
  }
  this->RecurseDelete("*", table_name, true);
  for (const std::string &view : this->dstate_.GetFlowsAffectBy(table_name)) {
    this->AddExplanation("VIEW UPDATE", view);
  }
}

void ExplainContext::Explain(const sqlast::Replace &query) {
  sqlast::Update update(query.table_name());
  for (size_t i = 0; i < this->schema_.size(); i++) {
    const sqlast::Value &v = query.GetValue(this->schema_.NameOf(i), i);
    update.AddColumnValue(this->schema_.NameOf(i),
                          std::make_unique<sqlast::LiteralExpression>(v));
  }

  sqlast::Expression::Type eq = sqlast::Expression::Type::EQ;
  std::unique_ptr<sqlast::BinaryExpression> where =
      std::make_unique<sqlast::BinaryExpression>(eq);
  size_t pkcol = this->schema_.keys().at(0);
  const std::string &pkcolname = this->schema_.NameOf(pkcol);
  const sqlast::Value &pkval = query.GetValue(pkcolname, pkcol);
  where->SetLeft(std::make_unique<sqlast::ColumnExpression>(pkcolname));
  where->SetRight(std::make_unique<sqlast::LiteralExpression>(pkval));
  update.SetWhereClause(std::move(where));
  this->Explain(update);
}

void ExplainContext::Explain(const sqlast::Update &query) {
  // Delete.
  this->Explain(query.DeleteDomain());
  // Followed by INSERT.
  sqlast::Insert insert(query.table_name());
  this->Explain(insert);
}

void ExplainContext::Explain(const sqlast::Select &query) {
  const std::string &table_name = query.table_name();
  if (this->dstate_.HasFlow(table_name)) {
    this->AddExplanation("VIEW LOOKUP", table_name);
  } else {
    std::string index = this->db_->GetIndex(table_name, query.GetWhereClause());
    this->AddExplanation("SELECT", table_name + " USING " + index);
  }
}

/*
 * Main entry point.
 */
#define EXPLAIN_CAST(type) *static_cast<const type *>(query)
absl::StatusOr<sql::SqlResult> ExplainContext::Exec() {
  const sqlast::AbstractStatement *query = this->stmt_.GetQuery();
  switch (query->type()) {
    case sqlast::AbstractStatement::Type::INSERT: {
      this->Explain(EXPLAIN_CAST(sqlast::Insert));
      break;
    }
    case sqlast::AbstractStatement::Type::REPLACE: {
      this->Explain(EXPLAIN_CAST(sqlast::Replace));
      break;
    }
    case sqlast::AbstractStatement::Type::UPDATE: {
      this->Explain(EXPLAIN_CAST(sqlast::Update));
      break;
    }
    case sqlast::AbstractStatement::Type::DELETE: {
      this->Explain(EXPLAIN_CAST(sqlast::Delete));
      break;
    }
    case sqlast::AbstractStatement::Type::SELECT: {
      this->Explain(EXPLAIN_CAST(sqlast::Select));
      break;
    }
    default:
      return absl::InvalidArgumentError("Invalid EXPLAIN query type");
  }

  return sql::SqlResult(
      sql::SqlResultSet(this->schema_, std::move(this->explanation_)));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
