// EXPLAIN QUERY statements handling.
#include "pelton/shards/sqlengine/explain.h"

#include <algorithm>

#include "pelton/shards/sqlengine/replace.h"
#include "pelton/shards/sqlengine/update.h"
#include "pelton/shards/types.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * Helpers.
 */

void ExplainContext::RecurseInsert(const std::string &table_name) {
  const Table &tbl = this->sstate_.GetTable(table_name);
  for (const auto &[dep_tbl, dep_desc] : tbl.dependents) {
    if (dep_desc->type == InfoType::DIRECT) {
      continue;
    }
    const std::string &shard_kind = dep_desc->shard_kind;
    std::string column = EXTRACT_VARIANT(column, dep_desc->info);
    this->AddExplanation("INSERT [" + shard_kind + "#" + column + "]", dep_tbl);
    // On disk index updates.
    for (const std::string &index : this->db_->GetIndices(dep_tbl)) {
      this->AddExplanation("INDEX UPDATE", dep_tbl + " ON (" + index + ")");
    }
    this->RecurseInsert(dep_tbl);
  }
}

void ExplainContext::RecurseDelete(const std::string &shard_kind,
                                   const std::string &table_name) {
  const Table &tbl = this->sstate_.GetTable(table_name);
  for (const auto &[dep_tbl, dep_desc] : tbl.dependents) {
    if (shard_kind != "*" && dep_desc->shard_kind != shard_kind) {
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
    this->RecurseDelete(dep_desc->shard_kind, dep_tbl);
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
      const std::string &index =
          std::get<VariableInfo>(desc->info).index->index_name;
      this->AddExplanation("INSERT [" + shard_kind + "#" + column + "]",
                           table_name + " USING " + index);
    }
    // On disk index updates.
    for (const std::string &index : this->db_->GetIndices(table_name)) {
      this->AddExplanation("INDEX UPDATE", table_name + " ON (" + index + ")");
    }
  }
  this->RecurseInsert(table_name);
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
  this->RecurseDelete("*", table_name);
  for (const std::string &view : this->dstate_.GetFlowsAffectBy(table_name)) {
    this->AddExplanation("VIEW UPDATE", view);
  }
}

void ExplainContext::Explain(const sqlast::Replace &query) {
  const std::string &table_name = query.table_name();
  const Table &tbl = this->sstate_.GetTable(table_name);
  ReplaceContext context(query, this->conn_, this->lock_);
  if (context.CanFastReplace()) {
    this->AddExplanation("FAST REPLACE", table_name);
    for (const std::string &index : this->db_->GetIndices(table_name)) {
      this->AddExplanation("INDEX UPDATE", table_name + " ON (" + index + ")");
    }
  } else {
    // A DELETE.
    const std::string &pk = tbl.schema.NameOf(tbl.schema.keys().front());
    using EType = sqlast::Expression::Type;
    auto bx = std::make_unique<sqlast::BinaryExpression>(EType::EQ);
    bx->SetLeft(std::make_unique<sqlast::ColumnExpression>(pk));
    bx->SetRight(std::make_unique<sqlast::LiteralExpression>(sqlast::Value()));
    sqlast::Delete del(query.table_name());
    del.SetWhereClause(std::move(bx));
    this->Explain(del);
    // Followed by INSERT.
    sqlast::Insert insert(query.table_name());
    this->Explain(insert);
  }
}

void ExplainContext::Explain(const sqlast::Update &query) {
  UpdateContext context(query, this->conn_, this->lock_);
  if (context.ModifiesSharding()) {
    this->AddExplanation("FAST UPDATE", query.table_name());
  } else {
    // A DELETE.
    this->Explain(query.DeleteDomain());
    // Followed by INSERT.
    sqlast::Insert insert(query.table_name());
    this->Explain(insert);
  }
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
}  // namespace pelton
