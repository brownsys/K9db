// GDPR statements handling (FORGET and GET).
#include "pelton/shards/sqlengine/gdpr.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "glog/logging.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/types.h"
#include "pelton/util/iterator.h"
#include "pelton/util/shard_name.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {

/*
 * Main entry point.
 */

absl::StatusOr<sql::SqlResult> GDPRContext::Exec() {
  if (this->stmt_.operation() == sqlast::GDPRStatement::Operation::FORGET) {
    return this->ExecForget();
  } else {
    return this->ExecGet();
  }
}

/*
 * Forget.
 */

absl::StatusOr<sql::SqlResult> GDPRContext::ExecForget() {
  Shard current_shard = this->sstate_.GetShard(this->shard_kind_);

  int count = 0;
  for (const auto &table_name : current_shard.owned_tables) {
    std::vector<dataflow::Record> dataflow_updates;

    util::ShardName shard(this->shard_kind_, this->user_id_str_);
    sql::SqlResultSet result =
        this->db_->DeleteShard(table_name, std::move(shard));

    // Time to update dataflows.
    count += result.size();
    if (this->dstate_.HasFlowsFor(table_name)) {
      std::vector<dataflow::Record> vec = result.Vec();
      std::vector<sqlast::Value> pks;
      for (const auto &r : vec) {
        size_t pk_col_index = r.schema().keys().at(0);
        pks.push_back(r.GetValue(pk_col_index));
      }

      // Count how many owners each record has.
      std::vector<size_t> counts = this->db_->CountShards(table_name, pks);
      CHECK_EQ(vec.size(), counts.size());
      for (size_t i = 0; i < vec.size(); i++) {
        if (counts.at(i) == 0) {
          dataflow_updates.push_back(std::move(vec.at(i)));
        }
      }

      // Update dataflow with deletions from table.
      this->dstate_.ProcessRecords(table_name, std::move(dataflow_updates));
    }
  }

  return sql::SqlResult(count);
}

/*
 * Get and helpers.
 */
void GDPRContext::AddOrAppendAndAnonOwned(const TableName &tbl, 
                                          sql::SqlResultSet &&set) {
  const std::vector<sqlast::AnonymizationRule> &rules = 
    this->sstate_.GetTable(tbl).create_stmt.GetAnonymizationRules();

  std::vector<dataflow::Record> &&records = set.Vec();

  for (dataflow::Record &record : records) {
    for (const sqlast::AnonymizationRule &rule : rules) {
      if (rule.GetType() == sqlast::AnonymizationOpTypeEnum::GET && 
        this->user_id_ == record.GetValue(record.schema().IndexOf(rule.GetDataSubject()))) {
        for (const std::string &anon_column : rule.GetAnonymizeColumns()) {
          size_t anon_idx = record.schema().IndexOf(anon_column);
          record.SetNull(true, anon_idx);
          std::cout << "ANONYMIZING!" << std::endl;
        }
      }
    }
  }
  std::cout << "DONE ANONYMIZING! FOR " << std::endl;

  auto it = this->result_.find(tbl);
  if (it == this->result_.end()) {
    this->result_.emplace(tbl, sql::SqlResultSet(set.schema(), std::move(records)));
  } else {
    it->second.AppendDeduplicate(sql::SqlResultSet(set.schema(), std::move(records)));
  }
}

void GDPRContext::AddOrAppendAndAnon(const TableName &tbl, 
                                     const std::string &accessed_column,
                                     sql::SqlResultSet &&set) {
  // Find the table with table name `tbl` among all tables; access create_stmt
  // Extract the vector of anonymization rules via GetAnonymizationRules
  // Use GetType, GetDataSubject, GetAnonymizeColumns
  // Match accessed_column with the result of GetDataSubject

  const std::vector<sqlast::AnonymizationRule> &rules = 
    this->sstate_.GetTable(tbl).create_stmt.GetAnonymizationRules();

  std::vector<dataflow::Record> &&records = set.Vec();

  for (dataflow::Record &record : records) {
    for (const sqlast::AnonymizationRule &rule : rules) {
      // Since it's only used in GDPR GET, we are checking for the type
      if (rule.GetType() == sqlast::AnonymizationOpTypeEnum::GET && 
        rule.GetDataSubject() == accessed_column) {
        for (const std::string &anon_column : rule.GetAnonymizeColumns()) {
          size_t anon_idx = record.schema().IndexOf(anon_column);
          record.SetNull(true, anon_idx);
        }
      }
    }
  }

  auto it = this->result_.find(tbl);
  if (it == this->result_.end()) {
    this->result_.emplace(tbl, sql::SqlResultSet(set.schema(), std::move(records)));
  } else {
    it->second.AppendDeduplicate(sql::SqlResultSet(set.schema(), std::move(records)));
  }
}

sqlast::Select GDPRContext::MakeAccessSelect(
    const TableName &tbl, const std::string &column,
    const std::vector<sqlast::Value> &invals) {
  // Build select statement on the form:
  // SELECT * FROM <tbl> WHERE <column> IN (<invals>);
  sqlast::Select select{tbl};
  select.AddColumn(sqlast::Select::ResultColumn("*"));

  std::unique_ptr<sqlast::BinaryExpression> binexpr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);
  binexpr->SetLeft(std::make_unique<sqlast::ColumnExpression>(column));
  binexpr->SetRight(std::make_unique<sqlast::LiteralListExpression>(invals));
  select.SetWhereClause(std::move(binexpr));

  return select;
}

void GDPRContext::FindInDependents(const TableName &table_name,
                                   const std::vector<dataflow::Record> &rows) {
  // Only considers access dependents within the same shard kind.
  const Table &table = this->sstate_.GetTable(table_name);
  for (const auto &[deptbl, depdesc] : table.access_dependents) {
    if (depdesc->shard_kind != this->shard_kind_) {
      continue;
    }

    // Figure out type of sharding.
    size_t colidx;
    switch (depdesc->type) {
      case InfoType::DIRECT:
        colidx = std::get<DirectInfo>(depdesc->info).next_column_index;
        break;
      case InfoType::TRANSITIVE:
        colidx = std::get<TransitiveInfo>(depdesc->info).next_column_index;
        break;
      case InfoType::VARIABLE:
        colidx = std::get<VariableInfo>(depdesc->info).origin_column_index;
        break;
      default:
        LOG(FATAL) << "UNREACHABLE";
    }

    // Extract the values of the fk column from rows and recurse.
    std::vector<sqlast::Value> next_vals;
    for (const dataflow::Record &record : rows) {
      next_vals.push_back(record.GetValue(colidx));
    }

    this->FindData(deptbl, depdesc, next_vals);
  }
}

void GDPRContext::FindData(const TableName &tbl, const ShardDescriptor *desc,
                           const std::vector<sqlast::Value> &fkvals) {
  // Find the accessed data.
  const std::string &column_name = EXTRACT_VARIANT(column, desc->info);
  sqlast::Select select = this->MakeAccessSelect(tbl, column_name, fkvals);
  SelectContext context(select, this->conn_, this->lock_);
  MOVE_OR_PANIC(sql::SqlResult result, context.Exec());

  // Recurse on access dependents.
  sql::SqlResultSet &resultset = result.ResultSets().front();
  if (resultset.size() > 0) {
    this->FindInDependents(tbl, resultset.rows());

    // Add to result set.
    this->AddOrAppendAndAnon(tbl, column_name, std::move(resultset));
  }
}

absl::StatusOr<sql::SqlResult> GDPRContext::ExecGet() {
  Shard current_shard = this->sstate_.GetShard(this->shard_kind_);
  for (const auto &table_name : current_shard.owned_tables) {
    // Get data from tables owned by user.
    util::ShardName shard(this->shard_kind_, this->user_id_str_);
    sql::SqlResultSet resultset =
        this->db_->GetShard(table_name, std::move(shard));

    // Recurse on access dependents.
    if (resultset.size() > 0) {
      this->FindInDependents(table_name, resultset.rows());

      // Add to result set.
      this->AddOrAppendAndAnonOwned(table_name, std::move(resultset));
    }
  }

  // Turn into sql::SqlResult.
  using I = std::unordered_map<TableName, sql::SqlResultSet>::iterator;
  using ref = std::iterator_traits<I>::reference;
  auto &&[it, end] = util::Map(&this->result_,
                               [](ref pair) { return std::move(pair.second); });
  return sql::SqlResult(std::vector<sql::SqlResultSet>(it, end));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
