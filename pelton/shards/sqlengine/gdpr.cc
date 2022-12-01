// GDPR statements handling (FORGET and GET).
#include "pelton/shards/sqlengine/gdpr.h"

#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "glog/logging.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/update.h"
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

sqlast::Update GDPRContext::MakeAccessAnonUpdate(
    const TableName &tbl, const std::unordered_set<std::string> &anon_cols,
    const std::string &pk_col, sqlast::Value pk_val) {
  // Build select statement on the form:
  // UPDATE <tbl> SET <anon_cols = NULL> WHERE <pk_col> = <pk_val>;
  sqlast::Update update{tbl};
  for (const auto &anon_col : anon_cols) {
    // FIX: change 600 to NULL
    update.AddColumnValue(anon_col, std::make_unique<sqlast::LiteralExpression>(
                                        sqlast::Value::FromSQLString("600")));
  }

  std::unique_ptr<sqlast::BinaryExpression> binexpr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::EQ);
  binexpr->SetLeft(std::make_unique<sqlast::ColumnExpression>(pk_col));
  binexpr->SetRight(std::make_unique<sqlast::LiteralExpression>(pk_val));
  update.SetWhereClause(std::move(binexpr));

  std::cout << update << std::endl;

  return update;
}

absl::StatusOr<sql::SqlResult> GDPRContext::ExecForget() {
  Shard current_shard = this->sstate_.GetShard(this->shard_kind_);

  int count = 0;
  for (const auto &table_name : current_shard.owned_tables) {
    std::vector<dataflow::Record> dataflow_updates;
    std::vector<dataflow::Record> anonymize_updates;

    util::ShardName shard(this->shard_kind_, this->user_id_str_);
    sql::SqlResultSet result =
        this->db_->DeleteShard(table_name, std::move(shard));
    count += result.size();

    const std::vector<sqlast::AnonymizationRule> &rules =
        this->sstate_.GetTable(table_name).create_stmt.GetAnonymizationRules();
    const dataflow::SchemaRef &current_schema =
        this->sstate_.GetTable(table_name).schema;

    std::vector<dataflow::Record> vec = result.Vec();

    std::cout << "DELETED VALUES FOR TABLE " << table_name << ": " << std::endl;
    for (const auto &rec : vec) {
      std::cout << rec << std::endl;
    }

    // Recurse on access dependents before the dataflow updates get moved.
    if (vec.size() > 0) {
      this->FindInDependents(table_name, vec, true);
    }

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
        if (this->dstate_.HasFlowsFor(table_name)) {
          dataflow_updates.push_back(std::move(vec.at(i)));
        }
      } else {
        anonymize_updates.push_back(std::move(vec.at(i)));
      }
    }

    // Update dataflow with deletions from table.
    this->dstate_.ProcessRecords(table_name, std::move(dataflow_updates));

    for (const auto &anonymized_record : anonymize_updates) {
      // Apply anonymization rules in copies of data.
      // All columns that need to be anonymized for the record
      std::unordered_set<std::string> anon_column_set = {};
      const auto &owners = this->sstate_.GetTable(table_name).owners;

      for (const auto &desc : owners) {
        // the next condition checks whether user owns record through desc
        if (desc->shard_kind == this->shard_kind_ &&
            this->OwnsRecordThroughDesc(desc, anonymized_record)) {
          for (const sqlast::AnonymizationRule &rule : rules) {
            if (rule.GetType() == sqlast::AnonymizationOpTypeEnum::DEL) {
              if (EXTRACT_VARIANT(column, desc->info) ==
                  rule.GetDataSubject()) {
                for (const auto &anon_col : rule.GetAnonymizeColumns()) {
                  anon_column_set.insert(anon_col);
                }
              }
            }
          }
        }
      }

      if (anon_column_set.size() > 0) {
        // Issue update to anonymize copies
        sqlast::Update update = this->MakeAccessAnonUpdate(
            table_name, anon_column_set,
            current_schema.NameOf(current_schema.keys().at(0)),
            anonymized_record.GetValue(current_schema.keys().at(0)));

        UpdateContext context(update, this->conn_, this->lock_);
        MOVE_OR_PANIC(sql::SqlResult result, context.Exec());
        count += result.UpdateCount();
      }
    }
  }

  for (const auto &table_name : current_shard.owned_tables) {
    // Delete any tables in shard added from anonymization updates.
    util::ShardName shard(this->shard_kind_, this->user_id_str_);
    sql::SqlResultSet result =
      this->db_->DeleteShard(table_name, std::move(shard));
  }

  return sql::SqlResult(count);
}

void GDPRContext::AnonAccessors(const TableName &tbl, sql::SqlResultSet &&set,
                                const std::string &accessed_column) {
  const std::vector<sqlast::AnonymizationRule> &rules =
      this->sstate_.GetTable(tbl).create_stmt.GetAnonymizationRules();
  const dataflow::SchemaRef &current_schema =
      this->sstate_.GetTable(tbl).schema;

  std::vector<dataflow::Record> &&records = set.Vec();

  for (dataflow::Record &record : records) {
    // All columns that need to be anonymized for the record
    std::unordered_set<std::string> anon_column_set = {};

    // accessorship case
    for (const sqlast::AnonymizationRule &rule : rules) {
      if (rule.GetDataSubject() == accessed_column) {
        for (const auto &anon_col : rule.GetAnonymizeColumns()) {
          anon_column_set.insert(anon_col);
        }
      }
    }

    // Issue update to anonymize copies
    sqlast::Update update = this->MakeAccessAnonUpdate(
        tbl, anon_column_set,
        current_schema.NameOf(current_schema.keys().at(0)),
        record.GetValue(current_schema.keys().at(0)));

    UpdateContext context(update, this->conn_, this->lock_);
    MOVE_OR_PANIC(sql::SqlResult result, context.Exec());
  }
}

/*
 * Get and helpers.
 */

std::vector<sqlast::Value> GDPRContext::ExtractUserIDs(
    const IndexDescriptor &index, sqlast::Value &&data_subject) {
  std::vector<sqlast::Value> user_ids = {};

  std::vector<dataflow::Record> indexed = index::LookupIndex(
      index, std::move(data_subject), this->conn_, this->lock_);

  // Collect user_ids in the vector above
  for (const dataflow::Record &rec : indexed) {
    user_ids.push_back(rec.GetValue(1));
  }
  return user_ids;
}

void GDPRContext::AddIntersect(std::vector<std::string> &into,
                               const std::vector<std::string> &from) {
  std::vector<std::string> intersection;
  std::set_intersection(into.begin(), into.end(), from.begin(), from.end(),
                        intersection.begin());
  into = intersection;
}

bool GDPRContext::OwnsRecordThroughDesc(
    const std::unique_ptr<ShardDescriptor> &owner_desc,
    const dataflow::Record &record) {
  // Find the owners, owning relation of which is originating from the
  // data subject column, and the rules attributed to them
  size_t data_subject_idx =
      record.schema().IndexOf(EXTRACT_VARIANT(column, owner_desc->info));
  sqlast::Value data_subject = record.GetValue(data_subject_idx);
  std::vector<sqlast::Value> user_ids;

  if (owner_desc->type == InfoType::DIRECT) {
    // If the type of sharding is direct, we know for sure that
    // data_subject column of the anonymized table would contain
    // user_id
    user_ids = {data_subject};
  } else if (owner_desc->type == InfoType::TRANSITIVE) {
    // If the type of sharding is transitive, we need to get the
    // user_id via an index lookup
    const IndexDescriptor &index =
        *(std::get<TransitiveInfo>(owner_desc->info).index);
    user_ids = this->ExtractUserIDs(index, std::move(data_subject));
  } else if (owner_desc->type == InfoType::VARIABLE) {
    // Same thing as for transitive type of sharding
    const IndexDescriptor &index =
        *(std::get<VariableInfo>(owner_desc->info).index);
    user_ids = this->ExtractUserIDs(index, std::move(data_subject));
  }
  return std::find(user_ids.begin(), user_ids.end(), this->user_id_) !=
         user_ids.end();
}

void GDPRContext::AddOrAppendAndAnon(const TableName &tbl,
                                     sql::SqlResultSet &&set,
                                     const std::string &accessed_column) {
  const std::vector<sqlast::AnonymizationRule> &rules =
      this->sstate_.GetTable(tbl).create_stmt.GetAnonymizationRules();

  std::vector<dataflow::Record> &&records = set.Vec();

  for (dataflow::Record &record : records) {
    // All columns that need to be anonymized for the record
    std::vector<std::vector<std::string>> anon_column_sets = {};
    const auto &owners = this->sstate_.GetTable(tbl).owners;

    for (const auto &desc : owners) {
      if (accessed_column == "") {
        // the next condition checks whether user owns record through desc
        if (desc->shard_kind == this->shard_kind_ &&
            this->OwnsRecordThroughDesc(desc, record)) {
          bool some_rule_applies = false;
          for (const sqlast::AnonymizationRule &rule : rules) {
            // Since it's only used in GDPR GET, we are checking for the type
            if (rule.GetType() == sqlast::AnonymizationOpTypeEnum::GET) {
              if (EXTRACT_VARIANT(column, desc->info) ==
                  rule.GetDataSubject()) {
                anon_column_sets.push_back(rule.GetAnonymizeColumns());
                some_rule_applies = true;
              }
            }
          }
          // if users owns record through desc but no rules apply, ensure that
          // the record does not get anonymized
          if (!some_rule_applies && accessed_column == "") {
            anon_column_sets.push_back({});
          }
        }
      } else {
        // accessorship case
        for (const sqlast::AnonymizationRule &rule : rules) {
          if (rule.GetDataSubject() == accessed_column) {
            anon_column_sets.push_back(rule.GetAnonymizeColumns());
          }
        }
      }
    }

    std::vector<std::string> anon_columns = {};
    for (size_t i = 0; i < anon_column_sets.size(); i++) {
      if (i == 0) {
        anon_columns = anon_column_sets.at(i);
      } else {
        this->AddIntersect(anon_columns, anon_column_sets.at(i));
      }
    }

    for (const std::string &anon_column : anon_columns) {
      size_t anon_idx = record.schema().IndexOf(anon_column);
      record.SetNull(true, anon_idx);
    }
  }

  auto it = this->result_.find(tbl);
  if (it == this->result_.end()) {
    this->result_.emplace(tbl,
                          sql::SqlResultSet(set.schema(), std::move(records)));
  } else {
    it->second.AppendMerge(sql::SqlResultSet(set.schema(), std::move(records)));
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
                                   const std::vector<dataflow::Record> &rows,
                                   bool forget) {
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

    this->FindData(deptbl, depdesc, next_vals, forget);
  }
}

void GDPRContext::FindData(const TableName &tbl, const ShardDescriptor *desc,
                           const std::vector<sqlast::Value> &fkvals,
                           bool forget) {
  // Find the accessed data.
  const std::string &column_name = EXTRACT_VARIANT(column, desc->info);
  sqlast::Select select = this->MakeAccessSelect(tbl, column_name, fkvals);
  SelectContext context(select, this->conn_, this->lock_);
  MOVE_OR_PANIC(sql::SqlResult result, context.Exec());

  // Recurse on access dependents.
  sql::SqlResultSet &resultset = result.ResultSets().front();
  if (resultset.size() > 0) {
    this->FindInDependents(tbl, resultset.rows(), forget);

    // Add to result set or anonymize.
    if (!forget) {
      this->AddOrAppendAndAnon(tbl, std::move(resultset), column_name);
    } else {
      this->AnonAccessors(tbl, std::move(resultset), column_name);
    }
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
      this->FindInDependents(table_name, resultset.rows(), false);

      // Add to result set.
      this->AddOrAppendAndAnon(table_name, std::move(resultset));
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
