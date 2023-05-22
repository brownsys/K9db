// GDPR FORGET statements handling.
// clang-format off
// NOLINTNEXTLINE
#include "k9db/shards/sqlengine/gdpr.h"
// clang-format on

#include <memory>
#include <utility>

#include "glog/logging.h"
#include "k9db/shards/sqlengine/update.h"
#include "k9db/shards/sqlengine/delete.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

namespace {

sqlast::Delete MakeDelete(const TableName &table, const std::string &column,
                          std::vector<sqlast::Value> &&invals) {
  // DELETE FROM <tbl> WHERE <column> IN (<invals>);
  sqlast::Delete del{table};
  std::unique_ptr<sqlast::BinaryExpression> binexpr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);
  binexpr->SetLeft(std::make_unique<sqlast::ColumnExpression>(column));
  binexpr->SetRight(
      std::make_unique<sqlast::LiteralListExpression>(std::move(invals)));
  del.SetWhereClause(std::move(binexpr));
  return del;
}

sqlast::Update MakeUpdate(const TableName &table, const std::string &column,
                          std::vector<sqlast::Value> &&invals,
                          const std::unordered_set<std::string> &anon) {
  sqlast::Value null;
  // UPDATE <tbl> SET <anon> = NULL WHERE <column> IN (<invals>);
  sqlast::Update update{table};
  for (const std::string &c : anon) {
    update.AddColumnValue(c, std::make_unique<sqlast::LiteralExpression>(null));
  }
  std::unique_ptr<sqlast::BinaryExpression> binexpr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);
  binexpr->SetLeft(std::make_unique<sqlast::ColumnExpression>(column));
  binexpr->SetRight(
      std::make_unique<sqlast::LiteralListExpression>(std::move(invals)));
  update.SetWhereClause(std::move(binexpr));
  return update;
}

}  // namespace

/*
 * Delete all records in the this user shard.
 */
absl::Status GDPRForgetContext::DeleteOwnedRecords() {
  const Shard &current_shard = this->sstate_.GetShard(this->shard_kind_);
  for (const auto &table_name : current_shard.owned_tables) {
    // Delete records.
    sql::SqlResultSet result =
        this->db_->DeleteShard(table_name, this->shard_.Copy());
    this->status_ += result.size();
    std::vector<dataflow::Record> records = result.Vec();

    // Find out whether the deleted records have any other copies left.
    std::vector<sqlast::Value> pks;
    for (const dataflow::Record &record : records) {
      size_t pkcol = record.schema().keys().at(0);
      pks.push_back(record.GetValue(pkcol));
    }

    // Count how many copies each record has left.
    std::vector<size_t> counts = this->db_->CountShards(table_name, pks);
    CHECK_EQ(records.size(), counts.size());

    // Those without copies need to be removed from dataflow.
    for (size_t i = 0; i < records.size(); i++) {
      if (counts.at(i) == 0) {
        this->records_[table_name].push_back(std::move(records.at(i)));
        this->deleted_[table_name].insert(pks.at(i).AsUnquotedString());
      }
    }
  }
  return absl::OkStatus();
}

/*
 * Anonymize records and helpers.
 */
absl::Status GDPRForgetContext::AnonymizeRecords() {
  // Get working set organized by anonimization rules.
  GroupedRecords working_set = this->GroupRecordsByAnonimzeColumns();
  for (auto &[table_name, vec] : working_set) {
    // Look at records one table at a time.
    const Table &table = this->sstate_.GetTable(table_name);
    const std::vector<sqlast::AnonymizationRule> &rules =
        table.create_stmt.GetAnonymizationRules();
    for (auto &pair : vec) {
      // Records are grouped by columns along with which they are owned or
      // accessed.
      auto &[columns, records] = pair;
      if (records.size() == 0 || columns.size() == 0) {
        continue;
      }

      // Determine which rules apply.
      bool delete_row = false;
      std::unordered_set<std::string> anonymize;
      for (const sqlast::AnonymizationRule &rule : rules) {
        if (rule.GetType() != sqlast::AnonymizationOpTypeEnum::DEL) {
          continue;
        }
        if (columns.count(rule.GetDataSubject()) == 0) {
          continue;
        }
        for (const std::string &column : rule.GetAnonymizeColumns()) {
          anonymize.insert(column);
        }
        if (rule.GetAnonymizeColumns().size() == 0) {
          delete_row = true;
          break;
        }
      }
      if (delete_row) {
        CHECK_STATUS(this->DeleteRecords(table_name, records));
      } else if (anonymize.size() > 0) {
        CHECK_STATUS(this->AnonymizeRecords(table_name, anonymize, records));
      }
    }
  }
  return absl::OkStatus();
}

absl::Status GDPRForgetContext::DeleteRecords(
    const std::string &table_name,
    const std::vector<dataflow::Record> &records) {
  size_t pkcol = records.front().schema().keys().at(0);
  const std::string &pkname = records.front().schema().NameOf(pkcol);

  // Extract PKs from records.
  std::vector<sqlast::Value> pks;
  for (const dataflow::Record &record : records) {
    sqlast::Value pkval = record.GetValue(pkcol);
    if (this->deleted_[table_name].count(pkval.AsUnquotedString()) == 0) {
      pks.push_back(std::move(pkval));
    }
  }
  if (pks.size() == 0) {
    return absl::OkStatus();
  }

  // Delete records.
  sqlast::Delete del = MakeDelete(table_name, pkname, std::move(pks));
  DeleteContext context(del, this->conn_, this->lock_);
  MOVE_OR_RETURN(DeleteContext::Result result, context.DeleteAnonymize());
  this->status_ += result.first;

  // Mark dataflow updates.
  std::vector<dataflow::Record> &vec = this->records_[table_name];
  for (dataflow::Record &record : result.second) {
    vec.push_back(std::move(record));
  }

  return absl::OkStatus();
}

absl::Status GDPRForgetContext::AnonymizeRecords(
    const std::string &table_name,
    const std::unordered_set<std::string> &columns,
    const std::vector<dataflow::Record> &records) {
  size_t pkcol = records.front().schema().keys().at(0);
  const std::string &pkname = records.front().schema().NameOf(pkcol);

  // Extract PKs from records.
  std::vector<sqlast::Value> pks;
  for (const dataflow::Record &record : records) {
    sqlast::Value pkval = record.GetValue(pkcol);
    if (this->deleted_[table_name].count(pkval.AsUnquotedString()) == 0) {
      pks.push_back(std::move(pkval));
    }
  }
  if (pks.size() == 0) {
    return absl::OkStatus();
  }

  // Delete records.
  sqlast::Update del = MakeUpdate(table_name, pkname, std::move(pks), columns);
  UpdateContext context(del, this->conn_, this->lock_);
  MOVE_OR_RETURN(UpdateContext::Result result, context.UpdateAnonymize());
  this->status_ += result.first;

  // Mark dataflow updates.
  std::vector<dataflow::Record> &vec = this->records_[table_name];
  for (dataflow::Record &record : result.second) {
    vec.push_back(std::move(record));
  }

  return absl::OkStatus();
}

/*
 * Main entry point.
 */

absl::StatusOr<sql::SqlResult> GDPRForgetContext::Exec() {
  std::vector<sql::SqlResultSet> result;

  // Begin transaction.
  this->db_->BeginTransaction(true);
  CHECK_STATUS(this->conn_->ctx->AddCheckpoint());

  // This builds the working set of records that this user has ownership or
  // access rights to.
  CHECK_STATUS(this->RecurseOverDependents());

  // Delete data from the user shard.
  CHECK_STATUS(this->DeleteOwnedRecords());

  // Anonymize data in other user shards.
  CHECK_STATUS(this->AnonymizeRecords());

  // Commit transaction.
  this->db_->CommitTransaction();
  CHECK_STATUS(this->conn_->ctx->CommitCheckpoint());

  // Decrement users.
  this->sstate_.DecrementUsers(this->shard_kind_, 1);

  // Update dataflow.
  for (auto &[table_name, records] : this->records_) {
    this->dstate_.ProcessRecords(table_name, std::move(records));
  }

  return sql::SqlResult(this->status_);
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
