// GDPR GET statements handling.
// clang-format off
// NOLINTNEXTLINE
#include "k9db/shards/sqlengine/gdpr.h"
// clang-format on

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

/*
 * Main entry point.
 */

absl::StatusOr<sql::SqlResult> GDPRGetContext::Exec() {
  std::vector<sql::SqlResultSet> result;

  // Begin transaction.
  this->db_->BeginTransaction(false);

  // This builds the working set of records that this user has ownership or
  // access rights to.
  CHECK_STATUS(this->RecurseOverDependents());

  // Get working set organized by anonimization rules.
  GroupedRecords working_set = this->GroupRecordsByAnonimzeColumns();
  for (auto &[table_name, vec] : working_set) {
    std::vector<dataflow::Record> output;
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
      std::unordered_set<size_t> anonymize;
      for (const sqlast::AnonymizationRule &rule : rules) {
        if (rule.GetType() != sqlast::AnonymizationOpTypeEnum::GET) {
          continue;
        }
        if (columns.count(rule.GetDataSubject()) == 0) {
          continue;
        }
        for (const std::string &column : rule.GetAnonymizeColumns()) {
          anonymize.insert(table.schema.IndexOf(column));
        }
        if (rule.GetAnonymizeColumns().size() == 0) {
          delete_row = true;
          break;
        }
      }
      if (delete_row) {
        continue;
      }

      // Records should be kept, but anonimized.
      for (dataflow::Record &record : records) {
        for (size_t column : anonymize) {
          record.SetNull(true, column);
        }
        output.push_back(std::move(record));
      }
    }
    // Push result set.
    result.emplace_back(table.schema, std::move(output));
  }

  // Nothing to commit; read only.
  this->db_->RollbackTransaction();

  // Turn into sql::SqlResult.
  return sql::SqlResult(std::move(result));
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
