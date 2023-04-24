// GDPR (GET | FORGET) statements handling.
#ifndef K9DB_SHARDS_SQLENGINE_GDPR_H_
#define K9DB_SHARDS_SQLENGINE_GDPR_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "k9db/connection.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/state.h"
#include "k9db/shards/state.h"
#include "k9db/shards/types.h"
#include "k9db/sql/connection.h"
#include "k9db/sql/result.h"
#include "k9db/sqlast/ast.h"
#include "k9db/util/shard_name.h"
#include "k9db/util/upgradable_lock.h"

namespace k9db {
namespace shards {
namespace sqlengine {

class GDPRContext {
 public:
  GDPRContext(const sqlast::GDPRStatement &stmt, Connection *conn,
              util::SharedLock *lock)
      : stmt_(stmt),
        shard_kind_(stmt.shard_kind()),
        user_id_(stmt.user_id()),
        user_id_str_(user_id_.AsUnquotedString()),
        shard_(shard_kind_, user_id_str_),
        conn_(conn),
        sstate_(conn->state->SharderState()),
        dstate_(conn->state->DataflowState()),
        db_(conn->session.get()),
        lock_(lock) {}

  virtual ~GDPRContext() = default;

 protected:
  /* Represents a "WHERE <column> IN <values>" condition. */
  struct Condition {
    size_t column;
    std::vector<sqlast::Value> values;
  };

  /* Add the given records to the records set. */
  void AddOwnedRecords(const std::string &table_name,
                       const std::vector<dataflow::Record> &records);
  void AddAccessedRecords(const std::string &table_name,
                          const std::string &along_column,
                          const std::vector<dataflow::Record> &records);

  /* Starts the recursive visiting of owned and accessed tables.
     Every time this visits a table, it calls one of the two handles above.
     This function is responsible for determining the FK condition that captures
     the dependency between a parent and a dependent table. */
  absl::Status RecurseOverDependents();

  /* Reorganize records so they are grouped by the columns along which
     anonimzation rules apply. */
  using Columns = std::unordered_set<std::string>;
  using ColumnsAndRecords = std::pair<Columns, std::vector<dataflow::Record>>;
  using GroupedRecords =
      std::unordered_map<std::string, std::vector<ColumnsAndRecords>>;
  GroupedRecords GroupRecordsByAnonimzeColumns();

 protected:
  /* Members. */
  const sqlast::GDPRStatement &stmt_;
  const std::string &shard_kind_;
  const sqlast::Value &user_id_;
  std::string user_id_str_;
  util::ShardName shard_;

  // K9db connection.
  Connection *conn_;

  // Connection components.
  SharderState &sstate_;
  dataflow::DataFlowState &dstate_;
  sql::Session *db_;

  // Shared Lock so we can read from the states safetly.
  util::SharedLock *lock_;

 private:
  /* Finds whether there are records in the table matching the given condition
     and owned by the given shard. */
  bool OwnedBy(const std::string &table_name, const Condition &condition);

  /* Finds all the columns along which this record is owned by the user of the
     given shard. */
  std::unordered_set<std::string> OwnedThrough(const std::string &table_name,
                                               const dataflow::Record &record);

  /* Recurses over access dependent tables. */
  absl::Status RecurseOverAccessDependents(
      const std::string &table_name, std::vector<dataflow::Record> &&records);

  // Keep track of the relevant records to be deleted or returned grouped by
  // table.

  using RecordAndPath = std::pair<dataflow::Record, Columns>;
  using RecordPKMap = std::unordered_map<std::string, RecordAndPath>;
  std::unordered_map<std::string, RecordPKMap> records_;
};

class GDPRGetContext : GDPRContext {
 public:
  GDPRGetContext(const sqlast::GDPRStatement &stmt, Connection *conn,
                 util::SharedLock *lock)
      : GDPRContext(stmt, conn, lock) {}

  absl::StatusOr<sql::SqlResult> Exec();
};

class GDPRForgetContext : GDPRContext {
 public:
  GDPRForgetContext(const sqlast::GDPRStatement &stmt, Connection *conn,
                    util::SharedLock *lock)
      : GDPRContext(stmt, conn, lock), status_(0) {}

  absl::StatusOr<sql::SqlResult> Exec();

 private:
  absl::Status DeleteOwnedRecords();

  /* Anonymization and helpers. */
  absl::Status AnonymizeRecords();

  absl::Status DeleteRecords(const std::string &table_name,
                             const std::vector<dataflow::Record> &records);

  absl::Status AnonymizeRecords(const std::string &table_name,
                                const std::unordered_set<std::string> &columns,
                                const std::vector<dataflow::Record> &records);

  // Track the number of affected records and the dataflow updates.
  int status_;
  std::unordered_map<std::string, std::vector<dataflow::Record>> records_;
  std::unordered_map<std::string, std::unordered_set<std::string>> deleted_;
};

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db

#endif  // K9DB_SHARDS_SQLENGINE_GDPR_H_
