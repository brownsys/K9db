
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/match.h"
#include "pelton/shards/sqlengine/delete.h"
#include "pelton/shards/sqlengine/gdpr.h"
#include "pelton/shards/sqlengine/index.h"
#include "pelton/shards/sqlengine/insert.h"
#include "pelton/shards/sqlengine/select.h"
#include "pelton/shards/sqlengine/update.h"
#include "pelton/shards/sqlengine/util.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sqlast/ast_statements.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace resubscribe {

namespace {
std::vector<std::string> split_record(std::string rec) {
  std::vector<std::string> ret;
  auto len = rec.length();
  auto sep = rec.find("|");
  while (sep + 1 != len) {
    ret.push_back(rec.substr(0, sep));
    rec = rec.substr(sep + 1);
    auto sep = rec.find("|");
  }
  return ret;
}
}  // namespace

absl::StatusOr<sql::SqlResult> Resubscribe(Connection *connection) {
  std::string line;
  std::ifstream user_data("/home/pelton/user_data.txt");
  // TO-DO assrt that file has content and is in right format
  getline(user_data, line);
  // std::string user_id = line
  size_t rows = 0;
  while (getline(user_data, line)) {
    // TO-DO: assuming table name does not have any spaces
    auto space_loc = line.find(" ");
    auto table_name = line.substr(0, space_loc);
    auto num_records = stoi(line.substr(space_loc + 1));
    rows += num_records;
    auto schema = connection->state->dataflow_state()->GetTableSchema(
        table_name) for (int i = 0; i < num_records; i++) {
      getline(user_data, line);
      auto ins = Insert(table_name, false);
      auto record = split_records(line);
      auto col_types = schema.column_types();
      for (int j = 0; j < record.size(), j++) {
        if (col_types[i] == ColumnDefinition::Type::TEXT ||
            col_types[i] == ColumnDefinition::Type::DATETIME) {
          record[i] = "'" + record[i] + "'";
        }
      }
      ins.SetValues(record);
      // TO-DO how to I write the insert statement and spefically where do I get
      // the lock from?
      SharedLock lock = connection->state->sharder_state()->ReaderLock();
      // TO-DO - ignore return value?
      Shard(ins, connection, &lock, true);
    }
    return sql::SqlResult(rows);
  }
}  // namespace resubscribe
}  // namespace resubscribe
}  // namespace sqlengine
}  // namespace shards
