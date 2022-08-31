
#include "pelton/shards/sqlengine/resubscribe.h"

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
  rec = rec.substr(1);
  auto len = rec.length();
  auto sep = rec.find("|");
  while (true) {
    ret.push_back(rec.substr(0, sep));
    std::cout << "record element: " << rec.substr(0, sep) << std::endl;
    rec = rec.substr(sep + 1);
    if (rec == "") {
      break;
    }
    sep = rec.find("|");
  }
  return ret;
}
}  // namespace

absl::StatusOr<sql::SqlResult> Resubscribe(Connection *connection) {
  std::string line;
  std::ifstream user_data("/home/pelton/user_data2.txt");
  // TO-DO assrt that file has content and is in right format
  getline(user_data, line);
  // std::string user_id = line
  size_t rows = 0;
  while (getline(user_data, line)) {
    // TO-DO: assuming table name does not have any spaces
    auto space_loc = line.find(" ");
    auto table_name = line.substr(0, space_loc);
    auto num_records = std::stoi(line.substr(space_loc + 1));
    rows += num_records;
    auto schema =
        connection->state->dataflow_state()->GetTableSchema(table_name);
    for (int i = 0; i < num_records; i++) {
      getline(user_data, line);
      std::cout << "the line is: " << line << std::endl;
      auto ins = pelton::sqlast::Insert(table_name, false);
      auto record = split_record(line);
      auto col_types = schema.column_types();
      for (int j = 0; j < record.size(); j++) {
        std::cout << "col_type: " << col_types[j] << std::endl;
        if (col_types[j] == pelton::sqlast::ColumnDefinition::Type::TEXT ||
            col_types[j] == pelton::sqlast::ColumnDefinition::Type::DATETIME) {
          std::cout << "reached" << std::endl;
          record[j] = "'" + record[j] + "'";
        }
      }
      std::cout << "printing record after mod" << std::endl;
      for (auto i : record) {
        std::cout << i << std::endl;
      }
      ins.SetValues(std::move(record));
      // TO-DO how to I write the insert statement and spefically where do I get
      // the lock from?
      SharedLock lock = connection->state->sharder_state()->ReaderLock();
      // TO-DO - ignore return value?
      pelton::shards::sqlengine::insert::Shard(ins, connection, &lock, true);
    }
  }
  user_data.close();
  return sql::SqlResult(rows);
}
}  // namespace resubscribe
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
