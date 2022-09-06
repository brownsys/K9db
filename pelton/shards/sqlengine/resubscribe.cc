
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
  std::ifstream user_data("/home/pelton/user_data.txt");
  // TO-DO assrt that file has content and is in right format
  getline(user_data, line);
  std::string user_id = line;
  std::cout << "user id: " << user_id << std::endl;
  size_t rows = 0;
  while (getline(user_data, line)) {
    // TO-DO: assuming table name does not have any spaces
    auto space_loc = line.find(" ");
    auto table_name = line.substr(0, space_loc);
    auto remainder = line.substr(space_loc + 1);
    space_loc = remainder.find(" ");
    auto sharded_table_name = remainder.substr(0, space_loc);
    auto num_records = std::stoi(remainder.substr(space_loc + 1));
    rows += num_records;
    auto schema =
        connection->state->dataflow_state()->GetTableSchema(table_name);
    auto pks = schema.keys();
    CHECK_EQ(pks.size(), 1);
    std::cout << "table: " << table_name << " pks index: " << pks[0]
              << std::endl;
    for (int i = 0; i < num_records; i++) {
      getline(user_data, line);
      std::cout << "the line is: " << line << std::endl;
      auto record = split_record(line);
      auto col_types = schema.column_types();
      for (int j = 0; j < record.size(); j++) {
        std::cout << "col_type: " << col_types[j] << std::endl;
        if (col_types[j] == pelton::sqlast::ColumnDefinition::Type::TEXT ||
            col_types[j] == pelton::sqlast::ColumnDefinition::Type::DATETIME) {
          if (record[j] != "NULL") {
            std::cout << "reached" << std::endl;
            record[j] = "'" + record[j] + "'";
          }
        }
      }
      auto select_stmt = pelton::sqlast::Select(table_name);
      auto bin_exp = new pelton::sqlast::BinaryExpression(
          pelton::sqlast::Expression::Type::EQ);
      auto pk_col_name = schema.column_names()[pks[0]];
      bin_exp->SetLeft(
          std::make_unique<pelton::sqlast::ColumnExpression>(pk_col_name));
      bin_exp->SetRight(
          std::make_unique<pelton::sqlast::LiteralExpression>(record[pks[0]]));
      std::unique_ptr<pelton::sqlast::BinaryExpression> bin_exp_ptr(bin_exp);
      select_stmt.SetWhereClause(std::move(bin_exp_ptr));
      for (std::string i : schema.column_names()) {
        select_stmt.AddColumn(i);
      }

      std::cout << "here 11" << std::endl;
      auto select_result = pelton::shards::sqlengine::select::Shard(
          select_stmt, connection, false);
      std::cout << "here 12" << std::endl;

      for (int i = 0; i < select_result.value().ResultSets().size(); i++) {
        auto records = select_result.value().ResultSets()[i].Vec();
        std::cout << "here 1" << std::endl;
        if (records.size() != 0) {
          std::cout << "here 1.5" << std::endl;
          for (int j = 0; j < record.size(); j++) {
            std::cout << "here 2" << std::endl;
            std::cout << "test: " << records[0].GetValueString(j) << std::endl;
            std::string value = records[0].GetValueString(j);
            if (col_types[j] == pelton::sqlast::ColumnDefinition::Type::TEXT ||
                col_types[j] ==
                    pelton::sqlast::ColumnDefinition::Type::DATETIME) {
              if (record[j] != "NULL") {
                value = "'" + value + "'";
              }
            }
            record[j] = value;
          }
          break;
        }
      }
      auto shard_column = sharded_table_name.substr(table_name.size() + 1);
      std::cout << "shard column: " << shard_column << std::endl;
      for (auto ele : schema.column_names()) {
        std::cout << ele << std::endl;
      }
      auto remove_index = std::find(schema.column_names().begin(),
                                    schema.column_names().end(), shard_column) -
                          schema.column_names().begin();
      // TO_DO - assert that find working as expected
      record.erase(record.begin() + remove_index);
      auto ins = pelton::sqlast::Insert(sharded_table_name, false);
      ins.SetValues(std::move(record));
      SharedLock lock = connection->state->sharder_state()->ReaderLock();
      auto &exec = connection->executor;
      exec.Shard(&ins, user_id);
      std::cout << "reached here" << std::endl;
    }
  }
  user_data.close();
  return sql::SqlResult(rows);
}
}  // namespace resubscribe
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
