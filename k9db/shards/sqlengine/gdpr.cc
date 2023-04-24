// GDPR (GET | FORGET) statements handling.
#include "k9db/shards/sqlengine/gdpr.h"

#include <memory>

#include "glog/logging.h"
#include "k9db/dataflow/schema.h"
#include "k9db/shards/sqlengine/select.h"
#include "k9db/util/status.h"

namespace k9db {
namespace shards {
namespace sqlengine {

// Helpers.
namespace {

template <typename T>
std::string Join(const T &set) {
  std::string str = "";
  for (const std::string &c : set) {
    str += c + ",";
  }
  return str;
}

sqlast::Select MakeSelect(const TableName &table, const std::string &column,
                          std::vector<sqlast::Value> &&invals) {
  // Build select statement on the form:
  // SELECT * FROM <tbl> WHERE <column> IN (<invals>);
  sqlast::Select select{table};
  select.AddColumn(sqlast::Select::ResultColumn("*"));

  std::unique_ptr<sqlast::BinaryExpression> binexpr =
      std::make_unique<sqlast::BinaryExpression>(sqlast::Expression::Type::IN);
  binexpr->SetLeft(std::make_unique<sqlast::ColumnExpression>(column));
  binexpr->SetRight(
      std::make_unique<sqlast::LiteralListExpression>(std::move(invals)));
  select.SetWhereClause(std::move(binexpr));
  return select;
}

}  // namespace.

/*
 * Finds whether there are records in the table matching the given condition
 * and owned by the given shard.
 */
bool GDPRContext::OwnedBy(const std::string &table_name,
                          const Condition &condition) {
  // Go one value at a time, find the shards of that value.
  for (size_t i = 0; i < condition.values.size(); i++) {
    const sqlast::Value &value = condition.values.at(i);
    std::unordered_set<util::ShardName> shards =
        this->db_->FindShards(table_name, condition.column, value);
    if (shards.count(this->shard_) > 0) {
      return true;
    }
  }
  return false;
}

/*
 * Finds all the columns along which this record is owned by the user of the
 * given shard.
 */
std::unordered_set<std::string> GDPRContext::OwnedThrough(
    const std::string &table_name, const dataflow::Record &record) {
  std::unordered_set<std::string> columns;
  const Table &table = this->sstate_.GetTable(table_name);

  // How many potential ways of owning this record are there at most.
  size_t owner_count = 0;
  const ShardDescriptor *some_owner = nullptr;
  for (const std::unique_ptr<ShardDescriptor> &desc : table.owners) {
    if (desc->shard_kind == this->shard_kind_) {
      owner_count++;
      some_owner = desc.get();
    }
  }

  // Optimization: only one way of owning.
  if (owner_count == 1) {
    if (some_owner->type == InfoType::VARIABLE) {
      const std::string &parent_table = some_owner->next_table();
      const std::string &parent_column = some_owner->upcolumn();
      columns.insert(parent_table + "(" + parent_column + ")");
    } else {
      columns.insert(EXTRACT_VARIANT(column, some_owner->info));
    }
    return columns;
  }

  // Need to look at each way of sharding this table.
  for (const std::unique_ptr<ShardDescriptor> &desc : table.owners) {
    if (desc->shard_kind != this->shard_kind_) {
      continue;
    }

    // Identify the ownership FK columns and parent table.
    size_t column_index = EXTRACT_VARIANT(column_index, desc->info);
    size_t parent_column = desc->upcolumn_index();
    const std::string &parent_table = desc->next_table();

    // Build parent condition.
    Condition condition;
    condition.column = parent_column;
    condition.values.push_back(record.GetValue(column_index));

    // Is shard an owner of parent?
    if (this->OwnedBy(parent_table, condition)) {
      if (desc->type == InfoType::VARIABLE) {
        columns.insert(parent_table + "(" + desc->upcolumn() + ")");
      } else {
        columns.insert(EXTRACT_VARIANT(column, desc->info));
      }
    }
  }

  return columns;
}

/*
 * Add the given records to the records set.
 */
void GDPRContext::AddOwnedRecords(
    const std::string &table_name,
    const std::vector<dataflow::Record> &records) {
  RecordPKMap &map = this->records_[table_name];
  for (const dataflow::Record &record : records) {
    // How is this record owned.
    std::unordered_set<std::string> columns =
        this->OwnedThrough(table_name, record);

    // Find PK.
    size_t pkidx = record.schema().keys().at(0);
    std::string str = record.GetValue(pkidx).AsUnquotedString();

    // Add record and columns.
    auto it = map.find(str);
    if (it == map.end()) {
      map.emplace(std::piecewise_construct,
                  std::forward_as_tuple(std::move(str)),
                  std::forward_as_tuple(record.Copy(), std::move(columns)));
    } else {
      for (const std::string &column : columns) {
        it->second.second.insert(column);
      }
    }
  }
}
void GDPRContext::AddAccessedRecords(
    const std::string &table_name, const std::string &along_column,
    const std::vector<dataflow::Record> &records) {
  RecordPKMap &map = this->records_[table_name];
  for (const dataflow::Record &record : records) {
    // Find PK.
    size_t pkidx = record.schema().keys().at(0);
    std::string str = record.GetValue(pkidx).AsUnquotedString();

    // Add record and columns.
    auto it = map.find(str);
    if (it == map.end()) {
      map.emplace(
          std::piecewise_construct, std::forward_as_tuple(std::move(str)),
          std::forward_as_tuple(record.Copy(),
                                std::unordered_set<std::string>{along_column}));
    } else {
      it->second.second.insert(along_column);
    }
  }
}

/*
 * Starts the recursive visiting of owned and accessed tables.
 * Every time this visits a table, it calls one of the two handles above.
 * This function is responsible for determining the FK condition that captures
 * the dependency between a parent and a dependent table.
 */
absl::Status GDPRContext::RecurseOverDependents() {
  const Shard &current_shard = this->sstate_.GetShard(this->shard_kind_);

  // Iterate over all owned tables.
  for (const auto &table_name : current_shard.owned_tables) {
    sql::SqlResultSet rset =
        this->db_->GetShard(table_name, this->shard_.Copy());

    // Add records to working set.
    this->AddOwnedRecords(table_name, rset.rows());

    // Iterate over access dependents.
    CHECK_STATUS(this->RecurseOverAccessDependents(table_name, rset.Vec()));
  }
  return absl::OkStatus();
}

/*
 * Recurses over access dependent tables.
 */
absl::Status GDPRContext::RecurseOverAccessDependents(
    const std::string &table_name, std::vector<dataflow::Record> &&records) {
  // Iterate over all the access dependents.
  const Table &table = this->sstate_.GetTable(table_name);
  for (const auto &[next_table, desc] : table.access_dependents) {
    if (desc->shard_kind != this->shard_kind_) {
      continue;
    }

    // Find the FK key columns.
    const std::string &fk_column = EXTRACT_VARIANT(column, desc->info);
    std::vector<sqlast::Value> vs;
    for (const dataflow::Record &record : records) {
      vs.push_back(record.GetValue(desc->upcolumn_index()));
    }

    // Get matching records in dependent table.
    sqlast::Select select = MakeSelect(next_table, fk_column, std::move(vs));
    SelectContext context(select, this->conn_, this->lock_);
    MOVE_OR_RETURN(sql::SqlResult result, context.ExecWithinTransaction());
    sql::SqlResultSet &rset = result.ResultSets().front();

    // Add access records to working set.
    std::string along_column = fk_column;
    if (desc->type == InfoType::VARIABLE) {
      along_column = desc->next_table() + "(" + desc->upcolumn() + ")";
    }
    this->AddAccessedRecords(next_table, along_column, rset.rows());

    // Recurse.
    CHECK_STATUS(this->RecurseOverAccessDependents(next_table, rset.Vec()));
  }
  return absl::OkStatus();
}

/*
 * Traverse records_ calling HandleAnonimzation on each record.
 */
GDPRContext::GroupedRecords GDPRContext::GroupRecordsByAnonimzeColumns() {
  GroupedRecords result;
  for (auto &[table_name, map] : this->records_) {
    std::vector<ColumnsAndRecords> &vec = result[table_name];
    std::unordered_map<std::string, size_t> str_to_index;
    for (auto &[_, pair] : map) {
      std::string str = Join(pair.second);
      auto it = str_to_index.find(str);
      if (it == str_to_index.end()) {
        str_to_index.emplace(str, vec.size());
        vec.emplace_back();
        vec.back().first = std::move(pair.second);
        vec.back().second.push_back(std::move(pair.first));
      } else {
        vec.at(it->second).second.push_back(std::move(pair.first));
      }
    }
    map.clear();
  }
  this->records_.clear();
  return result;
}

}  // namespace sqlengine
}  // namespace shards
}  // namespace k9db
