// Creation and management of secondary indices.
#include "pelton/shards/sqlengine/index.h"

#include <memory>
#include <utility>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/shards/state.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

/*
 * Helpers to build index SQL query.
 */
namespace {

std::string MakeIndexPiece(const std::string &table_name,
                           const std::string &column_name,
                           const DirectInfo &info) {
  std::string qual_col = table_name + "." + column_name;
  std::string qual_shard = table_name + "." + info.column;
  return "SELECT " + qual_col + " AS _col, " + qual_shard + " AS _shard " +
         "FROM " + table_name;
}

std::string MakeIndexPiece(const std::string &table_name,
                           const std::string &column_name,
                           const TransitiveInfo &info) {
  std::string qual_col = table_name + "." + column_name;
  std::string qual_shard = info.index->index_name + "._shard";
  std::string qual_join1 = table_name + "." + info.column;
  std::string qual_join2 = info.index->index_name + "._col";
  return "SELECT " + qual_col + " AS _col, " + qual_shard + " AS _shard " +
         "FROM " + table_name + " JOIN " + info.index->index_name + " " +
         "ON " + qual_join1 + " = " + qual_join2;
}

std::string MakeIndexPiece(const std::string &table_name,
                           const std::string &column_name,
                           const VariableInfo &info) {
  std::string qual_col = table_name + "." + column_name;
  std::string qual_shard = info.index->index_name + "._shard";
  std::string qual_join1 = table_name + "." + info.column;
  std::string qual_join2 = info.index->index_name + "._col";
  return "SELECT " + qual_col + " AS _col, " + qual_shard + " AS _shard " +
         "FROM " + table_name + " JOIN " + info.index->index_name + " " +
         "ON " + qual_join1 + " = " + qual_join2;
}

std::string UnionCount(const std::vector<std::string> &pieces) {
  std::string sql = "SELECT _col, _shard, COUNT(*) FROM ";
  for (size_t i = 0; i < pieces.size(); i++) {
    const std::string &piece = pieces.at(i);
    sql += "(" + piece + ")";
    if (i + 1 < pieces.size()) {
      sql += " UNION ";
    }
  }
  sql += " GROUP BY _col, _shard HAVING _col = ?";
  return sql;
}

}  // namespace

/*
 * Index creation.
 */
absl::StatusOr<IndexDescriptor> Create(const std::string &table_name,
                                       const std::string &shard_kind,
                                       const std::string &column_name,
                                       Connection *connection,
                                       util::UniqueLock *lock) {
  SharderState &sstate = connection->state->SharderState();

  // Create a unique name for the index.
  uint64_t idx = sstate.IncrementIndexCount();
  std::string index_name = "_index_" + std::to_string(idx);

  // Construct part of the index for each way of owning the table.
  std::vector<std::string> pieces;
  Table &table = sstate.GetTable(table_name);
  for (const std::unique_ptr<ShardDescriptor> &desc : table.owners) {
    if (desc->shard_kind == shard_kind) {
      pieces.push_back(visit(
          [&](const auto &info) {
            return MakeIndexPiece(table_name, column_name, info);
          },
          desc->info));
    }
  }

  // Combine pieces into one index.
  sqlast::CreateView create_stmt(index_name, UnionCount(pieces));

  // Plan and create the flow for the index.
  MOVE_OR_RETURN(sql::SqlResult result,
                 view::CreateView(create_stmt, connection, lock));
  ASSERT_RET(result.Success(), Internal, "Error VD index");

  // Return index descriptor.
  return IndexDescriptor{index_name, table_name, shard_kind, column_name};
}

/*
 * Index querying.
 */
std::vector<dataflow::Record> LookupIndex(const IndexDescriptor &index,
                                          dataflow::Value &&value,
                                          Connection *connection,
                                          util::SharedLock *lock) {
  const dataflow::DataFlowState &dstate = connection->state->DataflowState();
  const dataflow::DataFlowGraph &flow = dstate.GetFlow(index.index_name);
  // Lookup by given value as key.
  dataflow::Key key(1);
  key.AddValue(std::move(value));
  return flow.Lookup(key, -1, 0);
}

}  // namespace index
}  // namespace sqlengine
}  // namespace shards
}  // namespace pelton
