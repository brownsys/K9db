// Creation and management of secondary indices.
#include "pelton/shards/sqlengine/index.h"

#include <memory>
#include <utility>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/state.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/shards/state.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

/*
 * Helpers to build index SQL query.
 */
namespace {

// Forward declaration for co-recursion.
absl::StatusOr<std::string> BuildIndexSQL(const std::string &table_name,
                                          const std::string &shard_kind,
                                          const std::string &column_name,
                                          const SharderState &state);

// Direct sharding.
std::string MakeIndexPiece(const std::string &table_name,
                           const std::string &column_name,
                           const DirectInfo &info) {
  std::string col = table_name + "." + column_name;
  std::string sh = table_name + "." + info.column;
  return "SELECT " + col + " AS _col, " + sh + " AS _shard, 1 AS _c " +
         "FROM " + table_name;
}

// Transitive sharding.
std::string MakeIndexPiece(const std::string &table_name,
                           const std::string &column_name,
                           const TransitiveInfo &info) {
  std::string col = table_name + "." + column_name;
  std::string sh = info.index->index_name + "._shard";
  std::string j1 = table_name + "." + info.column;
  std::string j2 = info.index->index_name + "._col";
  std::string c = info.index->index_name + "._c";
  return "SELECT " + col + " AS _col, " + sh + " AS _shard, " + c + " AS _c " +
         "FROM " + table_name + " JOIN " + info.index->index_name + " " +
         "ON " + j1 + " = " + j2;
}

// Variable sharding.
absl::StatusOr<std::string> MakeIndexPiece(const std::string &table_name,
                                           const std::string &shard_kind,
                                           const std::string &column_name,
                                           const VariableInfo &info,
                                           const SharderState &state) {
  // Create an SQL statement that maps the parent column (aliased as _col)
  // to the shards that own rows with that value (aliased as _shard), and their
  // counts (aliased as _c).
  const std::string &parent_table = info.origin_relation;
  const std::string &parent_column = info.origin_column;
  const std::string &nested_name = parent_table + "_index";
  MOVE_OR_RETURN(std::string nested,
                 BuildIndexSQL(parent_table, shard_kind, parent_column, state));

  // Join with nested SQL statement.
  std::string col = table_name + "." + column_name;
  std::string sh = nested_name + "." + "_shard";
  std::string c = nested_name + "." + "_c";
  std::string j1 = table_name + "." + info.column;
  std::string j2 = nested_name + "." + "_col";
  return "SELECT " + col + " AS _col, " + sh + " AS _shard, " + c + " AS _c " +
         "FROM " + table_name + " JOIN (" + nested + ") AS " + nested_name +
         " ON " + j1 + " = " + j2;
}

// Combines all the pieces into a coherent union.
std::string UnionCount(const std::vector<std::string> &pieces) {
  std::string sql = "SELECT _col, _shard, SUM(_c) as _c FROM (";
  for (size_t i = 0; i < pieces.size(); i++) {
    const std::string &piece = pieces.at(i);
    sql += "(" + piece + ")";
    if (i + 1 < pieces.size()) {
      sql += " UNION ";
    }
  }
  sql += ") GROUP BY _col, _shard HAVING _col = ? AND _c > 0";
  return sql;
}

// Recursive function.
absl::StatusOr<std::string> BuildIndexSQL(const std::string &table_name,
                                          const std::string &shard_kind,
                                          const std::string &column_name,
                                          const SharderState &state) {
  // Construct part of the index for each way of owning the table.
  std::vector<std::string> pieces;
  const Table &table = state.GetTable(table_name);
  for (const std::unique_ptr<ShardDescriptor> &desc : table.owners) {
    if (desc->shard_kind == shard_kind) {
      switch (desc->type) {
        case InfoType::DIRECT: {
          const DirectInfo &info = std::get<DirectInfo>(desc->info);
          pieces.push_back(MakeIndexPiece(table_name, column_name, info));
          break;
        }
        case InfoType::TRANSITIVE: {
          const TransitiveInfo &info = std::get<TransitiveInfo>(desc->info);
          pieces.push_back(MakeIndexPiece(table_name, column_name, info));
          break;
        }
        case InfoType::VARIABLE: {
          const VariableInfo &info = std::get<VariableInfo>(desc->info);
          // We do not create an index over VARIABLE edges as an optimization,
          // as they are unneeded for inserts due to FK integrity.
          // However, now we have some transitive table further down that
          // depends on this VARIABLE edge, so we need to embed it into our
          // current index.
          // We need to do this recursively for all VARIABLE edges above, until
          // we reach a DIRECT sharding FK, or a transitive FK with an index.
          MOVE_OR_RETURN(
              std::string piece,
              MakeIndexPiece(table_name, shard_kind, column_name, info, state));
          pieces.push_back(std::move(piece));
          break;
        }
      }
    }
  }

  // Combine pieces into one index.
  ASSERT_RET(pieces.size() > 0, InvalidArgument, "Index over unsharded table!");
  return UnionCount(pieces);
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

  // Create the SQL query that expresses the index.
  MOVE_OR_RETURN(std::string sql,
                 BuildIndexSQL(table_name, shard_kind, column_name, sstate));
  sqlast::CreateView create_stmt(index_name, sql);

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
                                          sqlast::Value &&value,
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
