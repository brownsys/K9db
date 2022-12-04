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

// We can reuse some indices but it will make our deletes inconsistent when
// it deletes things in the table where the index is re-used while leaving
// rows in the table the index is defined over dangling.
// E.g. deleting a group without deleting its association.
#define PELTON_INDEX_REUSE false

/*
 * Helpers to build index SQL query.
 */
namespace {

std::string MakeIndexPiece(const std::string &table_name,
                           const std::string &column_name,
                           const DirectInfo &info) {
  std::string col = table_name + "." + column_name;
  std::string sh = table_name + "." + info.column;
  return "SELECT " + col + " AS _col, " + sh + " AS _shard, 1 AS _c " +
         "FROM " + table_name;
}

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

std::string MakeIndexPiece(const std::string &table_name,
                           const std::string &column_name,
                           const VariableInfo &info) {
  std::string col = table_name + "." + column_name;
  std::string sh = info.index->index_name + "._shard";
  std::string j1 = table_name + "." + info.column;
  std::string j2 = info.index->index_name + "._col";
  std::string c = info.index->index_name + "._c";
  // Reuse optimization; saves a join. This is used if a new view is still
  // needed for the index due to other ways of owning (i.e. to feed into UNION).
  if (PELTON_INDEX_REUSE && info.column == column_name) {
    return "SELECT " + j2 + " AS _col, " + sh + " AS _shard, " + c + " AS _c " +
           "FROM " + info.index->index_name;
  }
  // No reuse optimization.
  return "SELECT " + col + " AS _col, " + sh + " AS _shard, " + c + " AS _c " +
         "FROM " + table_name + " JOIN " + info.index->index_name + " " +
         "ON " + j1 + " = " + j2;
}

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
  std::vector<IndexDescriptor *> reuse;
  bool can_be_reused = PELTON_INDEX_REUSE;
  Table &table = sstate.GetTable(table_name);
  for (const std::unique_ptr<ShardDescriptor> &desc : table.owners) {
    if (desc->shard_kind == shard_kind) {
      switch (desc->type) {
        case InfoType::DIRECT: {
          const DirectInfo &info = std::get<DirectInfo>(desc->info);
          pieces.push_back(MakeIndexPiece(table_name, column_name, info));
          can_be_reused = false;
          break;
        }
        case InfoType::TRANSITIVE: {
          const TransitiveInfo &info = std::get<TransitiveInfo>(desc->info);
          pieces.push_back(MakeIndexPiece(table_name, column_name, info));
          can_be_reused = false;
          break;
        }
        case InfoType::VARIABLE: {
          const VariableInfo &info = std::get<VariableInfo>(desc->info);
          // Usually, an index already exists over the origin (i.e. association)
          // table. Because of the inverse directionality of an OWNS FK, that
          // index may be used directly without a join! We check if this is the
          // case below.
          pieces.push_back(MakeIndexPiece(table_name, column_name, info));
          if (info.column == column_name) {
            reuse.push_back(info.index);
          } else {
            can_be_reused = false;
          }
          break;
        }
      }
    }
  }

  // Try to reuse an existing index and avoid creating an index if possible.
  if (can_be_reused && reuse.size() == 1) {
    const std::string &reused_index = reuse.front()->index_name;
    return IndexDescriptor{reused_index, table_name, shard_kind, column_name};
  }

  // Combine pieces into one index.
  ASSERT_RET(pieces.size() > 0, InvalidArgument, "Index over unsharded table!");
  sqlast::CreateView create_stmt(index_name, UnionCount(pieces));

  // Plan and create the flow for the index.
  MOVE_OR_RETURN(sql::SqlResult result,
                 view::CreateView(create_stmt, connection, lock, true));
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
  const SharderState &sstate = connection->state->SharderState();
  if (!sstate.IndicesEnabled()) {
    return FakeLookup(index, std::move(value), connection, lock);
  }

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
