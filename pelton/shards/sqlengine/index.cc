// Creation and management of secondary indices.
#include "pelton/shards/sqlengine/index.h"

#include <utility>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/shards/sqlengine/view.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/status.h"

namespace pelton {
namespace shards {
namespace sqlengine {
namespace index {

/*
 * Index creation.
 */

namespace {

std::string EncodeSql(const IndexDescriptor &descriptor) {
  std::string sql = "";
  const std::string &indexed_table = descriptor.indexed_table;
  std::string indexed_column = indexed_table + "." + descriptor.indexed_column;
  switch (descriptor.type) {
    case IndexType::SIMPLE: {
      const SimpleIndexInfo &info = std::get<SimpleIndexInfo>(descriptor.info);
      std::string shard_column = indexed_table + "." + info.shard_column;
      sql += "SELECT " + indexed_column + ", " + shard_column + ", COUNT(*) ";
      sql += "FROM " + indexed_table + " ";
      sql += "GROUP BY " + indexed_column + ", " + shard_column + " ";
      sql += "HAVING " + indexed_column + " = ?";
      break;
    }
    case IndexType::JOINED: {
      const JoinedIndexInfo &info = std::get<JoinedIndexInfo>(descriptor.info);
      const std::string &join_table = info.join_table;
      std::string join_column1 = indexed_table + "." + info.join_column1;
      std::string join_column2 = join_table + "." + info.join_column2;
      std::string shard_column = join_table + "." + info.shard_column;
      sql += "SELECT " + indexed_column + ", " + shard_column + ", COUNT(*) ";
      sql += "FROM " + indexed_table + " JOIN " + join_table + " ";
      sql += "ON " + join_column1 + " = " + join_column2 + " ";
      sql += "GROUP BY " + indexed_column + ", " + shard_column + " ";
      sql += "HAVING " + indexed_column + " = ?";
      break;
    }
  }
  return sql;
}

}  // namespace

absl::StatusOr<sql::SqlResult> CreateIndex(const IndexDescriptor &index,
                                           Connection *connection,
                                           util::UniqueLock *lock) {
  sqlast::CreateView create_stmt(index.index_name, EncodeSql(index));
  return view::CreateView(create_stmt, connection, lock);
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
