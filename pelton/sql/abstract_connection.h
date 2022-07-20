// An abstract connection to some database.
#ifndef PELTON_SQL_ABSTRACT_CONNECTION_H_
#define PELTON_SQL_ABSTRACT_CONNECTION_H_

#include <string>
#include <utility>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/result.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

struct AugInfo {
  size_t index;
  std::string value;
};

struct RecordKeyVecs {
  std::vector<dataflow::Record> records;
  std::vector<std::string> keys;
};

class AbstractConnection {
 public:
  AbstractConnection() = default;
  virtual ~AbstractConnection() = default;

  // Close the connection.
  virtual void Open(const std::string &db_name) = 0;
  virtual void Close() = 0;

  // Execute statement by type.
  virtual bool ExecuteStatement(const sqlast::AbstractStatement *sql,
                                const std::string &shard_name) = 0;

  virtual std::pair<int, uint64_t> ExecuteUpdate(
      const sqlast::AbstractStatement *sql, const std::string &shard_name) = 0;

  virtual SqlResultSet ExecuteQueryShard(const sqlast::AbstractStatement *sql,
                                         const dataflow::SchemaRef &schema,
                                         const std::vector<AugInfo> &augments,
                                         const std::string &shard_name) = 0;

  virtual SqlResultSet ExecuteQuery(const sqlast::AbstractStatement *sql,
                                    const dataflow::SchemaRef &schema,
                                    const std::vector<AugInfo> &augs) = 0;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ABSTRACT_CONNECTION_H_
