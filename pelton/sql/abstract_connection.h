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

struct InsertResult {
  int status;               // < 0 is error, >= 0 number of affected rows.
  uint64_t last_insert_id;  // If PK is auto_increment, this has the value.
};

struct AugInfo {
  size_t index;
  std::string value;
};

class AbstractConnection {
 public:
  AbstractConnection() = default;
  virtual ~AbstractConnection() = default;

  // Close the connection.
  virtual void Open(const std::string &db_name) = 0;
  virtual void Close() = 0;

  // Statements.
  virtual bool ExecuteCreateTable(const sqlast::CreateTable &sql) = 0;
  virtual bool ExecuteCreateIndex(const sqlast::CreateIndex &sql) = 0;

  // Updates.
  virtual InsertResult ExecuteInsert(const sqlast::Insert &sql,
                                     const std::string &shard_name) = 0;

  virtual int ExecuteUpdate(const sqlast::Update &sql) = 0;

  virtual int ExecuteDelete(const sqlast::Delete &sql) = 0;

  // Queries / Select.
  virtual SqlResultSet ExecuteQuery(const sqlast::Select &sql,
                                    const dataflow::SchemaRef &schema,
                                    const std::vector<AugInfo> &augs) = 0;

  virtual SqlResultSet ExecuteQueryShard(const sqlast::Select &sql,
                                         const dataflow::SchemaRef &schema,
                                         const std::vector<AugInfo> &augments,
                                         const std::string &shard_name) = 0;

  virtual SqlResultSet ExecuteSelect(const sqlast::Select &sql,
                                     const dataflow::SchemaRef &out_schema,
                                     const std::vector<AugInfo> &augments) = 0;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_ABSTRACT_CONNECTION_H_
