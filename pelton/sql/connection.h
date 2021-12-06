// An abstract connection to some database.
#ifndef PELTON_SQL_CONNECTION_H__
#define PELTON_SQL_CONNECTION_H__

#include <string>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

struct AugInfo {
  size_t index;
  std::string value;
};

class PeltonConnection {
 public:
  PeltonConnection() = default;
  virtual ~PeltonConnection() = default;

  // Close the connection.
  virtual void Close() = 0;

  // Execute statement by type.
  virtual bool ExecuteStatement(const sqlast::AbstractStatement *sql) = 0;
  virtual int ExecuteUpdate(const sqlast::AbstractStatement *sql) = 0;
  virtual std::vector<dataflow::Record> ExecuteQuery(
      const sqlast::AbstractStatement *sql, const dataflow::SchemaRef &schema,
      const std::vector<AugInfo> &augments) = 0;
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTION_H__
