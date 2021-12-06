// Connection to rocksdb.
#ifndef PELTON_SQL_CONNECTIONS_ROCKSDB_H__
#define PELTON_SQL_CONNECTIONS_ROCKSDB_H__

#include <string>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sql/connection.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace sql {

class RocksdbConnection : public PeltonConnection {
 public:
  RocksdbConnection(const std::string &path, const std::string &db,
                    const std::string &user, const std::string &pass) {}

  ~RocksdbConnection() { this->Close(); }

  // Close the connection.
  void Close() override {}

  // Execute statement by type.
  bool ExecuteStatement(const sqlast::AbstractStatement *sql) override {
    return true;
  }
  int ExecuteUpdate(const sqlast::AbstractStatement *sql) override { return 0; }
  std::vector<dataflow::Record> ExecuteQuery(
      const sqlast::AbstractStatement *sql, const dataflow::SchemaRef &schema,
      const std::vector<AugInfo> &augments) override {
    return {};
  }
};

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_CONNECTIONS_ROCKSDB_H__
