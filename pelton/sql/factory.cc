#include "pelton/sql/factory.h"

#include "pelton/sql/rocksdb/rocksdb_connection.h"

namespace pelton {
namespace sql {

std::unique_ptr<Connection> MakeConnection() {
  return std::make_unique<rocks::RocksdbConnection>();
}

}  // namespace sql
}  // namespace pelton
