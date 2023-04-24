#include "k9db/sql/factory.h"

#include "k9db/sql/rocksdb/rocksdb_connection.h"

namespace k9db {
namespace sql {

std::unique_ptr<Connection> MakeConnection() {
  return std::make_unique<rocks::RocksdbConnection>();
}

}  // namespace sql
}  // namespace k9db
