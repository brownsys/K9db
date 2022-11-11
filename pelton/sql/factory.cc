#include "pelton/sql/factory.h"

#include "pelton/sql/rocksdb/connection.h"

namespace pelton {
namespace sql {

std::unique_ptr<AbstractConnection> MakeConnection() {
  return std::make_unique<RocksdbConnection>();
}

}  // namespace sql
}  // namespace pelton
