#ifndef K9DB_SQL_FACTORY_H_
#define K9DB_SQL_FACTORY_H_

#include <memory>

#include "k9db/sql/connection.h"

namespace k9db {
namespace sql {

std::unique_ptr<Connection> MakeConnection();

}  // namespace sql
}  // namespace k9db

#endif  // K9DB_SQL_FACTORY_H_
