#ifndef PELTON_SQL_FACTORY_H_
#define PELTON_SQL_FACTORY_H_

#include <memory>

#include "pelton/sql/connection.h"

namespace pelton {
namespace sql {

std::unique_ptr<Connection> MakeConnection();

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_FACTORY_H_
