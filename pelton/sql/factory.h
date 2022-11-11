#ifndef PELTON_SQL_FACTORY_H_
#define PELTON_SQL_FACTORY_H_

#include <memory>

#include "pelton/sql/abstract_connection.h"

namespace pelton {
namespace sql {

std::unique_ptr<AbstractConnection> MakeConnection();

}  // namespace sql
}  // namespace pelton

#endif  // PELTON_SQL_FACTORY_H_
