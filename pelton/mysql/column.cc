#include "pelton/mysql/column.h"

namespace pelton {
namespace mysql {

// Column.
Column::Column(const mysqlx::Column &col)
    : type_(col.getType()), name_(col.getColumnName()) {}

Column::Column(mysqlx::Type type, const std::string &name)
    : type_(type), name_(name) {}

mysqlx::Type Column::getType() const { return this->type_; }

const std::string &Column::getColumnName() const { return this->name_; }

}  // namespace mysql
}  // namespace pelton
