#include "pelton/mysql/row.h"

namespace pelton {
namespace mysql {

// Row.
std::string Row::StringRepr(size_t colnum) const {
  std::string delim("\0", 1);
  std::string str = "";
  for (size_t i = 0; i < colnum; i++) {
    const mysqlx::Value &value = this->get(i);
    switch (value.getType()) {
      case mysqlx::Value::STRING:
        str += value.get<std::string>();
        break;
      case mysqlx::Value::VNULL:
        str += delim;
        break;
      case mysqlx::Value::UINT64:
        str += std::to_string(uint64_t(value));
        break;
      case mysqlx::Value::INT64:
        str += std::to_string(int64_t(value));
        break;
      default:
        continue;
    }
    str += delim;
  }
  return str;
}

// MySqlRow.
const mysqlx::Value &MySqlRow::get(size_t pos) const { return this->row_[pos]; }
bool MySqlRow::isInlined() const { return false; }

// AugmentedRow.
const mysqlx::Value &AugmentedRow::get(size_t pos) const {
  if (pos == this->aug_index_) {
    return this->aug_value_;
  } else if (pos < this->aug_index_) {
    return this->row_[pos];
  } else {
    return this->row_[pos - 1];
  }
}
bool AugmentedRow::isInlined() const { return false; }

// InlinedRow.
const mysqlx::Value &InlinedRow::get(size_t pos) const {
  return this->values_.at(pos);
}
bool InlinedRow::isInlined() const { return true; }

}  // namespace mysql
}  // namespace pelton
