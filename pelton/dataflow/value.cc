#include "pelton/dataflow/value.h"

namespace pelton {
namespace dataflow {

// Copy underlying string.
Value::Value(const Value &o) : type_(o.type_), str_() {
  switch (this->type_) {
    case sqlast::ColumnDefinition::Type::UINT:
      this->uint_ = o.uint_;
      break;
    case sqlast::ColumnDefinition::Type::INT:
      this->sint_ = o.sint_;
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      this->str_ = o.str_;
      break;
    default:
      LOG(FATAL) << "Unsupported data type in value copy!";
  }
}
Value &Value::operator=(const Value &o) {
  CHECK(this->type_ == o.type_) << "Bad copy assign value type";
  // Copy stuff.
  switch (this->type_) {
    case sqlast::ColumnDefinition::Type::UINT:
      this->uint_ = o.uint_;
      break;
    case sqlast::ColumnDefinition::Type::INT:
      this->sint_ = o.sint_;
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      this->str_ = o.str_;  // Frees existing string and copies target.
      break;
    default:
      LOG(FATAL) << "Unsupported data type in value copy!";
  }
  return *this;
}

// Move moves the string.
Value::Value(Value &&o) : type_(o.type_), str_() {
  switch (this->type_) {
    case sqlast::ColumnDefinition::Type::UINT:
      this->uint_ = o.uint_;
      break;
    case sqlast::ColumnDefinition::Type::INT:
      this->sint_ = o.sint_;
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      this->str_ = std::move(o.str_);
      break;
    default:
      LOG(FATAL) << "Unsupported data type in value copy!";
  }
}
Value &Value::operator=(Value &&o) {
  CHECK(this->type_ == o.type_) << "Bad move assign value type";
  // Copy stuff (but move string if o is a string).
  switch (this->type_) {
    case sqlast::ColumnDefinition::Type::UINT:
      this->uint_ = o.uint_;
      break;
    case sqlast::ColumnDefinition::Type::INT:
      this->sint_ = o.sint_;
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      // Frees current string and moves traget.
      this->str_ = std::move(o.str_);
      break;
    default:
      LOG(FATAL) << "Unsupported data type in value copy!";
  }
  return *this;
}

// Comparisons.
bool Value::operator==(const Value &other) const {
  CheckType(other.type_);
  switch (this->type_) {
    case sqlast::ColumnDefinition::Type::UINT:
      return this->uint_ == other.uint_;
    case sqlast::ColumnDefinition::Type::INT:
      return this->sint_ == other.sint_;
    case sqlast::ColumnDefinition::Type::TEXT:
      return this->str_ == other.str_;
    default:
      LOG(FATAL) << "Unsupported data type in value comparison!";
  }
}

// Data access.
uint64_t Value::GetUInt() const {
  CheckType(sqlast::ColumnDefinition::Type::UINT);
  return this->uint_;
}
int64_t Value::GetInt() const {
  CheckType(sqlast::ColumnDefinition::Type::INT);
  return this->sint_;
}
const std::string &Value::GetString() const {
  CheckType(sqlast::ColumnDefinition::Type::TEXT);
  return this->str_;
}

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os, const pelton::dataflow::Value &v) {
  switch (v.type_) {
    case sqlast::ColumnDefinition::Type::UINT:
      os << v.uint_;
      break;
    case sqlast::ColumnDefinition::Type::INT:
      os << v.sint_;
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      os << v.str_;
      break;
    default:
      LOG(FATAL) << "Unsupported data type in record << operator";
  }
  return os;
}

}  // namespace dataflow
}  // namespace pelton
