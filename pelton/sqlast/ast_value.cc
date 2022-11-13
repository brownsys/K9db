#include "pelton/sqlast/ast_value.h"

#include "glog/logging.h"

namespace pelton {
namespace sqlast {

#define _CHECK_TYPE(t)                                                   \
  if (this->data_.index() != t) {                                        \
    LOG(FATAL) << "Type mismatch: value type is " << this->data_.index() \
               << ", tried to access as " << t;                          \
  }

// Parse from SQL string.
Value Value::FromSQLString(const std::string &v) {
  CHECK(!v.empty()) << "Cannot parse empty SQL string";
  if (v == "NULL") {
    return Value();
  }
  if (v.at(0) == '\'' && v.at(v.size() - 1) == '\'') {
    CHECK(v.size() >= 2) << "Cannot parse singular quote";
    return Value(v.substr(1, v.size() - 2));
  }
  // If value is not a number, the below throws an exception.
  int64_t i = std::stoll(v.data());
  return Value(i);
}

// Comparisons.
bool Value::operator<(const Value &other) const {
  // Nothing is smaller than NULL.
  if (other.data_.index() == Type::_NULL) {
    return false;
  }
  // NULL is smaller than anything not null.
  if (this->data_.index() == Type::_NULL) {
    return true;
  }
  _CHECK_TYPE(other.data_.index())
  switch (this->data_.index()) {
    case Type::UINT:
      return std::get<uint64_t>(this->data_) < std::get<uint64_t>(other.data_);
    case Type::INT:
      return std::get<int64_t>(this->data_) < std::get<int64_t>(other.data_);
    case Type::TEXT: {
      const std::string &tstr = std::get<std::string>(this->data_);
      const std::string &ostr = std::get<std::string>(other.data_);
      if (tstr.size() != ostr.size()) {
        return tstr.size() < ostr.size();
      }
      for (size_t i = 0; i < tstr.size(); i++) {
        if (tstr.at(i) < ostr.at(i)) {
          return true;
        } else if (tstr.at(i) != ostr.at(i)) {
          return false;
        }
      }
      return false;
    }
    default:
      LOG(FATAL) << "Unsupported data type in value comparison!";
  }
}

// Data access.
uint64_t Value::GetUInt() const {
  _CHECK_TYPE(Type::UINT)
  return std::get<uint64_t>(this->data_);
}
int64_t Value::GetInt() const {
  _CHECK_TYPE(Type::INT)
  return std::get<int64_t>(this->data_);
}
const std::string &Value::GetString() const {
  _CHECK_TYPE(Type::TEXT)
  return std::get<std::string>(this->data_);
}

std::string Value::AsUnquotedString() const {
  switch (this->data_.index()) {
    case Type::_NULL:
      LOG(FATAL) << "Cannot unquote NULL value";
    case Type::UINT:
      return std::to_string(std::get<uint64_t>(this->data_));
    case Type::INT:
      return std::to_string(std::get<int64_t>(this->data_));
    case Type::TEXT:
      return std::get<std::string>(this->data_);
    default:
      LOG(FATAL) << "unimplemented unquoted for Value";
  }
}
std::string Value::AsSQLString() const {
  switch (this->data_.index()) {
    case Type::_NULL:
      return "NULL";
    case Type::UINT:
      return std::to_string(std::get<uint64_t>(this->data_));
    case Type::INT:
      return std::to_string(std::get<int64_t>(this->data_));
    case Type::TEXT:
      return "'" + std::get<std::string>(this->data_) + "'";
    default:
      LOG(FATAL) << "unimplemented sql escape for Value";
  }
}

// Type conversions.
bool Value::TypeCompatible(ColumnDefinitionTypeEnum type) const {
  switch (this->type()) {
    case Type::_NULL:
      return true;
    case Type::UINT:
      return type == ColumnDefinitionTypeEnum::UINT;
    case Type::INT:
      return type == ColumnDefinitionTypeEnum::INT ||
             type == ColumnDefinitionTypeEnum::UINT;
    case Type::TEXT:
      return type == ColumnDefinitionTypeEnum::TEXT ||
             type == ColumnDefinitionTypeEnum::DATETIME;
    default:
      LOG(FATAL) << "Unsupported type in TypeCompatible";
  }
}
ColumnDefinitionTypeEnum Value::ConvertType() const {
  switch (this->type()) {
    case sqlast::Value::_NULL:
      LOG(FATAL) << "Cannot convert NULL value type to column definition type";
    case sqlast::Value::UINT:
      return ColumnDefinitionTypeEnum::UINT;
    case sqlast::Value::INT:
      return ColumnDefinitionTypeEnum::INT;
    case sqlast::Value::TEXT:
      return ColumnDefinitionTypeEnum::TEXT;
    default:
      LOG(FATAL) << "Cannot convert value type to column definition type";
  }
}

void Value::ConvertTo(ColumnDefinitionTypeEnum target) {
  switch (this->type()) {
    case Type::_NULL:
      return;
    case Type::UINT: {
      if (target != ColumnDefinitionTypeEnum::UINT) {
        LOG(FATAL) << "Parsed type incompatible with schema type";
      }
      return;
    }
    case Type::INT: {
      if (target == ColumnDefinitionTypeEnum::UINT) {
        int64_t v = std::get<int64_t>(this->data_);
        CHECK_GE(v, 0) << "Parsed value is negative, expected unsigned";
        this->data_ = static_cast<uint64_t>(v);
      } else if (target != ColumnDefinitionTypeEnum::INT) {
        LOG(FATAL) << "Parsed type incompatible with schema type";
      }
      return;
    }
    case Type::TEXT:
      if (target != ColumnDefinitionTypeEnum::TEXT &&
          target != ColumnDefinitionTypeEnum::DATETIME) {
        LOG(FATAL) << "Parsed type incompatible with schema type";
      }
      return;
    default:
      LOG(FATAL) << "Unsupported type in ConvertTo";
  }
}

// The size in memory.
size_t Value::SizeInMemory() const {
  switch (this->data_.index()) {
    case Type::_NULL:
    case Type::UINT:
    case Type::INT:
      return sizeof(uint64_t);
    case Type::TEXT:
      return std::get<std::string>(this->data_).size();
    default:
      LOG(FATAL) << "Unsupported data type in Value.SizeInMemory()";
  }
}

// Deterministic hash.
size_t Value::Hash() const {
  switch (this->data_.index()) {
    case Type::_NULL:
      return std::hash<std::string>{}("[NULL]");
    case Type::UINT:
      return std::hash<std::uint64_t>{}(std::get<uint64_t>(this->data_));
    case Type::INT:
      return std::hash<std::int64_t>{}(std::get<int64_t>(this->data_));
    case Type::TEXT:
      return std::hash<std::string>{}(std::get<std::string>(this->data_));
    default:
      LOG(FATAL) << "Unsupported data type in Hash()";
  }
}

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os, const pelton::sqlast::Value &v) {
  switch (v.data_.index()) {
    case pelton::sqlast::Value::Type::_NULL:
      os << "[NULL]";
      break;
    case pelton::sqlast::Value::Type::UINT:
      os << std::get<uint64_t>(v.data_);
      break;
    case pelton::sqlast::Value::Type::INT:
      os << std::get<int64_t>(v.data_);
      break;
    case pelton::sqlast::Value::Type::TEXT:
      os << std::get<std::string>(v.data_);
      break;
    default:
      LOG(FATAL) << "Unsupported data type in Value << operator";
  }
  return os;
}

}  // namespace sqlast
}  // namespace pelton
