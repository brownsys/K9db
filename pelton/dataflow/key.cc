#include "pelton/dataflow/key.h"

namespace pelton {
namespace dataflow {

// Must be copyable to be used as a key for absl maps.
Key::Key(const Key &o) : type_(o.type_), str_() {
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
      LOG(FATAL) << "Unsupported data type in key copy!";
  }
}
Key &Key::operator=(const Key &o) {
  CHECK(this->type_ == o.type_) << "Bad copy assign key type";
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
      LOG(FATAL) << "Unsupported data type in key copy!";
  }
  return *this;
}

// Move moves the string.
Key::Key(Key &&o) : type_(o.type_), str_() {
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
      LOG(FATAL) << "Unsupported data type in key copy!";
  }
}
Key &Key::operator=(Key &&o) {
  CHECK(this->type_ == o.type_) << "Bad move assign key type";
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
      LOG(FATAL) << "Unsupported data type in key copy!";
  }
  return *this;
}

// Comparisons.
bool Key::operator==(const Key &other) const {
  CheckType(other.type_);
  switch (this->type_) {
    case sqlast::ColumnDefinition::Type::UINT:
      return this->uint_ == other.uint_;
    case sqlast::ColumnDefinition::Type::INT:
      return this->sint_ == other.sint_;
    case sqlast::ColumnDefinition::Type::TEXT:
      return this->str_ == other.str_;
    default:
      LOG(FATAL) << "Unsupported data type in key comparison!";
  }
}

// Data access.
uint64_t Key::GetUInt() const {
  CheckType(sqlast::ColumnDefinition::Type::UINT);
  return this->uint_;
}
int64_t Key::GetInt() const {
  CheckType(sqlast::ColumnDefinition::Type::INT);
  return this->sint_;
}
const std::string &Key::GetString() const {
  CheckType(sqlast::ColumnDefinition::Type::TEXT);
  return this->str_;
}

}  // namespace dataflow
}  // namespace pelton
