#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

// Helper for Record::DataVariant.
sqlast::ColumnDefinition::Type Record::TypeOfVariant(const DataVariant &v) {
  switch (v.index()) {
    case 0:
      return sqlast::ColumnDefinition::Type::TEXT;
    case 1:
      return sqlast::ColumnDefinition::Type::UINT;
    case 2:
      return sqlast::ColumnDefinition::Type::INT;
    default:
      LOG(FATAL) << "Unsupported variant type!";
  }
}

// Explicit deep copying.
Record Record::Copy() const {
  // Create a copy.
  Record record{this->schema_, this->positive_};
  record.timestamp_ = this->timestamp_;

  // Copy bitmap
  record.bitmap_ = new uint64_t[NumBits()];
  memcpy(record.bitmap_, this->bitmap_, NumBits() * sizeof(uint64_t));

  // Copy data.
  for (size_t i = 0; i < this->schema_.size(); i++) {
    const auto &type = this->schema_.column_types().at(i);
    switch (type) {
      case sqlast::ColumnDefinition::Type::UINT:
        record.data_[i].uint = this->data_[i].uint;
        break;
      case sqlast::ColumnDefinition::Type::INT:
        record.data_[i].sint = this->data_[i].sint;
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
        if (this->data_[i].str) {
          record.data_[i].str =
              std::make_unique<std::string>(*this->data_[i].str);
        }
        break;
      default:
        LOG(FATAL) << "Unsupported data type in record copy!";
    }
  }

  return record;
}

// Data access.
void Record::SetUInt(uint64_t uint, size_t i) {
  CheckType(i, sqlast::ColumnDefinition::Type::UINT);
  CHECK(!IsNull(i));
  this->data_[i].uint = uint;
}
void Record::SetInt(int64_t sint, size_t i) {
  CheckType(i, sqlast::ColumnDefinition::Type::INT);
  CHECK(!IsNull(i));
  this->data_[i].sint = sint;
}
void Record::SetString(std::unique_ptr<std::string> &&v, size_t i) {
  CheckType(i, sqlast::ColumnDefinition::Type::TEXT);
  CHECK(!IsNull(i));
  this->data_[i].str = std::move(v);
}
uint64_t Record::GetUInt(size_t i) const {
  CheckType(i, sqlast::ColumnDefinition::Type::UINT);
  CHECK(!IsNull(i));
  return this->data_[i].uint;
}
int64_t Record::GetInt(size_t i) const {
  CheckType(i, sqlast::ColumnDefinition::Type::INT);
  CHECK(!IsNull(i));
  return this->data_[i].sint;
}
const std::string &Record::GetString(size_t i) const {
  CheckType(i, sqlast::ColumnDefinition::Type::TEXT);
  CHECK(!IsNull(i));
  return *(this->data_[i].str);
}

// Data access with generic type.
Key Record::GetValue(size_t i) const {
  CHECK(!IsNull(i));
  switch (this->schema_.TypeOf(i)) {
    case sqlast::ColumnDefinition::Type::UINT:
      return Key(this->data_[i].uint);
    case sqlast::ColumnDefinition::Type::INT:
      return Key(this->data_[i].sint);
    case sqlast::ColumnDefinition::Type::TEXT:
      return Key(*this->data_[i].str);
    default:
      LOG(FATAL) << "Unsupported data type in value extraction!";
  }
}
Key Record::GetKey() const {
  CHECK_NE(this->data_, nullptr) << "Cannot get key for moved record";
  const std::vector<ColumnID> &keys = this->schema_.keys();
  CHECK_EQ(keys.size(), 1U);
  size_t key_index = keys.at(0);
  switch (this->schema_.TypeOf(key_index)) {
    case sqlast::ColumnDefinition::Type::UINT:
      return Key(this->data_[key_index].uint);
    case sqlast::ColumnDefinition::Type::INT:
      return Key(this->data_[key_index].sint);
    case sqlast::ColumnDefinition::Type::TEXT:
      if (this->data_[key_index].str) {
        return Key(*(this->data_[key_index].str));
      }
      return Key("");
    default:
      LOG(FATAL) << "Unsupported data type in key extraction!";
  }
  return Key(0ULL);
}

// Data type transformation.
void Record::SetValue(const std::string &value, size_t i) {
  CHECK_NE(this->data_, nullptr) << "Cannot set value for moved record";
  CHECK(!IsNull(i));
  switch (this->schema_.TypeOf(i)) {
    case sqlast::ColumnDefinition::Type::UINT:
      this->data_[i].uint = std::stoull(value);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      this->data_[i].sint = std::stoll(value);
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      this->data_[i].str = std::make_unique<std::string>(value);
      break;
    default:
      LOG(FATAL) << "Unsupported data type in setvalue";
  }
}

// Equality: schema must be identical (pointer wise) and all values must be
// equal.
bool Record::operator==(const Record &other) const {
  CHECK_NE(this->data_, nullptr) << "Left record == has been moved";
  CHECK_NE(other.data_, nullptr) << "Right record == has been moved";
  // Compare schemas (as pointers).
  if (this->schema_ != other.schema_) {
    return false;
  }

  // compare bitmaps
  CHECK_EQ(NumBits(), other.NumBits());
  if(0 != memcmp(bitmap_, other.bitmap_, NumBits() * sizeof(uint64_t)))
    return false;

  // Compare data (deep compare).
  for (size_t i = 0; i < this->schema_.size(); i++) {
    const auto &type = this->schema_.column_types().at(i);
    switch (type) {
      case sqlast::ColumnDefinition::Type::UINT:
        if (this->data_[i].uint != other.data_[i].uint) {
          return false;
        }
        break;
      case sqlast::ColumnDefinition::Type::INT:
        if (this->data_[i].sint != other.data_[i].sint) {
          return false;
        }
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
        // If the pointers are not literally identical pointers.
        if (this->data_[i].str != other.data_[i].str) {
          // Either is null but not both.
          if (!this->data_[i].str || !other.data_[i].str) {
            return false;
          }
          // Compare contents.
          if (*this->data_[i].str != *other.data_[i].str) {
            return false;
          }
        }
        break;
      default:
        LOG(FATAL) << "Unsupported data type in record ==";
        return false;
    }
  }
  return true;
}

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os, const pelton::dataflow::Record &r) {
  CHECK_NE(r.data_, nullptr) << "Cannot << moved record";
  os << "|";
  for (unsigned i = 0; i < r.schema_.size(); ++i) {
    if(r.IsNull(i)) {
      os << "NULL" << "|";
      continue;
    }

    switch (r.schema_.TypeOf(i)) {
      case sqlast::ColumnDefinition::Type::UINT:
        os << r.data_[i].uint << "|";
        break;
      case sqlast::ColumnDefinition::Type::INT:
        os << r.data_[i].sint << "|";
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
        if (r.data_[i].str) {
          os << *r.data_[i].str << "|";
        } else {
          os << "[nullptr]"
             << "|";
        }
        break;
      default:
        LOG(FATAL) << "Unsupported data type in record << operator";
    }
  }
  return os;
}

}  // namespace dataflow
}  // namespace pelton
