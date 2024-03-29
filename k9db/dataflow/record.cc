#include "k9db/dataflow/record.h"

#include <algorithm>
#include <functional>
#include <string>

namespace k9db {
namespace dataflow {

// Explicit deep copying.
Record Record::Copy() const {
  // Create a copy.
  Record record{this->schema_, this->positive_};
  record.timestamp_ = this->timestamp_;

  // Copy bitmap
  record.bitmap_ = nullptr;
  if (bitmap_) {
    record.bitmap_ = new uint64_t[NumBits()];
    memcpy(record.bitmap_, this->bitmap_, NumBits() * sizeof(uint64_t));
  }

  // Copy data.
  for (size_t i = 0; i < this->schema_.size(); i++) {
    if (!this->IsNull(i)) {
      const auto &type = this->schema_.column_types().at(i);
      switch (type) {
        case sqlast::ColumnDefinition::Type::UINT:
          record.data_[i].uint = this->data_[i].uint;
          break;
        case sqlast::ColumnDefinition::Type::INT:
          record.data_[i].sint = this->data_[i].sint;
          break;
        case sqlast::ColumnDefinition::Type::TEXT:
        // TODO(malte): DATETIME should not be stored as a string,
        // see below
        case sqlast::ColumnDefinition::Type::DATETIME:
          record.data_[i].str =
              std::make_unique<std::string>(*this->data_[i].str);
          break;
        default:
          LOG(FATAL) << "Unsupported data type " << type << " in record copy!";
      }
    }
  }

  return record;
}

// Size in memory.
size_t Record::SizeInMemory() const {
  size_t size = sizeof(Record);
  for (size_t i = 0; i < this->schema_.size(); i++) {
    if (!this->IsNull(i)) {
      switch (this->schema_.TypeOf(i)) {
        case sqlast::ColumnDefinition::Type::TEXT:
        case sqlast::ColumnDefinition::Type::DATETIME:
          if (this->data_[i].str != nullptr) {
            size += this->data_[i].str->size();
          }
          break;
        case sqlast::ColumnDefinition::Type::UINT:
        case sqlast::ColumnDefinition::Type::INT:
          size += sizeof(uint64_t);
          break;
      }
    }
  }
  return size;
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
void Record::SetDateTime(std::unique_ptr<std::string> &&v, size_t i) {
  CheckType(i, sqlast::ColumnDefinition::Type::DATETIME);
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
  CHECK_NOTNULL(this->data_[i].str);
  return *(this->data_[i].str);
}
const std::string &Record::GetDateTime(size_t i) const {
  CheckType(i, sqlast::ColumnDefinition::Type::DATETIME);
  CHECK(!IsNull(i));
  CHECK_NOTNULL(this->data_[i].str);
  return *(this->data_[i].str);
}

// Data access with generic type.
Key Record::GetKey() const {
  CHECK_NE(this->data_, nullptr) << "Cannot get key for moved record";
  return this->GetValues(this->schema_.keys());
}
Key Record::GetValues(const std::vector<ColumnID> &cols) const {
  // Construct key with given capacity, and fill it up with values.
  Key key{cols.size()};
  for (ColumnID col : cols) {
    if (this->IsNull(col)) {
      key.AddNull();
      continue;
    }
    switch (this->schema_.TypeOf(col)) {
      case sqlast::ColumnDefinition::Type::UINT:
        key.AddValue(this->data_[col].uint);
        break;
      case sqlast::ColumnDefinition::Type::INT:
        key.AddValue(this->data_[col].sint);
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
      case sqlast::ColumnDefinition::Type::DATETIME:
        if (this->data_[col].str) {
          key.AddValue(*this->data_[col].str);
        } else {
          key.AddValue("");
        }
        break;
      default:
        LOG(FATAL) << "Unsupported data type in key extraction!";
    }
  }
  return key;
}
sqlast::Value Record::GetValue(ColumnID col) const {
  CHECK_NE(this->data_, nullptr) << "Cannot get value for moved record";
  if (this->IsNull(col)) {
    return sqlast::Value();
  }
  switch (this->schema_.TypeOf(col)) {
    case sqlast::ColumnDefinition::Type::UINT:
      return sqlast::Value(this->data_[col].uint);
    case sqlast::ColumnDefinition::Type::INT:
      return sqlast::Value(this->data_[col].sint);
    case sqlast::ColumnDefinition::Type::TEXT:
    case sqlast::ColumnDefinition::Type::DATETIME:
      return sqlast::Value(*this->data_[col].str);
    default:
      LOG(FATAL) << "Unsupported data type in value extraction!";
  }
}

// Data type transformation.
void Record::SetValue(const sqlast::Value &value, size_t i) {
  CHECK_NOTNULL(this->data_);
  if (value.IsNull()) {
    this->SetNull(true, i);
    return;
  }
  this->SetNull(false, i);
  switch (this->schema_.TypeOf(i)) {
    case sqlast::ColumnDefinition::Type::UINT:
      this->SetUInt(value.GetUInt(), i);
      break;
    case sqlast::ColumnDefinition::Type::INT:
      this->SetInt(value.GetInt(), i);
      break;
    case sqlast::ColumnDefinition::Type::TEXT:
      this->SetString(std::make_unique<std::string>(value.GetString()), i);
      break;
    case sqlast::ColumnDefinition::Type::DATETIME:
      this->SetDateTime(std::make_unique<std::string>(value.GetString()), i);
      break;
  }
}

// Deterministic hashing for partitioning / mutli-threading.
size_t Record::Hash(const std::vector<ColumnID> &cols) const {
  size_t hash_value = 0;
  for (ColumnID col : cols) {
    if (this->IsNull(col)) {
      continue;
    }
    switch (this->schema_.TypeOf(col)) {
      case sqlast::ColumnDefinition::Type::UINT:
        hash_value += std::hash<std::uint64_t>{}(this->data_[col].uint);
        break;
      case sqlast::ColumnDefinition::Type::INT:
        hash_value += std::hash<std::int64_t>{}(this->data_[col].sint);
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
      case sqlast::ColumnDefinition::Type::DATETIME:
        hash_value += std::hash<std::string>{}(*this->data_[col].str);
        break;
      default:
        LOG(FATAL) << "Unsupported data type when computing hash of record!";
    }
  }
  return hash_value;
}

// Create a new record resulting from applying the update to this record.
// Updating the sequence given an update statement and schema.
Record Record::Update(const UpdateMap &updates) const {
  Record updated{this->schema_, true};
  for (size_t i = 0; i < this->schema_.size(); i++) {
    const std::string &column_name = this->schema_.NameOf(i);
    auto it = updates.find(column_name);
    if (it == updates.end()) {
      updated.SetValue(this->GetValue(i), i);
    } else {
      std::function<sqlast::Value(const sqlast::Expression *)> evaluate =
          [&](const sqlast::Expression *expr) {
            switch (expr->type()) {
              case sqlast::Expression::Type::LITERAL: {
                auto l = static_cast<const sqlast::LiteralExpression *>(expr);
                return l->value();
              }
              case sqlast::Expression::Type::COLUMN: {
                auto c = static_cast<const sqlast::ColumnExpression *>(expr);
                int idx = this->schema_.IndexOf(c->column());
                CHECK_GE(idx, 0);
                return this->GetValue(idx);
              }
              case sqlast::Expression::Type::PLUS: {
                const sqlast::BinaryExpression *binexpr =
                    static_cast<const sqlast::BinaryExpression *>(expr);
                sqlast::Value left = evaluate(binexpr->GetLeft());
                sqlast::Value right = evaluate(binexpr->GetRight());
                CHECK(left.type() == sqlast::Value::Type::INT);
                CHECK(right.type() == sqlast::Value::Type::INT);
                return sqlast::Value(left.GetInt() + right.GetInt());
              }
              case sqlast::Expression::Type::MINUS: {
                const sqlast::BinaryExpression *binexpr =
                    static_cast<const sqlast::BinaryExpression *>(expr);
                sqlast::Value left = evaluate(binexpr->GetLeft());
                sqlast::Value right = evaluate(binexpr->GetRight());
                CHECK(left.type() == sqlast::Value::Type::INT);
                CHECK(right.type() == sqlast::Value::Type::INT);
                return sqlast::Value(left.GetInt() - right.GetInt());
              }
              default:
                LOG(FATAL) << "Unsupported update expression";
            }
          };
      sqlast::Value value = evaluate(it->second);
      CHECK(value.TypeCompatible(this->schema_.TypeOf(i)));
      updated.SetValue(value, i);
    }
  }
  return updated;
}

// Equality: schema must be identical (pointer wise) and all values must be
// equal.
bool Record::operator==(const Record &other) const {
  CHECK_NOTNULL(this->data_);
  CHECK_NOTNULL(other.data_);
  // Compare schemas (as pointers).
  if (this->schema_ != other.schema_) {
    return false;
  }

  // compare bitmaps
  CHECK_EQ(NumBits(), other.NumBits());

  // check whether bitmaps are different
  if ((!bitmap_ && other.bitmap_) || (bitmap_ && !other.bitmap_)) return false;
  if (bitmap_) {
    CHECK(other.bitmap_);
    if (0 != memcmp(bitmap_, other.bitmap_, NumBits() * sizeof(uint64_t)))
      return false;
  }

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
      case sqlast::ColumnDefinition::Type::DATETIME:
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
std::ostream &operator<<(std::ostream &os, const k9db::dataflow::Record &r) {
  CHECK_NOTNULL(r.data_);
  os << "|";
  for (unsigned i = 0; i < r.schema_.size(); ++i) {
    if (r.IsNull(i)) {
      os << "NULL"
         << "|";
      continue;
    }

    switch (r.schema_.TypeOf(i)) {
      case sqlast::ColumnDefinition::Type::UINT:
        os << r.data_[i].uint << "|";
        break;
      case sqlast::ColumnDefinition::Type::INT:
        os << r.data_[i].sint << "|";
        break;
      case sqlast::ColumnDefinition::Type::DATETIME:
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

  if (!r.IsPositive()) {
    os << " -- negative";
  }

  return os;
}

}  // namespace dataflow
}  // namespace k9db
