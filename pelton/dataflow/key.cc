#include "pelton/dataflow/key.h"

#include <utility>

namespace pelton {
namespace dataflow {

// Constructor reserves values according to schema.
Key::Key(size_t capacity) : values_() { this->values_.reserve(capacity); }

// Must be copyable to be used as a key for absl maps.
Key::Key(const Key &o) {
  this->values_ = o.values_;
  this->values_.reserve(o.values_.capacity());
}
Key &Key::operator=(const Key &o) {
  this->values_ = o.values_;
  this->values_.reserve(o.values_.capacity());
  return *this;
}

// Move moves contained strings.
Key::Key(Key &&o) { this->values_ = std::move(o.values_); }
Key &Key::operator=(Key &&o) {
  this->values_ = std::move(o.values_);
  this->values_.reserve(o.values_.capacity());
  return *this;
}

// Comparisons.
bool Key::operator==(const Key &other) const {
  return this->values_ == other.values_;
}
bool Key::operator!=(const Key &other) const { return !(*this == other); }
bool Key::operator<(const Key &other) const {
  CHECK_EQ(this->values_.size(), other.values_.size())
      << "Comparing keys of different sizes";
  for (size_t i = 0; i < this->values_.size(); i++) {
    if (this->values_.at(i) < other.values_.at(i)) {
      return true;
    } else if (this->values_.at(i) != other.values_.at(i)) {
      break;
    }
  }
  return false;
}

// Adding values.
void Key::AddValue(uint64_t v) {
  this->CheckSize();
  this->values_.emplace_back(v);
}
void Key::AddValue(int64_t v) {
  this->CheckSize();
  this->values_.emplace_back(v);
}
void Key::AddValue(const std::string &v) {
  this->CheckSize();
  this->values_.emplace_back(v);
}
void Key::AddValue(std::string &&v) {
  this->CheckSize();
  this->values_.emplace_back(std::move(v));
}
void Key::AddNull(sqlast::ColumnDefinition::Type type) {
  this->CheckSize();
  this->values_.emplace_back(type);
}
void Key::AddValue(Value &&value) {
  this->CheckSize();
  this->values_.emplace_back(std::move(value));
}

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os, const pelton::dataflow::Key &k) {
  os << "Key = |";
  for (const pelton::dataflow::Value &value : k.values_) {
    os << value << "|";
  }
  return os;
}

// Deterministic hashing for partitioning / mutli-threading.
size_t Key::Hash() const {
  size_t hash_value = 0;
  for (const Value &val : this->values_) {
    if (val.IsNull()) {
      hash_value += std::hash<std::string>{}("NULL");
      continue;
    }
    switch (val.type()) {
      case sqlast::ColumnDefinition::Type::UINT:
        hash_value += std::hash<std::uint64_t>{}(val.GetUInt());
        break;
      case sqlast::ColumnDefinition::Type::INT:
        hash_value += std::hash<std::int64_t>{}(val.GetInt());
        break;
      case sqlast::ColumnDefinition::Type::TEXT:
      case sqlast::ColumnDefinition::Type::DATETIME:
        hash_value += std::hash<std::string>{}(val.GetString());
        break;
      default:
        LOG(FATAL) << "Unsupported data type when computing hash of record!";
    }
  }
  return hash_value;
}

}  // namespace dataflow
}  // namespace pelton
