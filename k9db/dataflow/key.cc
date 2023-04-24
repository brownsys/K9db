#include "k9db/dataflow/key.h"

#include <utility>

namespace k9db {
namespace dataflow {

// Constructor reserves values according to schema.
Key::Key(size_t capacity) : values_() { this->values_.reserve(capacity); }

// Comparisons.
bool Key::operator<(const Key &other) const {
  CHECK_EQ(this->values_.size(), other.values_.size())
      << "Comparing keys of different sizes";
  for (size_t i = 0; i < this->values_.size(); i++) {
    if (this->values_.at(i) < other.values_.at(i)) {
      return true;
    } else if (this->values_.at(i) != other.values_.at(i)) {
      return false;
    }
  }
  return false;
}

// Adding values.
void Key::AddNull() {
  this->CheckSize();
  this->values_.emplace_back();
}
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
void Key::AddValue(sqlast::Value &&value) {
  this->CheckSize();
  this->values_.emplace_back(std::move(value));
}
void Key::AddValue(const sqlast::Value &value) {
  this->CheckSize();
  this->values_.push_back(value);
}

// Deterministic hashing for partitioning / mutli-threading.
size_t Key::Hash() const {
  size_t hash_value = 0;
  for (const sqlast::Value &val : this->values_) {
    hash_value += val.Hash();
  }
  return hash_value;
}

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os, const k9db::dataflow::Key &k) {
  os << "Key = |";
  for (const k9db::sqlast::Value &value : k.values_) {
    os << value << "|";
  }
  return os;
}

}  // namespace dataflow
}  // namespace k9db
