#include "pelton/dataflow/key.h"

namespace pelton {
namespace dataflow {

// Constructor reserves values according to schema.
Key::Key(size_t capacity) : values_() { this->values_.reserve(capacity); }

// Must be copyable to be used as a key for absl maps.
Key::Key(const Key &o) { this->values_ = o.values_; }
Key &Key::operator=(const Key &o) {
  this->values_ = o.values_;
  return *this;
}

// Move moves contained strings.
Key::Key(Key &&o) { this->values_ = std::move(o.values_); }
Key &Key::operator=(Key &&o) {
  this->values_ = std::move(o.values_);
  return *this;
}

// Comparisons.
bool Key::operator==(const Key &other) const {
  return this->values_ == other.values_;
}
bool Key::operator!=(const Key &other) const { return !(*this == other); }

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

// Printing a record to an output stream (e.g. std::cout).
std::ostream &operator<<(std::ostream &os, const pelton::dataflow::Key &k) {
  os << "Key = |";
  for (const pelton::dataflow::Value &value : k.values_) {
    os << value << "|";
  }
  return os;
}

}  // namespace dataflow
}  // namespace pelton
