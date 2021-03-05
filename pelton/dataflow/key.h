#ifndef PELTON_DATAFLOW_KEY_H_
#define PELTON_DATAFLOW_KEY_H_

#include <string>
#include <utility>
#include <vector>

#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/dataflow/value.h"

namespace pelton {
namespace dataflow {

class Key {
 public:
  // Constructor reserves values according to schema.
  explicit Key(size_t capacity);

  // Must be copyable to be used as a key for absl maps.
  Key(const Key &o);
  Key &operator=(const Key &o);

  // Move moves contained strings.
  Key(Key &&o);
  Key &operator=(Key &&o);

  // Manually destruct string if key is a string.
  ~Key() = default;

  // Comparisons.
  bool operator==(const Key &other) const;
  bool operator!=(const Key &other) const;
  bool operator<(const Key &other) const;

  // Hash to use as a key in absl hash tables.
  template <typename H>
  friend H AbslHashValue(H h, const Key &k) {
    h = H::combine(std::move(h), k.values_.size());
    for (const Value &value : k.values_) {
      h = H::combine(std::move(h), value);
    }
    return h;
  }

  // Data access.
  const Value &value(size_t i) const { return this->values_.at(i); }
  size_t size() const { return this->values_.size(); }

  // Adding values.
  void AddValue(uint64_t v);
  void AddValue(int64_t v);
  void AddValue(const std::string &v);
  void AddValue(std::string &&v);

  // For logging and printing...
  friend std::ostream &operator<<(std::ostream &os, const Key &k);

 private:
  std::vector<Value> values_;

  inline void CheckSize() const {
    if (this->values_.size() == this->values_.capacity()) {
      LOG(FATAL) << "Key is already full";
    }
  }
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_KEY_H_
