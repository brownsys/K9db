//
// Created by Leonhard Spiegelberg on 1/4/21.
//

#ifndef PELTON_DATAFLOW_GROUPED_DATA_H_
#define PELTON_DATAFLOW_GROUPED_DATA_H_

#include <vector>

#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/operator.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

// class to handle multiple records
// being grouped after a key
class GroupedData {
 public:
  inline std::vector<Record>::iterator beginGroup(const Key& key) {
    if (!contents_.contains(key)) return std::vector<Record>{}.end();
    return std::begin(contents_.at(key));
  }
  inline std::vector<Record>::iterator endGroup(const Key& key) {
    if (!contents_.contains(key)) return std::vector<Record>{}.end();
    return std::end(contents_.at(key));
  }

  inline std::vector<Record> group(const Key& key) const {
    if (!contents_.contains(key)) return std::vector<Record>{};
    return contents_.at(key);
  }

  inline bool contains(const Key& key) { return contents_.contains(key); }

  inline bool insert(const Key& key, const Record& r) {
    if (!contents_.contains(key)) {
      // need to add key -> records entry as empty vector
      std::vector<Record> v;
      contents_.emplace(key, v);
    }
    std::vector<Record>& v = contents_[key];
    if (r.positive()) {
      v.push_back(r);
    } else {
      auto it = std::find(std::begin(v), std::end(v), r);
      v.erase(it);
    }
    return true;
  }

  inline std::vector<Record> contents() const {
    std::vector<Record> out;
    for (const auto& [_, r] : this->contents_) {
      out.insert(out.end(), r.cbegin(), r.cend());
    }
    return out;
  }

 private:
  absl::flat_hash_map<Key, std::vector<Record>> contents_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_GROUPED_DATA_H_
