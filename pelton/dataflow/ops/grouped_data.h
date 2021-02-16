//
// Created by Leonhard Spiegelberg on 1/4/21.
//

#ifndef PELTON_DATAFLOW_OPS_GROUPED_DATA_H_
#define PELTON_DATAFLOW_OPS_GROUPED_DATA_H_

#include <algorithm>
#include <cstdint>
#include <iterator>
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
  GroupedData() {}

  inline std::vector<Record>::const_iterator BeginGroup(const Key &key) const {
    if (!contents_.contains(key)) return EMPTY_VEC.cend();
    return std::cbegin(contents_.at(key));
  }
  inline std::vector<Record>::const_iterator EndGroup(const Key &key) const {
    if (!contents_.contains(key)) return EMPTY_VEC.cend();
    return std::cend(contents_.at(key));
  }

  inline const std::vector<Record> &Get(const Key &key) const {
    if (!contents_.contains(key)) return EMPTY_VEC;
    return contents_.at(key);
  }

  inline bool Contains(const Key &key) const { return contents_.contains(key); }

  inline bool Insert(const Record &r) {
    Key key = r.GetKey();
    if (!contents_.contains(key)) {
      // need to add key -> records entry as empty vector
      contents_.emplace(key, std::vector<Record>{});
    }

    std::vector<Record> &v = contents_.at(key);
    if (r.IsPositive()) {
      v.push_back(r.Copy());
    } else {
      auto it = std::find(std::begin(v), std::end(v), r);
      v.erase(it);
    }
    return true;
  }

  // Our own iterator over all the records in all the groups.
  // The iterator exposes an API as if all the data was a single flat
  // vector of records.
  // It iterates over keys one at a time, each time it retrieves a new key
  // it iterates over all its records first, and when they are all consumed,
  // it moves to the next key, until all are consumed.
  class const_iterator {
   public:
    // Constructor: takes the iterator of the groups/keys.
    const_iterator(
        absl::flat_hash_map<Key, std::vector<Record>>::const_iterator begin,
        absl::flat_hash_map<Key, std::vector<Record>>::const_iterator end)
        : group_it_(begin), group_end_(end) {
      // Unless the iterator is empty, get the first key and set up
      // internal record iterator to point to the first record of that key.
      if (this->group_it_ != this->group_end_) {
        const auto &[_, records] = *(this->group_it_);
        this->record_it_ = std::cbegin(records);
        this->record_end_ = std::cend(records);
      } else {
        this->record_it_ = EMPTY_VEC.cend();
        this->record_end_ = EMPTY_VEC.cend();
      }
    }

    // Increment: increment internal record iterator, if reach the end,
    // increment the key iterator, and update the record iterator to go over
    // records of that key.
    const_iterator &operator++() {
      this->record_it_++;
      if (this->record_it_ == this->record_end_) {
        this->group_it_++;
        if (this->group_it_ != this->group_end_) {
          const auto &[_, records] = *(this->group_it_);
          this->record_it_ = std::cbegin(records);
          this->record_end_ = std::cend(records);
        }
      }
      return *this;
    }
    const_iterator operator++(int) {
      const_iterator retval = *this;
      ++(*this);
      return retval;
    }

    // All internal iterators must be equal.
    bool operator==(const_iterator o) const {
      return this->group_it_ == o.group_it_ && this->record_it_ == o.record_it_;
    }
    bool operator!=(const_iterator o) const { return !(*this == o); }

    // Get next record.
    const Record &operator*() { return *this->record_it_; }

    // iterator traits
    using difference_type = int64_t;
    using value_type = Record;
    using pointer = const Record *;
    using reference = const Record &;
    using iterator_category = std::input_iterator_tag;

   private:
    absl::flat_hash_map<Key, std::vector<Record>>::const_iterator group_it_;
    absl::flat_hash_map<Key, std::vector<Record>>::const_iterator group_end_;
    std::vector<Record>::const_iterator record_it_;
    std::vector<Record>::const_iterator record_end_;
  };

  inline const_iterator cbegin() const {
    return const_iterator{std::cbegin(contents_), std::cend(contents_)};
  }
  inline const_iterator cend() const {
    return const_iterator{std::cend(contents_), std::cend(contents_)};
  }

 private:
  static const std::vector<Record> EMPTY_VEC;
  absl::flat_hash_map<Key, std::vector<Record>> contents_;
};

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_GROUPED_DATA_H_
