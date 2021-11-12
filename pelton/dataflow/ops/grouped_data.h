//
// Created by Leonhard Spiegelberg on 1/4/21.
//

#ifndef PELTON_DATAFLOW_OPS_GROUPED_DATA_H_
#define PELTON_DATAFLOW_OPS_GROUPED_DATA_H_

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <list>
#include <set>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/util/merge_sort.h"
#include "pelton/util/perf.h"

namespace pelton {
namespace dataflow {

// This class will either have a RecordCompare member variable or not depending
// on whether T == std::nullptr_t.
template <typename T, typename Enable = void>
class GroupedData {
 protected:
  GroupedData() {}
};

template <typename T>
class GroupedData<T, std::enable_if_t<!std::is_null_pointer<T>::value>> {
 public:
  const T &compare() const { return this->compare_; }

 protected:
  GroupedData() = delete;
  explicit GroupedData(T compare) : compare_(compare) {}
  T compare_;
};

// This class stores a set of records, this set is divided into groups,
// each mapped by a key. This class is used as the underlying storage for
// stateful operators. Most notably, MatviewOperator.
//
// The key used in this class is not necessarily the same as the PK key of the
// records stored. Instead, it is externally specified by the operators and
// planner. The key corresponds to the values that determine which records
// to lookup when a concrete query must be looked up by the host application.
// This key is deduced by the planner from the underlying query/flow.
//
// For example, the following query:
//   SELECT id, department, course_name FROM courses WHERE department = ?
// yields an instance of this class where records are keyed by department, even
// though the primary key of records from the courses tables is id.
// This way, when a specific concrete query (say one looking up the CS
// department) needs to be looked up, this can be done efficiently.
//
// In cases where a query does not have such ? arguments (i.e. it is fully known
// at flow construction time), the planner yields a materialized view without
// any key. Thus, the corresponding instance of this class consists of a single
// group, stored as a single entry in the underling map type.
//
// The records inside this data are stored in a two-layer/nested container
// of the shape: MapType<Key, VectorType<Record>>.
// The class is generic over these two container types, which are specified by
// template variables M and V respectively.
//
// This class provide three ways of accessing the underyling data. It is easiest
// to understand them when the key is a single column, but the logic extends
// to having composite keys.
// 1. Lookup by key via #Lookup(key): these return a vector of records
//    corresponding to the given key in whatever underlying order they are
//    stored in. The vector owns these records (they are copies).
//    These also have a version with optional arguments for an offset and limit.
// 2. Complete iteration via #All(): this returns a vector of all the records in
//    all the keys with the underlying order of keys and/or records.
// 3. Key iteration via #Keys(): returns a vector of keys in the underlying
//    order.
// 4. Insert(Key, record): inserts a copy of record into the group of the given
//    key. If the record is negative, this corresponds to deleting the record.
//
// We provide three concrete instantiations of this generic type at the end of
// this file:
//
// 1. GroupedDataT<absl::flat_hash_map, std::list>:
//    A completely unsorted set, used for storing views of completely unordered
//    queries. For example: SELECT * FROM <table> WHERE <col> = ?.
//    A key in such a case is a value from <col>, and the associated vector
//    contains all records that has that value from <col>.
//    Both #Lookup(key), #All(), and #Keys() give us the data in some arbitrary
//    order.
//
// 2. GroupedDataT<absl::flat_hash_map, absl::btree_set>:
//    A set of sorted records, where the records are sorted by a different set
//    of column(s) than what they are keyed by.
//    For example: SELECT * FROM <table> WHERE <col1> = ? ORDER BY <col2>.
//    #Lookup(key) gives us the records ordered by <col2> (local to one value of
//    <col1>), while #Keys() gives us the available values of <col1> in some
//    arbitrary order. #All() gives us all the records ordered by <col2>
//    regardless of <col1>.
//
// 3. GroupedDataT<absl::btree_map, std::list>:
//    This is a set of unsorted records grouped by sorted keys. This is used
//    when the key column set and sorting column set are identical.
//    For example: SELECT * FROM <table> WHERE <col> = ? ORDER BY <col>.
//    The output of such a query is ordered by <col>, but records that have the
//    same value for <col> have no particular order.
//    In this case, #Lookup(key) API gives us the records belonging to that key
//    in some arbitrary order (order of arrival). However, #Keys() gives us the
//    keys in order. #All() gives us all the record in a global order by <col>.
//    Finally, this instantiation provides an additional function,
//    #LookupGreater(key), which provides a vector of all records whose key
//    value is greater than key, ordered by their value of key.
//
// Performance:
// All reads are in O(n), where n is the number of records in the
// underlying group (for Lookup(key)) or in the overall structure (for All()).
// Modifications are more complicated:
// A) For inserts
//    1) Unordered: in O(1).
//    2) key ordered: in O(log(|keys|))
//    3) record ordered: in O(log(|records in key|)).
// B) For deletes
//    1) Unordered: in O(1) if PK is part of schema, or
//                  in O(|records in key|) otherwise.
//    2) key ordered: in O(log(|keys|) if PK is part of schema, or
//                    in O(log(|keys|) + |records in key|) otherwise.
//    3) record ordered: in O(1) if PK is part of schema, or
//                    in O(log(|records in key|)) otherwise.
template <typename M, typename V, typename RecordCompare = std::nullptr_t,
          typename R = Record>
class GroupedDataT : public GroupedData<RecordCompare> {
 public:
  using NoCompare = std::is_null_pointer<RecordCompare>;
  using KeyOrdered = std::is_same<M, absl::btree_map<Key, std::list<Record>>>;

  // If this is not ordered, we do not need to provide anything.
  template <typename X = void,
            typename std::enable_if<NoCompare::value, X>::type * = nullptr>
  GroupedDataT() : count_(0), contents_(), schema_has_pk_(false), pk_index_() {}

  // If this is ordered, we need to provide a custom comparator.
  template <typename X = void,
            typename std::enable_if<!NoCompare::value, X>::type * = nullptr>
  explicit GroupedDataT(const RecordCompare &compare)
      : GroupedData<RecordCompare>(compare),
        count_(0),
        contents_(),
        schema_has_pk_(false),
        pk_index_() {}

  // Initialize with the schema to be used.
  void Initialize(const SchemaRef &schema) {
    this->schema_has_pk_ = schema.keys().size() > 0;
  }
  void Initialize(bool use_pk) { this->schema_has_pk_ = false; }

  // Generic API:
  // Count of elements in the entire map.
  size_t count() const { return this->count_; }
  // Total size consumed in memory.
  uint64_t SizeInMemory() const {
    uint64_t size = 0;
    for (const Key &key : this->Keys()) {
      for (const auto &record : this->Lookup(key)) {
        size += record.SizeInMemory();
      }
    }
    return size;
  }

  // Lookup(key) API:
  // Return an Iterable set of records corresponding to the given key, in the
  // underlying order (specified by V).
  std::vector<R> Lookup(const Key &key, int limit = -1,
                        size_t offset = 0) const {
    auto it = this->contents_.find(key);
    if (it == this->contents_.end()) {
      return {};
    }
    if (static_cast<size_t>(it->second.size()) <= offset) {
      return {};
    }

    // Get records between [offset, offset + limit).
    auto begin = it->second.begin();
    auto end = it->second.end();
    std::advance(begin, offset);

    // Copy records.
    std::vector<R> result;
    result.reserve(limit > -1 ? limit : it->second.size() - offset);
    for (auto it = begin; it != end; ++it) {
      if (limit > -1 && result.size() >= static_cast<size_t>(limit)) {
        break;
      }
      if constexpr (std::is_same<R, Record>::value) {
        result.push_back(it->Copy());
      } else {
        // Use copy constructor.
        result.push_back(*it);
      }
    }
    return result;
  }
  // Looking up within a key, for records > than cmp.
  template <typename X = void,
            typename std::enable_if<!NoCompare::value, X>::type * = nullptr>
  std::vector<R> LookupGreater(const Key &key, const R &cmp, int limit = -1,
                               size_t offset = 0) const {
    auto it = this->contents_.find(key);
    if (it == this->contents_.end()) {
      return {};
    }
    if (static_cast<size_t>(it->second.size()) <= offset) {
      return {};
    }

    // Get records greater than cmp.
    auto begin = it->second.lower_bound(cmp);
    auto end = it->second.end();
    while (begin != end && offset > 0) {
      ++begin;
      --offset;
    }

    // Copy records.
    std::vector<R> result;
    for (auto it = begin; it != end; ++it) {
      if (limit > -1 && result.size() >= static_cast<size_t>(limit)) {
        break;
      }
      if constexpr (std::is_same<R, Record>::value) {
        result.push_back(it->Copy());
      } else {
        // Use copy constructor.
        result.push_back(*it);
      }
    }
    return result;
  }
  // Looking up by > key, only available for key ordered instantiations.
  // Due to hash partitioning, having an offset here is entirely meaningless.
  template <typename X = void,
            typename std::enable_if<KeyOrdered::value, X>::type * = nullptr>
  std::vector<R> LookupGreater(const Key &key, int limit = -1) const {
    // Find smallest k > key.
    auto begin = this->contents_.lower_bound(key);
    auto end = this->contents_.end();
    if (begin == end) {
      return {};
    }

    std::vector<R> result;
    for (auto it = begin; it != end; ++it) {
      auto begin = it->second.begin();
      auto end = it->second.end();
      for (auto it = begin; it != end; ++it) {
        if (limit > -1 && result.size() >= static_cast<size_t>(limit)) {
          return result;
        }
        if constexpr (std::is_same<R, Record>::value) {
          result.push_back(it->Copy());
        } else {
          // Use copy constructor.
          result.push_back(*it);
        }
      }
    }
    return result;
  }

  // All() API:
  // If not record ordered, All() appends all buckets.
  template <typename X1 = void,
            typename std::enable_if<NoCompare::value, X1>::type * = nullptr>
  std::vector<R> All(int limit = -1) const {
    std::vector<R> result;
    result.reserve(this->count_);
    for (auto it = this->contents_.begin(); it != this->contents_.end(); ++it) {
      for (auto it2 = it->second.begin(); it2 != it->second.end(); ++it2) {
        if (limit > -1 && result.size() >= static_cast<size_t>(limit)) {
          break;
        }

        if constexpr (std::is_same<R, Record>::value) {
          result.push_back(it2->Copy());
        } else {
          // Use copy constructor.
          result.push_back(*it2);
        }
      }
    }
    return result;
  }
  // If record ordered, each bucket is ordered, and All() returns a merged
  // concatention of all buckets that is also ordered.
  template <typename X2 = void,
            typename std::enable_if<!NoCompare::value, X2>::type * = nullptr>
  std::vector<R> All(int limit = -1) const {
    std::list<R> result;
    for (auto it = this->contents_.begin(); it != this->contents_.end(); ++it) {
      util::MergeInto(&result, it->second, this->compare_, limit);
    }
    return util::ToVector(&result, limit, 0);
  }
  // If record ordered, we should be able to get All() with an ordering
  // filter on records.
  template <typename X = void,
            typename std::enable_if<!NoCompare::value, X>::type * = nullptr>
  std::vector<R> All(const R &cmp, int limit = -1) const {
    std::list<R> result;
    for (auto it = this->contents_.begin(); it != this->contents_.end(); ++it) {
      auto bucket = this->LookupGreater(it->first, cmp, limit);
      util::MergeInto(&result, bucket, this->compare_, limit);
    }
    return util::ToVector(&result, limit, 0);
  }

  // Key-based API:
  // Return an Iterable set of keys contained by this group, in the underlying
  // order (specified by M). Because of hash partitioning having an offset here
  // would be meaningless.
  std::vector<Key> Keys(int limit = -1) const {
    auto begin = this->contents_.begin();
    auto end = this->contents_.end();

    std::vector<Key> result;
    for (auto it = begin; it != end; ++it) {
      if (limit > -1 && result.size() >= static_cast<size_t>(limit)) {
        break;
      }
      result.push_back(it->first);
    }
    return result;
  }
  // Looking up by > key, only available for key ordered instantiations.
  // Because of hash partitioning, having an offset here is meaningless.
  template <typename X = void,
            typename std::enable_if<KeyOrdered::value, X>::type * = nullptr>
  std::vector<Key> KeysGreater(const Key &key, int limit = -1) const {
    auto begin = this->contents_.lower_bound(key);
    auto end = this->contents_.end();

    std::vector<Key> result;
    for (auto it = begin; it != end; ++it) {
      if (limit > -1 && result.size() >= static_cast<size_t>(limit)) {
        break;
      }
      result.push_back(it->first);
    }
    return result;
  }

  // Count of records corresponding to a given key
  size_t Count(const Key &key) const {
    auto it = this->contents_.find(key);
    if (it == this->contents_.end()) {
      return 0;
    }
    return it->second.size();
  }
  // Checks if we have any records mapped to the given key.
  bool Contains(const Key &key) const {
    return this->contents_.find(key) != this->contents_.end();
  }
  // Erase all entires keyed by key.
  bool Erase(const Key &k) {
    this->contents_.erase(k);
    return true;
  }

  // Insert() API:
  // Insert a record mapped by given key.
  bool Insert(const Key &k, R &&r) {
    V *bucket = nullptr;
    auto it = this->contents_.find(k);
    if (it == this->contents_.end()) {
      if constexpr (NoCompare::value) {
        bucket = &this->contents_[k];
      } else {
        auto emplaced = this->contents_.emplace(k, this->compare_);
        bucket = &emplaced.first->second;
      }
    } else {
      bucket = &it->second;
    }
    // Add the new record to the approprite bin.
    this->count_++;
    typename V::const_iterator insert_it;
    if constexpr (!NoCompare::value) {
      // Sorted (by record) container.
      insert_it = bucket->insert(std::move(r));
    } else {
      // Unsorted container.
      bucket->push_back(std::move(r));
      insert_it = --(bucket->end());
    }
    // Update pk index.
    if constexpr (std::is_same<R, Record>::value) {
      if (this->schema_has_pk_) {
        return this->pk_index_.emplace(insert_it->GetKey(), insert_it).second;
      }
    }
    return true;
  }
  bool Delete(const Key &k, R &&r) {
    auto it = this->contents_.find(k);
    if (it == this->contents_.end()) {
      return false;
    }
    V &bucket = it->second;

    // Find the element corresponding to r in the fastest possible way.
    bool used_pk_index = false;
    typename V::const_iterator erase_it;

    // Try to find things via the primary key index if possible.
    if constexpr (std::is_same<R, Record>::value) {
      if (this->schema_has_pk_) {
        used_pk_index = true;
        // Records have a primary key, we can use our PK index!
        Key pk = r.GetKey();
        auto pk_it = this->pk_index_.find(pk);
        if (pk_it == this->pk_index_.end()) {
          // Should never happen.
          return false;
        }
        erase_it = pk_it->second;
        this->pk_index_.erase(pk_it);  // Update pk index.
      }
    }

    // No primary key, must look up in V.
    if (!used_pk_index) {
      if constexpr (!NoCompare::value) {
        // Sorted (by record) container, log(n) search.
        erase_it = bucket.find(r);
      } else {
        // Linked list, slow linear scan.
        erase_it = std::find(std::begin(bucket), std::end(bucket), r);
      }
    }

    if (erase_it == bucket.end()) {
      // Element not found (should never happen).
      return false;
    } else {
      // Element found, let us delete it!
      this->count_--;
      bucket.erase(erase_it);
      if (bucket.size() == 0) {  // Remove bucket if empty!
        this->contents_.erase(it);
      }
      return true;
    }
  }

  // This is not for use in Matview.
  V &Get(const Key &key) { return this->contents_.at(key); }

 private:
  // Total number of records.
  size_t count_ = 0;
  // Underlying content.
  M contents_;
  // Index V by primary key for fast deletion.
  bool schema_has_pk_;
  // Index iterators into V by primary key for fast look up.
  absl::flat_hash_map<Key, typename V::const_iterator> pk_index_;
};

// Supported instantiations of GroupedData:
// 1. Unordered,
// 2. Ordered by keys, but records unordered.
// 3. Ordered by records, keys are unordered.
using UnorderedGroupedData =
    GroupedDataT<absl::flat_hash_map<Key, std::list<Record>>,
                 std::list<Record>>;
using KeyOrderedGroupedData =
    GroupedDataT<absl::btree_map<Key, std::list<Record>>, std::list<Record>>;
using RecordOrderedGroupedData = GroupedDataT<
    absl::flat_hash_map<Key, std::multiset<Record, Record::Compare>>,
    std::multiset<Record, Record::Compare>, Record::Compare>;

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_GROUPED_DATA_H_
