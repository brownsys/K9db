//
// Created by Leonhard Spiegelberg on 1/4/21.
//

#ifndef PELTON_DATAFLOW_OPS_GROUPED_DATA_H_
#define PELTON_DATAFLOW_OPS_GROUPED_DATA_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "absl/container/btree_map.h"
#include "absl/container/btree_set.h"
#include "absl/container/flat_hash_map.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/record.h"

namespace pelton {
namespace dataflow {

// A static/compile time way to test if type T has a desired member function.
// This works for inherited functions, as well as functions with variable number
// of arguments, and various modifiers of record (ref, const ref, etc).
template <class F>
struct HasFunction {
 private:
  // if p->find(*r) does not exist, this will not cause an error, rather
  // it will just cause this overloaded variant of Test not to be defined.
  // This Test(...) has return type std::true_type() only when p->find(*r)
  // exists and is defined and callable, and std::false_type otherwise.
  template <typename T>
  static auto FindTest(T *p, Record *r)
      -> decltype(p->find(*r), std::true_type());

  template <class>
  static auto FindTest(...) -> std::false_type;

  // Similar but for insert.
  template <typename T>
  static auto InsertTest(T *p, Record *r)
      -> decltype(p->insert(*r), std::true_type());

  template <class>
  static auto InsertTest(...) -> std::false_type;

 public:
  static constexpr bool Find() {
    return decltype(FindTest<F>(nullptr, nullptr))::value;
  }
  static constexpr bool Insert() {
    return decltype(InsertTest<F>(nullptr, nullptr))::value;
  }
};

// Cannot easily initialize static member of a templated class without
// doing messy template magic. Instead, we have this super untemplated class
// contain that static member.
class UntemplatedGroupedData {
 protected:
  static const std::vector<Record> EMPTY_VEC;
};

// A generic iterator that can iterate over any container producing the
// specified generic type.
// This generic iterator is merely a wrapper around the iterator of that
// underlying container. The underlying container's iterator is generically
// stored inside GenericIterator<T>::Impl, which exposes functionality to
// compare, increment, and access the iterator. GenericIterator stores a pointer
// to Impl, and defers all operations to it.
// Most of the code here is templates + inheritance to get this to work in a
// type-safe way.
template <typename T, typename C = std::true_type>
class GenericIterator {
 private:
  using vcit = typename std::vector<T>::const_iterator;
  using vit = typename std::vector<T>::iterator;
  // Abstract concept of an iterator wrapper, we store and interact with
  // pointers to this class to hid the genericity of Impl<I>.
  class AbsImpl {
   public:
    virtual std::unique_ptr<AbsImpl> Clone() const = 0;
    virtual void Increment() = 0;
    virtual bool Equals(const std::unique_ptr<AbsImpl> &o) const = 0;
    virtual typename GenericIterator<T, C>::reference Access() const = 0;
    virtual ~AbsImpl() = default;
  };  // AbsImpl.

 public:
  // Iterator traits.
  using difference_type = int64_t;
  using value_type = T;
  using iterator_category = std::input_iterator_tag;
  using pointer = typename std::conditional<C::value, const T *, T *>::type;
  using reference = typename std::conditional<C::value, const T &, T &>::type;

  // This is our wrapper around generic underlying iterators.
  template <typename I>  // I is an iterator for some container.
  class Impl : public AbsImpl {
   public:
    explicit Impl(I start_it) : it_(start_it) {}
    std::unique_ptr<AbsImpl> Clone() const override {
      return std::make_unique<Impl<I>>(this->it_);
    }
    void Increment() { this->it_++; }
    bool Equals(const std::unique_ptr<AbsImpl> &o) const {
      // Because AbsImpl is private, we know that Impl is the only possible
      // concrete class derived from it.
      return this->it_ == static_cast<Impl<I> *>(o.get())->it_;
    }
    typename GenericIterator<T, C>::reference Access() const {
      if constexpr (std::is_same<T, Key>::value) {
        return this->it_->first;
      } else {
        return *(this->it_);
      }
    }

   private:
    I it_;
  };  // Impl.

  // Empty impl for an empty iterator.
  using EmptyIterType = typename std::conditional<C::value, vcit, vit>::type;
  using EmptyImpl = Impl<EmptyIterType>;

  // Construct the generic iterator by providing a concrete implemenation.
  explicit GenericIterator(std::unique_ptr<AbsImpl> impl)
      : impl_(std::move(impl)) {}

  // All operations translate to operation on the underlying implementation.
  GenericIterator<T, C> &operator++() {
    // Increment in place.
    this->impl_->Increment();
    return *this;
  }
  GenericIterator<T, C> operator++(int n) {
    // Increment copy, leave this unchanged.
    GenericIterator copy(this->impl_->Clone());
    ++this;
    return copy;
  }
  bool operator==(const GenericIterator<T, C> &o) const {
    return this->impl_->Equals(o.impl_);
  }
  bool operator!=(const GenericIterator<T, C> &o) const {
    return !this->impl_->Equals(o.impl_);
  }
  reference operator*() const { return this->impl_->Access(); }

 private:
  std::unique_ptr<AbsImpl> impl_;
};

// Host code only sees two types of (semi-concrete) iterators: one that produces
// records and another that produces keys. The source of the iterators are still
// generic (could be a vector or map, etc).
using const_KeyIterator = GenericIterator<Key>;
using const_RecordIterator = GenericIterator<Record>;
using KeyIterator = GenericIterator<Key, std::false_type>;
using RecordIterator = GenericIterator<Record, std::false_type>;

// Represents an abstract container that can be iterated over in some generic
// way. We return instances of this class instead of the underlying iterator
// directly to make it easy for client code to iterate (e.g. using for-each
// loops).
// Note: an instance of this class can only be used once, using it (e.g. in a
// for-each loop) consumes it because of moves. Further usage of such instance
// will cause a failure.
template <typename T, typename C = std::true_type>
class GenericIterable {
 public:
  GenericIterable(GenericIterator<T, C> &&begin, GenericIterator<T, C> &&end)
      : begin_(std::move(begin)), end_(std::move(end)) {}

  GenericIterator<T, C> begin() { return std::move(this->begin_); }
  GenericIterator<T, C> end() { return std::move(this->end_); }
  bool IsEmpty() const { return this->begin_ == this->end_; }
  // Create an empty iterable.
  static GenericIterable<T, C> CreateEmpty() {
    static std::vector<T> empty = {};
    EIterType begin;
    EIterType end;
    if constexpr (C::value) {
      begin = empty.cbegin();
      end = empty.cend();
    } else {
      begin = empty.begin();
      end = empty.end();
    }
    return GenericIterable{
        GenericIterator<T, C>{std::make_unique<EImpl>(begin)},
        GenericIterator<T, C>{std::make_unique<EImpl>(end)}};
  }

 private:
  GenericIterator<T, C> begin_;
  GenericIterator<T, C> end_;
  using EIterType = typename GenericIterator<T, C>::EmptyIterType;
  using EImpl = typename GenericIterator<T, C>::EmptyImpl;
};

using const_KeyIterable = GenericIterable<Key>;
using const_RecordIterable = GenericIterable<Record>;
using KeyIterable = GenericIterable<Key, std::false_type>;
using RecordIterable = GenericIterable<Record, std::false_type>;

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
// This class provide three ways of accessing the underyling data:
// 1. Lookup by key via #Lookup(key): these return an Iterable set of records
//    corresponding to the given key in whatever underlying order they are
//    stored in.
//    These also have a version with optional arguments for an offset and limit.
// 2. Key iteration via #Keys(): this returns an Iterable set of keys for going
//    through all keys in the underlying order.
// 3. Insert(Key, record): inserts a copy of record into the group of the given
//    key.
//
// We provide three concrete instantiations of this generic type at the end of
// this file:
//
// 1. GroupedDataT<absl::flat_hash_map, std::vector>:
//    A completely unsorted set, used for storing views of completely unordered
//    queries. For example: SELECT * FROM <table> WHERE <col> = ?.
//    A key in such a case is a value from <col>, and the associated vector
//    contains all records that has that value from <col>.
//    Both #Lookup(key) and #Keys() give us the data in some arbitrary order.
//
// 2. GroupedDataT<absl::flat_hash_map, absl::btree_set>:
//    A set of sorted records, where the records are sorted by different set of
//    column(s) than what they are keyed by.
//    For example: SELECT * FROM <table> WHERE <col1> = ? ORDER BY <col2>.
//    Iterating over this class's contents produces records in order of <col>,
//    multiple records with the same value of <col> are traversed in arrival
//    order, which determines their location in the underlying vector.
//    #Lookup(key) gives us the records ordered by <col2>, while #Keys gives us
//    the available values of <col1> in some arbitrary order.
//    Note: this does not give us a global ordering over *all* the records by
//          <col2>. We only know the order of records that have the same <col1>.
//          This is intentional: records in such a view are only meant to be
//          accessed for a single specific group (i.e. value for <col1>) at a
//          time.
//    Note: when the view has no key (SELECT * FROM <table> ORDER BY <col2>),
//          then the underlying map only contains a single record set (for the
//          empty key), giving us a global order over all records.
//
// 3. GroupedDataT<absl::btree_map, std::vector>:
//    This is a set of unsorted records grouped by sorted keys. This is used
//    when the key column set and sorting column set are identical.
//    For example: SELECT * FROM <table> WHERE <col> = ? ORDER BY <col>.
//    The output of such a query is ordered by <col>, but records that have the
//    same value for <col> have no particular order.
//    In this case, #Lookup(key) API gives us the records belonging to that key
//    in some arbitrary order (order of arrival). However, #Keys() gives us the
//    keys in order.
template <typename M, typename V, typename RecordCompare = nullptr_t>
class GroupedDataT : public UntemplatedGroupedData {
 private:
  using M_iterator = typename M::iterator;
  using V_iterator = typename V::iterator;
  using M_citerator = typename M::const_iterator;
  using V_citerator = typename V::const_iterator;

  using K_impl = typename KeyIterator::template Impl<M_iterator>;
  using R_impl = typename RecordIterator::template Impl<V_iterator>;
  using K_cimpl = typename const_KeyIterator::template Impl<M_citerator>;
  using R_cimpl = typename const_RecordIterator::template Impl<V_citerator>;

 public:
  // If this is not ordered, we do not need to provide anything.
  template <typename = typename std::enable_if<
                std::is_null_pointer<RecordCompare>::value>>
  GroupedDataT() {}

  // If this is ordered, we need to provide a custom comparator.
  template <typename = typename std::enable_if<
                !std::is_null_pointer<RecordCompare>::value>>
  explicit GroupedDataT(const RecordCompare &compare) : compare_(compare) {}

  // Count of elements in the entire map.
  size_t count() const { return this->count_; }

  // Return an Iterable set of records corresponding to the given key, in the
  // underlying order (specified by V).
  const_RecordIterable Lookup(const Key &key, int limit = -1,
                              size_t offset = 0) const {
    auto it = this->contents_.find(key);
    if (it == this->contents_.end() ||
        static_cast<size_t>(it->second.size()) <= offset) {
      return const_RecordIterable::CreateEmpty();
    }

    // Key exists and its associated records count is > offset.
    auto begin = it->second.cbegin();
    auto end = it->second.cend();
    size_t size = it->second.size();
    if (offset > 0) {
      begin = std::next(begin, offset);
    }
    if (limit > -1 && static_cast<size_t>(limit) + offset < size) {
      end = std::next(begin, limit);
    }
    return const_RecordIterable{
        const_RecordIterator{std::make_unique<R_cimpl>(begin)},
        const_RecordIterator{std::make_unique<R_cimpl>(end)}};
  }
  RecordIterable Lookup(const Key &key, int limit = -1, size_t offset = 0) {
    auto it = this->contents_.find(key);
    if (it == this->contents_.end() ||
        static_cast<size_t>(it->second.size()) <= offset) {
      return RecordIterable::CreateEmpty();
    }

    // Key exists and its associated records count is > offset.
    auto begin = it->second.begin();
    auto end = it->second.end();
    size_t size = it->second.size();
    if (offset > 0) {
      begin = std::next(begin, offset);
    }
    if (limit > -1 && static_cast<size_t>(limit) + offset < size) {
      end = std::next(begin, limit);
    }
    return RecordIterable{RecordIterator{std::make_unique<R_impl>(begin)},
                          RecordIterator{std::make_unique<R_impl>(end)}};
  }

  // Return an Iterable set of records from the given key that are larger than
  // a given record. Should only be used on record ordered containers.
  const_RecordIterable LookupGreater(const Key &key, const Record &cmp,
                                     int limit = -1, size_t offset = 0) const {
    if constexpr (!std::is_null_pointer<RecordCompare>::value) {
      auto it = this->contents_.find(key);
      if (it == this->contents_.end() ||
          static_cast<size_t>(it->second.size()) <= offset) {
        return const_RecordIterable::CreateEmpty();
      }

      // Key exists and its associated records count is > offset.
      auto cbeg = it->second.cbegin();
      auto end = it->second.cend();
      auto begin = std::upper_bound(cbeg, end, cmp, this->compare_);
      size_t size = std::distance(begin, end);
      if (offset > 0) {
        begin = std::next(begin, offset);
      }
      if (limit > -1 && static_cast<size_t>(limit) + offset < size) {
        end = std::next(begin, limit);
      }

      return const_RecordIterable{
          const_RecordIterator{std::make_unique<R_cimpl>(begin)},
          const_RecordIterator{std::make_unique<R_cimpl>(end)}};
    }

    return const_RecordIterable::CreateEmpty();
  }

  // Return an Iterable set of keys contained by this group, in the underlying
  // order (specified by M).
  const_KeyIterable Keys() const {
    return const_KeyIterable{
        const_KeyIterator{std::make_unique<K_cimpl>(this->contents_.cbegin())},
        const_KeyIterator{std::make_unique<K_cimpl>(this->contents_.cend())}};
  }
  KeyIterable Keys() {
    return KeyIterable{
        KeyIterator{std::make_unique<K_impl>(this->contents_.begin())},
        KeyIterator{std::make_unique<K_impl>(this->contents_.end())}};
  }

  // Checks if we have any records mapped to the given key.
  bool Contains(const Key &key) const {
    return this->contents_.find(key) != this->contents_.end();
  }

  // Insert a record mapped by given key.
  bool Insert(const Key &k, const Record &r) {
    // Insert an empty vector for k if k does not exist in the map.
    V *v;
    if constexpr (std::is_null_pointer<RecordCompare>::value) {
      v = &this->contents_[k];
    } else {
      v = &this->contents_.emplace(k, compare_).first->second;
    }

    // Add or remove r from the vector of k in the map.
    if (r.IsPositive()) {
      if constexpr (HasFunction<V>::Insert()) {
        v->insert(r.Copy());
      } else {
        v->push_back(r.Copy());
      }
      this->count_++;
    } else {
      // We need to find r in v in the most efficient way possible.
      // If V supports .find() we use it (e.g. sorted containers).
      // Otherwise, we use a linear scan.
      V_citerator it;
      if constexpr (HasFunction<V>::Find()) {
        it = v->find(r);
      } else {
        it = std::find(std::begin(*v), std::end(*v), r);
      }
      // If we found the record, we erase it, and remove all of v from
      // the map if it becomes empty.
      if (it != std::end(*v)) {
        v->erase(it);
        this->count_--;
        if (v->empty()) {
          this->contents_.erase(k);
        }
      }
    }
    return true;
  }

  // Erase entry keyed on @param k
  bool Erase(const Key &k) {
    this->contents_.erase(k);
    return true;
  }

 private:
  RecordCompare compare_;
  size_t count_ = 0;
  M contents_;
};

using UnorderedGroupedData =
    GroupedDataT<absl::flat_hash_map<Key, std::vector<Record>>,
                 std::vector<Record>>;
using KeyOrderedGroupedData =
    GroupedDataT<absl::btree_map<Key, std::vector<Record>>,
                 std::vector<Record>>;
using RecordOrderedGroupedData = GroupedDataT<
    absl::flat_hash_map<Key, absl::btree_multiset<Record, Record::Compare>>,
    absl::btree_multiset<Record, Record::Compare>, Record::Compare>;

}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_OPS_GROUPED_DATA_H_
