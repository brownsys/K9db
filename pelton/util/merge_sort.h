// Performance profiling.
#ifndef PELTON_UTIL_MERGE_SORT_H_
#define PELTON_UTIL_MERGE_SORT_H_

#include <list>
#include <type_traits>
#include <utility>
#include <vector>

namespace pelton {
namespace util {

// T is usually dataflow::Record.
// L is a sorted container of Ts.
// C is a comparator of Ts, e.g. dataflow::Record::Compare.
template <typename T, typename L, typename C>
void MergeInto(std::list<T> *target, const L &source, const C &compare,
               int limit) {
  auto it = target->begin();
  auto end = target->end();
  size_t count = 0;
  for (const T &record : source) {
    if (limit > -1 && count >= static_cast<size_t>(limit)) {
      break;
    }
    while (it != end && compare(*it, record)) {
      ++it;
    }
    if (it == end) {
      target->push_back(record.Copy());
    } else {
      target->insert(it, record.Copy());
    }
    count++;
  }
}

// L is a sorted contain of Ts.
// Elements of T are compared using <.
template <typename T, typename L>
void MergeInto(std::list<T> *target, L source) {
  auto it = target->begin();
  auto end = target->end();
  for (const T &record : source) {
    while (it != end && *it < record) {
      ++it;
    }
    if (it == end) {
      target->push_back(record.Copy());
    } else {
      target->insert(it, record.Copy());
    }
  }
}

template <typename T>
std::vector<T> ToVector(std::list<T> *list, int limit, size_t offset) {
  std::vector<T> vec;
  if (limit == -1 || vec.size() < static_cast<size_t>(limit)) {
    vec.reserve(vec.size());
  } else {
    vec.reserve(static_cast<size_t>(limit));
  }
  for (T &e : *list) {
    if (offset != 0) {
      offset--;
      continue;
    }
    if (limit > -1 && vec.size() >= static_cast<size_t>(limit)) {
      break;
    }
    vec.push_back(std::move(e));
  }
  return vec;
}

template <typename T>
void Trim(std::vector<T> *vec, int limit, size_t offset) {
  if (offset >= vec->size()) {
    vec->clear();
    return;
  }
  if (offset > 0) {
    vec->erase(vec->begin(), vec->begin() + offset);
  }
  if (limit > -1 && static_cast<size_t>(limit) < vec->size()) {
    vec->erase(vec->begin() + limit, vec->end());
  }
}

}  // namespace util
}  // namespace pelton

#endif  // PELTON_UTIL_MERGE_SORT_H_
