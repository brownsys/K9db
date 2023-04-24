// Merging of K sorted arrays.
#ifndef K9DB_UTIL_MERGE_SORT_H_
#define K9DB_UTIL_MERGE_SORT_H_

#include <queue>
#include <set>
#include <utility>
#include <vector>

namespace k9db {
namespace util {

using element = std::pair<size_t, size_t>;

// T is usually dataflow::Record.
// V is a sorted container of Ts, usually a std::vector.
// C is a comparator of Ts, e.g. dataflow::Record::Compare.
template <typename T, typename C>
std::vector<T> KMerge(std::vector<const std::multiset<T, C> *> &&V,
                      const dataflow::Record::Compare &compare, int limit,
                      size_t offset) {
  using itelm = std::pair<size_t, typename std::multiset<T, C>::const_iterator>;
  auto cmp = [&](itelm &l, itelm &r) { return compare(*r.second, *l.second); };

  std::vector<T> result;
  std::priority_queue<itelm, std::vector<itelm>, decltype(cmp)> queue(cmp);
  for (size_t i = 0; i < V.size(); i++) {
    if (V.at(i)->size() > 0) {
      queue.emplace(i, V.at(i)->begin());
    }
  }

  size_t count = 0;
  while (!queue.empty()) {
    const auto &e = queue.top();
    size_t i = e.first;
    typename std::multiset<T, C>::const_iterator j = e.second;
    queue.pop();
    if (i >= offset) {
      if (limit == -1 || count < offset + static_cast<size_t>(limit)) {
        result.push_back(j->Copy());
      } else {
        break;
      }
    }
    if (++j != V.at(i)->end()) {
      queue.emplace(i, j);
    }
    count++;
  }
  return result;
}

template <typename T, typename C>
std::vector<T> KMerge(std::vector<std::vector<T>> &&V, const C &compare,
                      int limit, size_t offset) {
  auto cmp = [&](element &l, element &r) {
    return compare(V.at(r.first).at(r.second), V.at(l.first).at(l.second));
  };

  std::vector<T> result;
  std::priority_queue<element, std::vector<element>, decltype(cmp)> queue(cmp);
  for (size_t i = 0; i < V.size(); i++) {
    if (V.at(i).size() > 0) {
      queue.emplace(i, 0);
    }
  }

  size_t count = 0;
  while (!queue.empty()) {
    const auto &e = queue.top();
    size_t i = e.first;
    size_t j = e.second;
    queue.pop();
    if (i >= offset) {
      if (limit == -1 || count < offset + static_cast<size_t>(limit)) {
        result.push_back(std::move(V.at(i).at(j)));
      } else {
        break;
      }
    }
    if (j + 1 < V.at(i).size()) {
      queue.emplace(i, j + 1);
    }
    count++;
  }
  return result;
}

template <typename T>
std::vector<T> KMerge(std::vector<std::vector<T>> &&V) {
  auto cmp = [&](element &l, element &r) {
    return V.at(r.first).at(r.second) < V.at(l.first).at(l.second);
  };

  std::vector<T> result;
  std::priority_queue<element, std::vector<element>, decltype(cmp)> queue(cmp);
  for (size_t i = 0; i < V.size(); i++) {
    if (V.at(i).size() > 0) {
      queue.emplace(i, 0);
    }
  }
  while (!queue.empty()) {
    const auto &e = queue.top();
    size_t i = e.first;
    size_t j = e.second;
    queue.pop();
    result.push_back(std::move(V.at(i).at(j)));
    if (j + 1 < V.at(i).size()) {
      queue.emplace(i, j + 1);
    }
  }
  return result;
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
}  // namespace k9db

#endif  // K9DB_UTIL_MERGE_SORT_H_
