#ifndef PELTON_UTIL_ITERATOR_H_
#define PELTON_UTIL_ITERATOR_H_

#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

namespace pelton {
namespace util {

// F: struct with R operator() function.
template <typename I, typename F>
class MapIt {
 public:
  explicit MapIt(const I &it) : it_(it), func_() {}
  MapIt(const I &it, const F &func) : it_(it), func_(func) {}

  // Iterator traits
  using difference_type = int64_t;
  using value_type = decltype(std::declval<F>().Map(std::declval<I *>()));
  using reference = std::remove_reference<value_type>::type &;
  using pointer = std::remove_reference<value_type>::type *;
  using iterator_category = std::input_iterator_tag;

  MapIt &operator++() {
    ++this->it_;
    return *this;
  }
  MapIt operator++(int) {
    MapIt tmp = *this;
    ++(this->it_);
    return tmp;
  }

  bool operator==(const MapIt<I, F> &o) const { return this->it_ == o.it_; }
  bool operator!=(const MapIt<I, F> &o) const { return this->it_ != o.it_; }

  // Access current element.
  value_type operator*() const { return this->func_.Map(&this->it_); }
  value_type operator*() { return this->func_.Map(&this->it_); }

 private:
  I it_;
  F func_;
};

template <typename I>
class Iterable {
 public:
  Iterable(const I &b, const I &e) : begin_(b), end_(e) {}
  Iterable(I &&b, I &&e) : begin_(std::move(b)), end_(std::move(e)) {}

  const I &begin() const { return this->begin_; }
  const I &end() const { return this->end_; }

 private:
  I begin_;
  I end_;
};

}  // namespace util
}  // namespace pelton

#endif  // PELTON_UTIL_ITERATOR_H_
