#ifndef PELTON_UTIL_ITERATOR_H_
#define PELTON_UTIL_ITERATOR_H_

#include <cassert>
#include <functional>
#include <iterator>
#include <tuple>
#include <type_traits>
#include <utility>

namespace pelton {
namespace util {

/*
 * Generic iterable object.
 * Abstracts away the type of the underlying container/iterators.
 */
template <typename I>
class Iterable {
 public:
  using iterator = I;

  Iterable(const I &b, const I &e) : begin_(b), end_(e) {}
  Iterable(I &&b, I &&e) : begin_(std::move(b)), end_(std::move(e)) {}

  iterator begin() { return this->begin_; }
  iterator end() { return this->end_; }

  // Structural binding.
  template <size_t Index>
  std::tuple_element_t<Index, Iterable<I>> get() {
    if constexpr (Index == 0) return this->begin_;
    if constexpr (Index == 1) return this->end_;
  }

 private:
  I begin_;
  I end_;
};

/*
 * Mapping iterator: Takes an input iterator that derefences to V, and a
 * functor mapping V -> R, and returns an input iterator that deferences to R.
 */

// V: type of argument, R: type of returned values.
template <typename V, typename R>
struct MapFunctor {
 public:
  // Identity..
  MapFunctor() : functor_() {}
  explicit MapFunctor(const std::function<R(V)> &f) : functor_(f) {}
  explicit MapFunctor(std::function<R(V)> &&f)
      : functor_(std::forward<std::function<R(V)>>(f)) {}

  R Map(V i) { return this->functor_(i); }

 private:
  std::function<R(V)> functor_;
};

// F: struct with `R Map(V)` function mapping V -> R, where V == *I.
template <typename I, typename F>
class MapIt {
 private:
  using V = typename std::iterator_traits<I>::reference;

 public:
  // From the struct as a type using default constructor.
  explicit MapIt(const I &it) : it_(it), functor_() {}

  // From a lambda function.
  template <typename R>
  MapIt(const I &it, const std::function<R(V)> &lambda)
      : it_(it), functor_(lambda) {}

  template <typename R>
  MapIt(const I &it, std::function<R(V)> &&lambda)
      : it_(it), functor_(std::forward<std::function<R(V)>>(lambda)) {}

  // From an instance of the struct.
  template <typename M = decltype(&F::Map)>
  MapIt(const I &it, const F &functor) : it_(it), functor_(functor) {}

  template <typename M = decltype(&F::Map)>
  MapIt(const I &it, F &&functor)
      : it_(it), functor_(std::forward<F>(functor)) {}

  // From lambda.
  template <typename A,
            typename V = typename std::iterator_traits<I>::reference,
            typename FN = decltype(std::function(std::declval<A &&>())),
            typename R = decltype(std::declval<FN>()(std::declval<V>()))>
  MapIt(const I &it, A &&lam)
      : MapIt(it, std::function(std::forward<A>(lam))) {}

  // Iterator traits
  using difference_type = int64_t;
  using value_type = decltype(std::declval<F>().Map(std::declval<V>()));
  using reference = value_type;
  using pointer = value_type *;
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

  // We can compare MapIt with different mapping but the same underlying
  template <typename FF>
  bool operator==(const MapIt<I, FF> &o) const {
    return this->it_ == o.it_;
  }
  template <typename FF>
  bool operator!=(const MapIt<I, FF> &o) const {
    return this->it_ != o.it_;
  }

  // With these we can compare against unmapped end() iterator.
  bool operator==(const I &o) const { return this->it_ == o; }
  bool operator!=(const I &o) const { return this->it_ != o; }

  // Access current element.
  value_type operator*() { return this->functor_.Map(*this->it_); }
  value_type operator*() const { return this->functor_.Map(*this->it_); }

 private:
  I it_;
  F functor_;
};

// Additional deduction guidance for lambda constructor.
template <typename I, typename R,
          typename V = typename std::iterator_traits<I>::reference>
MapIt(const I &, const std::function<R(V)> &) -> MapIt<I, MapFunctor<V, R>>;

template <typename I, typename R,
          typename V = typename std::iterator_traits<I>::reference>
MapIt(const I &, std::function<R(V)> &&) -> MapIt<I, MapFunctor<V, R>>;

template <typename I, typename A,
          typename V = typename std::iterator_traits<I>::reference,
          typename FN = decltype(std::function(std::declval<A &&>())),
          typename R = decltype(std::declval<FN>()(std::declval<V>()))>
MapIt(const I &it, A &&lam) -> MapIt<I, MapFunctor<V, R>>;

// Shorthand for creating Map with a default constructible Map functor struct.
template <typename F, typename I,
          typename V = std::iterator_traits<I>::value_type,
          typename M = decltype(&F::Map)>
MapIt<I, F> Map(const I &i) {
  return MapIt<I, F>(i);
}

/*
 * Shorthand for creating Iterable with a MapIt in one shot.
 */

// Default constructor.
template <typename F, typename C,
          typename I = decltype(std::declval<C>().begin()),
          typename EI = decltype(std::declval<C>().end())>
Iterable<MapIt<I, F>> Map(C *c) {
  static_assert(std::is_same_v<I, decltype(std::declval<C>().end())>);
  return Iterable(MapIt<I, F>(c->begin()), MapIt<I, F>(c->end()));
}

// With std::function.
template <typename C, typename R,
          typename I = decltype(std::declval<C>().begin()),
          typename V = typename std::iterator_traits<I>::reference>
Iterable<MapIt<I, MapFunctor<V, R>>> Map(C *c, const std::function<R(V)> &f) {
  static_assert(std::is_same_v<I, decltype(std::declval<C>().end())>);
  return Iterable(MapIt(c->begin(), f), MapIt(c->end(), std::function<R(V)>()));
}
template <typename C, typename R,
          typename I = decltype(std::declval<C>().begin()),
          typename V = typename std::iterator_traits<I>::reference>
Iterable<MapIt<I, MapFunctor<V, R>>> Map(C *c, std::function<R(V)> &&f) {
  static_assert(std::is_same_v<I, decltype(std::declval<C>().end())>);
  return Iterable(MapIt(c->begin(), std::forward<std::function<R(V)>>(f)),
                  MapIt(c->end(), std::function<R(V)>()));
}

// With an instance of a functor struct.
template <typename C, typename F,
          typename I = decltype(std::declval<C>().begin()),
          typename V = typename std::iterator_traits<I>::reference,
          typename R = decltype(std::declval<F>().Map(std::declval<V>()))>
Iterable<MapIt<I, F>> Map(C *c, const F &f) {
  static_assert(std::is_same_v<I, decltype(std::declval<C>().end())>);
  return Iterable(MapIt(c->begin(), f), MapIt(c->end(), f));
}
template <typename C, typename F,
          typename I = decltype(std::declval<C>().begin()),
          typename V = typename std::iterator_traits<I>::reference,
          typename R = decltype(std::declval<F>().Map(std::declval<V>()))>
Iterable<MapIt<I, F>> Map(C *c, F &&f) {
  static_assert(std::is_same_v<I, decltype(std::declval<C>().end())>);
  auto b = MapIt(c->begin(), std::forward<F>(f));
  auto e = MapIt(c->end(), std::forward<F>(f));  // Moving twice but never used.
  return Iterable(std::move(b), std::move(e));
}

// From lambda.
template <typename C, typename A,
          typename I = decltype(std::declval<C>().begin()),
          typename V = typename std::iterator_traits<I>::reference,
          typename FN = decltype(std::function(std::declval<A &&>())),
          typename R = decltype(std::declval<FN>()(std::declval<V>()))>
Iterable<MapIt<I, MapFunctor<V, R>>> Map(C *c, A &&lam) {
  static_assert(std::is_same_v<I, decltype(std::declval<C>().end())>);
  return Map(c, std::function<R(V)>(lam));
}

/*
 * ZipIterator: takes in two iterators and produces an iterator to pairs
 * with values from each iterator respectively.
 * Mimics the weakest iterator. E.g. If both iterator are RandomAccess, this
 * will be RandomAccess, but if one is not, this iterator wont be.
 */

#define OP(op, p, T1, T2)                                   \
  bool operator op(const p<T1, T2> &o) const {              \
    auto r = static_cast<const std::pair<T1, T2> &>(*this); \
    auto l = static_cast<const std::pair<T1, T2> &>(o);     \
    return r op l;                                          \
  }

// For Zipping two iterators.
// T1 and T2 are not references.
template <typename T1, typename T2>
class ValPair : public std::pair<T1, T2> {
 private:
  using R1 = std::remove_reference<T1>::type;
  using R2 = std::remove_reference<T2>::type;
  static_assert(!std::is_same_v<T1, R1 &>);
  static_assert(!std::is_same_v<T2, R2 &>);

 public:
  ValPair(const T1 &v1, const T2 &v2) : std::pair<T1, T2>(v1, v2) {}
  ValPair(T1 &&v1, T2 &&v2)
      : std::pair<T1, T2>(std::forward<T1>(v1), std::forward<T2>(v2)) {}

  // Comparators.
  OP(<, ValPair, T1, T2)
  OP(<=, ValPair, T1, T2)
  OP(>, ValPair, T1, T2)
  OP(>=, ValPair, T1, T2)
};

// R1, R2 are references, they may be const references as well!
template <typename R1, typename R2>
class RefPair : public std::pair<R1, R2> {
 private:
  using T1 = std::remove_reference<R1>::type;
  using T2 = std::remove_reference<R2>::type;
  static_assert(std::is_same_v<R1, T1 &>);
  static_assert(std::is_same_v<R2, T2 &>);

 public:
  // NOLINTNEXTLINE
  RefPair(T1 &r1, T2 &r2) : std::pair<R1, R2>(r1, r2) {}
  // NOLINTNEXTLINE
  RefPair(ValPair<T1, T2> &o) : std::pair<R1, R2>(o.first, o.second) {}

  // Copy/move assignment from another ValPair.
  // NOLINTNEXTLINE
  RefPair<R1, R2> &operator=(ValPair<T1, T2> &v) {
    this->first = v.first;
    this->second = v.second;
    return *this;
  }
  RefPair<R1, R2> &operator=(ValPair<T1, T2> &&v) {
    this->first = std::move(v.first);
    this->second = std::move(v.second);
    return *this;
  }

  // Comparators.
  OP(==, RefPair, R1, R2)
  OP(!=, RefPair, R1, R2)
  OP(<, RefPair, R1, R2)
  OP(<=, RefPair, R1, R2)
  OP(>, RefPair, R1, R2)
  OP(>=, RefPair, R1, R2)

  // Transform into a value by copying.
  operator ValPair<T1, T2>() const {
    return ValPair(this->first, this->second);
  }
};

template <typename I1, typename I2>
class ZipIt {
 private:
  using Traits1 = std::iterator_traits<I1>;
  using Traits2 = std::iterator_traits<I2>;
  using Val1 = typename Traits1::value_type;
  using Val2 = typename Traits2::value_type;
  using Ref1 = typename Traits1::reference;
  using Ref2 = typename Traits2::reference;
  static_assert(std::is_same_v<typename Traits1::difference_type,
                               typename Traits2::difference_type>);
  static_assert(std::is_same_v<typename Traits1::iterator_category,
                               typename Traits2::iterator_category>);

 public:
  ZipIt() = default;
  ZipIt(I1 i1, I2 i2) : i1_(i1), i2_(i2) {}

  // Deduce iterator traits from I1 and I2.
  using value_type = ValPair<Val1, Val2>;
  using reference = RefPair<Ref1, Ref2>;
  using pointer = value_type *;
  using difference_type = Traits1::difference_type;
  using iterator_category = Traits1::iterator_category;

  // EqualityComparable.
  bool operator==(const ZipIt<I1, I2> &o) const {
    return this->i1_ == o.i1_ && this->i2_ == o.i2_;
  }
  bool operator!=(const ZipIt<I1, I2> &o) const {
    return this->i1_ != o.i1_ && this->i2_ != o.i2_;
  }

  // LegacyIterator.
  ZipIt<I1, I2> &operator++() {
    ++this->i1_;
    ++this->i2_;
    return *this;
  }

  // LegacyInputIterator (LegacyOutputIterator is implied).
  ZipIt<I1, I2> operator++(int) {
    ZipIt<I1, I2> tmp = *this;
    ++(this->i1_);
    ++(this->i2_);
    return tmp;
  }

  // Bi-directional.
  ZipIt<I1, I2> &operator--() {
    --this->i1_;
    --this->i2_;
    return *this;
  }
  ZipIt<I1, I2> operator--(int) {
    ZipIt<I1, I2> tmp = *this;
    --(this->i1_);
    --(this->i2_);
    return tmp;
  }

  // LegacyRandomAccessIterator.
  ZipIt<I1, I2> &operator+=(difference_type n) {
    this->i1_ += n;
    this->i2_ += n;
    return *this;
  }
  ZipIt<I1, I2> &operator-=(difference_type n) {
    this->i1_ -= n;
    this->i2_ -= n;
    return *this;
  }
  difference_type operator-(const ZipIt<I1, I2> &o) const {
    difference_type n1 = this->i1_ - o.i1_;
    difference_type n2 = this->i2_ - o.i2_;
    assert(n1 == n2);
    return n1;
  }

  ZipIt<I1, I2> operator+(difference_type n) const {
    return ZipIt(this->i1_ + n, this->i2_ + n);
  }
  ZipIt<I1, I2> operator-(difference_type n) const {
    return ZipIt(this->i1_ - n, this->i2_ - n);
  }

  bool operator<(const ZipIt<I1, I2> &o) const {
    return this->i1_ < o.i1_ && this->i2_ < o.i2_;
  }
  bool operator<=(const ZipIt<I1, I2> &o) const {
    return this->i1_ <= o.i1_ && this->i2_ <= o.i2_;
  }
  bool operator>(const ZipIt<I1, I2> &o) const {
    return this->i1_ > o.i1_ && this->i2_ > o.i2_;
  }
  bool operator>=(const ZipIt<I1, I2> &o) const {
    return this->i1_ >= o.i1_ && this->i2_ >= o.i2_;
  }

  // Lookup: must provide read-only and writtable versions.
  // Writing must change underlying iterators.
  reference operator*() const { return reference(*this->i1_, *this->i2_); }
  reference operator*() { return reference(*this->i1_, *this->i2_); }

  reference operator[](difference_type i) const {
    return reference(this->i1_[i], this->i2_[i]);
  }
  reference operator[](difference_type i) {
    return reference(this->i1_[i], this->i2_[i]);
  }

 private:
  I1 i1_;
  I2 i2_;
};

// <nunber> + <ZipIt>
template <typename I1, typename I2>
ZipIt<I1, I2> operator+(typename std::iterator_traits<I1>::difference_type n,
                        const ZipIt<I1, I2> &o) {
  static_assert(std::is_same_v<std::iterator_traits<I1>::difference_type,
                               std::iterator_traits<I2>::difference_type>);
  return o + n;
}

// Shorthand to Zip containers quickly.
template <typename V1, typename V2,
          typename I1 = decltype(std::declval<V1>().begin()),
          typename I2 = decltype(std::declval<V2>().begin())>
Iterable<ZipIt<I1, I2>> Zip(V1 *v1, V2 *v2) {
  return Iterable(ZipIt(v1->begin(), v2->begin()), ZipIt(v1->end(), v2->end()));
}

}  // namespace util
}  // namespace pelton

// Structural bindings for Iterable.
namespace std {

// Swap underlying references.
template <typename R1, typename R2>
void swap(pelton::util::RefPair<R1, R2> l, pelton::util::RefPair<R1, R2> r) {
  std::swap(l.first, r.first);
  std::swap(l.second, r.second);
}

// Iterable has 2 elements for binding, each of type I.
template <typename I>
struct tuple_size<pelton::util::Iterable<I>> : integral_constant<size_t, 2> {};

template <size_t Index, typename I>
struct tuple_element<Index, pelton::util::Iterable<I>>
    : tuple_element<Index, tuple<I, I>> {};

}  // namespace std

#endif  // PELTON_UTIL_ITERATOR_H_
