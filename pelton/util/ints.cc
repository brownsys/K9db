#include "pelton/util/ints.h"

namespace pelton {

// User defined literals only support certain argument types.
// For example, (signed) int is not supported.
// The compiler will treat any literal as unsigned long long when passed to
// our user defined literal.
// Specifically, this means that -1_s for example is read as:
// ((unsigned long long) -1)_s.
//
// This is fine, we know negative values will only be used with _s,
// first thing we do in _s is to cast input to long long, which re-introduces
// the sign (i.e. undoes the typing of -1 as unsigned long long), and then
// cast to the final representation.
//
// In other words, the expressions -1_s really becomes:
// (int64_t) ((long long) ((unsigned long long) -1))

// NOLINTNEXTLINE
uint64_t operator"" _u(unsigned long long x) {
  return static_cast<uint64_t>(x);
}
// NOLINTNEXTLINE
int64_t operator"" _s(unsigned long long x) {
  // NOLINTNEXTLINE
  return static_cast<int64_t>(static_cast<long long>(x));
}
// NOLINTNEXTLINE
std::unique_ptr<std::string> operator"" _Uptr(const char *x, size_t len) {
  return std::make_unique<std::string>(x, len);
}

}  // namespace pelton
