#ifndef PELTON_UTIL_INTS_H_
#define PELTON_UTIL_INTS_H_

#include <cstdint>
#include <memory>
#include <string>

namespace pelton {

// Conversions to allow numeric literals to be typed with uint64_t or int64_t
// in a portable way.

// Linter complains about using unsigned long long here. However, per c++
// standard, a user defined literal must take exactly "unsigned long long".
// https://en.cppreference.com/w/cpp/language/user_literal
// NOLINTNEXTLINE
uint64_t operator"" _u(unsigned long long x);
// NOLINTNEXTLINE
int64_t operator"" _s(unsigned long long x);

std::unique_ptr<std::string> operator"" _uptr(const char *x, size_t len);

}  // namespace pelton

#endif  // PELTON_UTIL_INTS_H_
