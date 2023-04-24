#ifndef K9DB_UTIL_INTS_H_
#define K9DB_UTIL_INTS_H_

#include <cstdint>
#include <memory>
#include <string>

namespace k9db {

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

}  // namespace k9db

#endif  // K9DB_UTIL_INTS_H_
