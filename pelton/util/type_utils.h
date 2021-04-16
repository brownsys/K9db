// NOLINTFILE
#ifndef PELTON_UTIL_TYPE_UTILS_H_
#define PELTON_UTIL_TYPE_UTILS_H_

#include <memory>
#include <string>

#include "glog/logging.h"

template <typename Arg>
std::string TypeNameFor(Arg &&t) {
  if constexpr (std::is_same<std::remove_reference_t<Arg>, uint64_t>::value) {
    return std::string("uint64_t");
    // NOLINTNEXTLINE
  } else if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                    int64_t>::value) {
    return std::string("int64_t");
    // NOLINTNEXTLINE
  } else if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                    std::unique_ptr<std::string>>::value) {
    return std::string("std::unique_ptr<std::string>");
    // NOLINTNEXTLINE
  } else if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                    long>::value) {  // NOLINT
    return std::string("long");
  } else {
    LOG(FATAL) << "Unrecognized type";
  }
}

#endif  // PELTON_UTIL_TYPE_UTILS_H_
