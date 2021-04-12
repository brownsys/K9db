#ifndef PELTON_UTILS_TYPE_UTILS_H_
#define PELTON_UTILS_TYPE_UTILS_H_

#include <string>

#include "glog/logging.h"

template <typename Arg>
std::string TypeNameFor(Arg &&t) {
  if constexpr (std::is_same<std::remove_reference_t<Arg>, uint64_t>::value) {
    return std::string("uint64_t");
  } else if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                    int64_t>::value) {
    return std::string("int64_t");
  } else if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                    std::unique_ptr<std::string>>::value) {
    return std::string("std::unique_ptr<std::string>");
  } else if constexpr (std::is_same<std::remove_reference_t<Arg>,
                                    long>::value) {
    return std::string("long");
  } else {
    LOG(FATAL) << "Unrecognized type";
  }
}

#endif  // PELTON_UTILS_TYPE_UTILS_H_
