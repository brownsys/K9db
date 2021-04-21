// Performance profiling.
#ifndef PELTON_UTIL_PERF_H_
#define PELTON_UTIL_PERF_H_

#define PELTON_USE_PERF

// NOLINTNEXTLINE
#include <chrono>
#include <string>
#include <unordered_map>

namespace pelton {
namespace perf {

class Perf {
 public:
  static Perf &INSTANCE();

  Perf() {}

  void Start(const std::string &label);
  void End(const std::string &label);
  void PrintAll();

 private:
  using TimeType = std::chrono::time_point<std::chrono::high_resolution_clock>;
  using DurationType = std::chrono::nanoseconds;

  static Perf PERF;

  std::unordered_map<std::string, TimeType> start_times_;
  std::unordered_map<std::string, uint64_t> times_;
};

inline void Start(const std::string &label) {
#ifdef PELTON_USE_PERF
  Perf::INSTANCE().Start(label);
#endif
}
inline void End(const std::string &label) {
#ifdef PELTON_USE_PERF
  Perf::INSTANCE().End(label);
#endif
}
inline void PrintAll() {
#ifdef PELTON_USE_PERF
  Perf::INSTANCE().PrintAll();
#endif
}

}  // namespace perf
}  // namespace pelton

#endif  // PELTON_UTIL_PERF_H_
