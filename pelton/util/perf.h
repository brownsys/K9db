// Performance profiling.
#ifndef PELTON_UTIL_PERF_H_
#define PELTON_UTIL_PERF_H_

// NOLINTNEXTLINE
#include <chrono>
#include <string>
#include <unordered_map>

namespace pelton {
namespace perf {

class Perf {
 public:
  static Perf &INSTANCE();

  Perf() : begin_(false), start_times_(), times_() {}

  void Start();
  void Start(const std::string &label);
  void End(const std::string &label);
  void PrintAll();

 private:
  using TimeType = std::chrono::time_point<std::chrono::high_resolution_clock>;
  using DurationType = std::chrono::nanoseconds;

  static Perf PERF;

  bool begin_;
  std::unordered_map<std::string, TimeType> start_times_;
  std::unordered_map<std::string, uint64_t> times_;
};

inline void Start() {
#ifndef PELTON_OPT
  Perf::INSTANCE().Start();
  Perf::INSTANCE().Start("all");
#endif
}

inline void Start(const std::string &label) {
#ifndef PELTON_OPT
  Perf::INSTANCE().Start(label);
#endif
}
inline void End(const std::string &label) {
#ifndef PELTON_OPT
  Perf::INSTANCE().End(label);
#endif
}
inline void PrintAll() {
#ifndef PELTON_OPT
  Perf::INSTANCE().End("all");
  Perf::INSTANCE().PrintAll();
#endif
}

}  // namespace perf
}  // namespace pelton

#endif  // PELTON_UTIL_PERF_H_
