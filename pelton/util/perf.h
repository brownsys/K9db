// Performance profiling.
#ifndef PELTON_UTIL_PERF_H_
#define PELTON_UTIL_PERF_H_

// NOLINTNEXTLINE
#include <chrono>
#include <string>
#include <unordered_map>
#include <vector>

namespace pelton {
namespace perf {

class Perf {
 public:
  Perf() : begin_(true), start_times_(), times_() {}

  void Start(const std::string &label) {}
  void End(const std::string &label) {}
  void PrintAll() {}
  void Combine(const Perf &perf) {}

 private:
  using TimeType = std::chrono::time_point<std::chrono::high_resolution_clock>;
  using DurationType = std::chrono::nanoseconds;

  bool begin_;
  std::unordered_map<std::string, TimeType> start_times_;
  std::unordered_map<std::string, std::vector<uint64_t>> times_;
};

}  // namespace perf
}  // namespace pelton

#endif  // PELTON_UTIL_PERF_H_
