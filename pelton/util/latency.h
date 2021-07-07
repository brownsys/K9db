// Performance profiling.
#ifndef PELTON_UTIL_LATENCY_H_
#define PELTON_UTIL_LATENCY_H_

// NOLINTNEXTLINE
#include <chrono>
#include <string>
#include <unordered_map>

namespace pelton {
namespace latency {

class Latency {
 public:
  Latency() : begin_(false) {}

  std::string TurnOn();
  void Start(const std::string &label);
  void End(const std::string &label);
  std::string Measure(const std::string &comment);
  void PrintAll();

 private:
  using TimeType = std::chrono::time_point<std::chrono::high_resolution_clock>;
  using DurationType = std::chrono::nanoseconds;

  bool begin_;
  std::unordered_map<std::string, TimeType> start_times_;
  std::unordered_map<std::string, uint64_t> times_;
  std::unordered_map<std::string, uint64_t> count_;
};

}  // namespace latency
}  // namespace pelton

#endif  // PELTON_UTIL_LATENCY_H_
