// Performance profiling.
#include "pelton/util/perf.h"

#include <algorithm>
#include <iostream>
#include <utility>
#include <vector>

namespace pelton {
namespace perf {

namespace {

bool CompareSecond(const std::pair<std::string, uint64_t> &l,
                   const std::pair<std::string, uint64_t> &r) {
  if (l.second / 1000000 == r.second / 1000000) {
    return l.first > r.first;
  }
  return l.second > r.second;
}

}  // namespace

// Initialize singleton.
Perf Perf::PERF = Perf();

// Get singleton.
Perf &Perf::INSTANCE() { return Perf::PERF; }

// Timing.
void Perf::Start(const std::string &label) {
  auto time = std::chrono::high_resolution_clock::now();
  this->start_times_.insert({label, time});
}

void Perf::End(const std::string &label) {
  auto end = std::chrono::high_resolution_clock::now();
  auto start = this->start_times_.at(label);
  this->start_times_.erase(label);
  uint64_t diff = std::chrono::duration_cast<DurationType>(end - start).count();
  this->times_[label] += diff;
}

// Display results of timing.
void Perf::PrintAll() {
  std::vector<std::pair<std::string, uint64_t>> vec(this->times_.begin(),
                                                    this->times_.end());
  std::sort(vec.begin(), vec.end(), CompareSecond);
  for (const auto &[label, duration] : vec) {
    std::cout << label << ": " << (duration / 1000000) << "ms" << std::endl;
  }
}

}  // namespace perf
}  // namespace pelton
