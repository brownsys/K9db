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
  if (l.second / 1000 == r.second / 1000) {
    return l.first > r.first;
  }
  return l.second > r.second;
}

}  // namespace

/*
// Combine many perfs.
void Perf::Combine(const Perf &perf) {
  for (const auto &[label, v] : perf.times_) {
    auto &tvec = this->times_[label];
    tvec.insert(tvec.end(), v.begin(), v.end());
  }
}

// Timing.
void Perf::Start(const std::string &label) {
  if (this->begin_) {
    auto time = std::chrono::high_resolution_clock::now();
    this->start_times_.insert({label, time});
  }
}

void Perf::End(const std::string &label) {
  if (this->begin_) {
    auto end = std::chrono::high_resolution_clock::now();
    auto start = this->start_times_.at(label);
    this->start_times_.erase(label);
    uint64_t diff =
        std::chrono::duration_cast<DurationType>(end - start).count();
    this->times_[label].push_back(diff);
  }
}

// Display results of timing.
void Perf::PrintAll() {
  std::unordered_map<std::string, uint64_t> avgs;
  for (const auto &[label, vec] : this->times_) {
    uint64_t sum = 0;
    for (uint64_t v : vec) {
      sum += v;
    }
    avgs[label] = sum / vec.size();
  }

  std::vector<std::pair<std::string, uint64_t>> vec(avgs.begin(),
                                                    avgs.end());
  std::sort(vec.begin(), vec.end(), CompareSecond);
  for (const auto &[label, duration] : vec) {
    std::cout << label << ": " << (duration / 1000) << "mu"
              << " count: " << this->times_[label].size() << std::endl;
  }
}
*/

}  // namespace perf
}  // namespace pelton
