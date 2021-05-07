// Performance profiling.
#include "pelton/util/latency.h"

#include <algorithm>
#include <iostream>
#include <utility>
#include <vector>

#include "absl/strings/match.h"

namespace pelton {
namespace latency {

void Latency::TurnOn() { this->begin_ = true; }

// Timing.
void Latency::Start(const std::string &label) {
  if (this->begin_) {
    this->count_[label]++;
    auto time = std::chrono::high_resolution_clock::now();
    this->start_times_.insert({label, time});
  }
}

void Latency::End(const std::string &label) {
  if (this->begin_) {
    this->count_[label]++;
    auto end = std::chrono::high_resolution_clock::now();
    auto start = this->start_times_.at(label);
    this->start_times_.erase(label);
    uint64_t diff =
        std::chrono::duration_cast<DurationType>(end - start).count();
    this->times_[label] += diff;
  }
}

bool Latency::Measure(const std::string &comment,
                      const std::string &skip_endpoints) {
  if (absl::StartsWithIgnoreCase(comment, "--start: ")) {
    if (absl::StrContains(skip_endpoints, comment.substr(10))) {
      return false;
    }
    this->Start(comment.substr(9));
    return true;
  } else if (absl::StartsWithIgnoreCase(comment, "--start ")) {
    if (absl::StrContains(skip_endpoints, comment.substr(9))) {
      return false;
    }
    this->Start(comment.substr(8));
    return true;
  }
  if (absl::StartsWithIgnoreCase(comment, "--end: ")) {
    if (absl::StrContains(skip_endpoints, comment.substr(8))) {
      return false;
    }
    this->End(comment.substr(7));
    return true;
  } else if (absl::StartsWithIgnoreCase(comment, "--end ")) {
    if (absl::StrContains(skip_endpoints, comment.substr(7))) {
      return false;
    }
    this->End(comment.substr(6));
    return true;
  } else {
    return true;
  }
}

// Display results of timing.
void Latency::PrintAll() {
  std::cout << "Latency profiler: ----" << std::endl;
  std::vector<std::pair<std::string, uint64_t>> vec(this->times_.begin(),
                                                    this->times_.end());
  std::sort(vec.begin(), vec.end());  // Sort by endpoint name.
  for (const auto &[label, duration] : vec) {
    uint64_t ms = duration / 1000000;
    double avg = ms / this->count_.at(label);
    std::cout << label << ": " << avg << "ms" << std::endl;
  }
  std::cout << "----------------------" << std::endl;
}

}  // namespace latency
}  // namespace pelton
