// Performance profiling.
#include "pelton/util/latency.h"

#include <algorithm>
#include <cassert>
#include <iostream>
#include <utility>
#include <vector>

#include "absl/strings/match.h"

namespace pelton {
namespace latency {

std::string Latency::TurnOn() {
  this->begin_ = true;
  this->Start("free");
  return "free";
}

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

std::string Latency::Measure(const std::string &comment) {
  if (!this->begin_) {
    return "";
  }

  if (comment == "free") {
    this->Start("free");
    return "free";
  }
  if (comment == "free") {
    this->End("free");
    return "free";
  }

  if (absl::StartsWithIgnoreCase(comment, "--start: ")) {
    this->End("free");
    this->Start(comment.substr(9));
    return comment.substr(9);
  } else if (absl::StartsWithIgnoreCase(comment, "--start ")) {
    this->End("free");
    this->Start(comment.substr(8));
    return comment.substr(8);
  }
  if (absl::StartsWithIgnoreCase(comment, "--end: ")) {
    this->End(comment.substr(7));
    this->Start("free");
    return "free";
  } else if (absl::StartsWithIgnoreCase(comment, "--end ")) {
    this->End(comment.substr(6));
    this->Start("free");
    return "free";
  }

  assert(false);
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
