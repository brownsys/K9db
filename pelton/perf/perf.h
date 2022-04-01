#ifndef PELTON_PERF_PERF_H_
#define PELTON_PERF_PERF_H_

// NOLINTNEXTLINE
#include <chrono>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/shards/upgradable_lock.h"
#include "pelton/sql/result.h"

namespace pelton {
namespace perf {

// Performance information about a single query.
class QueryPerf {
 public:
  explicit QueryPerf(const std::string &query);

  // Timing breakdown.
  void StartTime(const std::string &name);
  void EndTime(const std::string &name);

  // Total timing.
  uint64_t Done(int status);
  uint64_t Done(const sql::SqlResult &result);

  // Comparison (for sorting).
  bool operator<(const QueryPerf &rhs) const { return this->time_ < rhs.time_; }

  // To a record (for printing/sending to client).
  void ToRecords(std::vector<dataflow::Record> *records) const;

 private:
  // Perf info.
  std::string query_;
  int64_t result_size_;
  std::unordered_map<std::string, uint64_t> breakdown_;
  uint64_t time_;
  // Temporary.
  std::chrono::time_point<std::chrono::steady_clock> start_;
  std::unordered_map<std::string,
                     std::chrono::time_point<std::chrono::steady_clock>>
      start_breakdown_;
};

// Performance information about an entire endpoint.
class EndpointPerf {
 public:
  EndpointPerf() = default;

  // Initialize an endpoint perf tracker.
  void Initialize(const std::string &name);

  // Adding queries.
  void AddQuery(const std::string &query);
  void DoneQuery(int status);
  void DoneQuery(const sql::SqlResult &result);

  // Timing breakdown.
  void StartTime(const std::string &name);
  void EndTime(const std::string &name);

  // Comparison (for sorting).
  bool operator<(const EndpointPerf &rhs) const {
    return this->total_time_ < rhs.total_time_;
  }

  void ToRecords(std::vector<dataflow::Record> *records) const;

 private:
  std::string name_;
  std::vector<QueryPerf> queries_;
  uint64_t total_time_;
};

// Collects data about all the perfs.
class PerfManager {
 public:
  PerfManager() = default;

  void AddEndPoint(EndpointPerf &&endpoint);
  std::vector<dataflow::Record> ToRecords() const;

 private:
  std::set<EndpointPerf> endpoints_;
  // Synchronize endpoints_.
  mutable shards::UpgradableMutex mtx_;
};

}  // namespace perf
}  // namespace pelton

#endif  // PELTON_PERF_PERF_H_
