#include "pelton/perf/perf.h"

#include <memory>
#include <utility>

#include "pelton/sqlast/ast.h"

namespace pelton {
namespace perf {

namespace {

using CType = sqlast::ColumnDefinition::Type;
using F = pelton::dataflow::SchemaFactory;

#define STR(x) std::make_unique<std::string>(x)
#define I(x) static_cast<int64_t>(x)
#define U(x) static_cast<uint64_t>(x)

}  // namespace

// QueryPerf.
QueryPerf::QueryPerf(const std::string &query) : query_(query) {
  this->start_ = std::chrono::steady_clock::now();
}

// Timing breakdown.
void QueryPerf::StartTime(const std::string &name) {
  this->start_breakdown_.emplace(name, std::chrono::steady_clock::now());
}
void QueryPerf::EndTime(const std::string &name) {
  auto end = std::chrono::steady_clock::now();
  auto diff = end - this->start_breakdown_.at(name);
  auto d = std::chrono::duration_cast<std::chrono::microseconds>(diff).count();
  this->breakdown_.emplace(name, d);
}

// Total timing.
uint64_t QueryPerf::Done(int status) {
  // Measure elapsed time.
  auto end = std::chrono::steady_clock::now();
  auto diff = end - this->start_;
  auto d = std::chrono::duration_cast<std::chrono::microseconds>(diff).count();
  this->time_ = d;
  this->result_size_ = status;
  // Return total time.
  return this->time_;
}
uint64_t QueryPerf::Done(const sql::SqlResult &result) {
  // Measure elapsed time.
  auto end = std::chrono::steady_clock::now();
  auto diff = end - this->start_;
  auto d = std::chrono::duration_cast<std::chrono::microseconds>(diff).count();
  this->time_ = d;
  // Store result size.
  if (result.IsStatement()) {
    this->result_size_ = result.Success() ? 0 : -1;
  } else if (result.IsUpdate()) {
    this->result_size_ = result.UpdateCount();
  } else if (result.IsQuery()) {
    this->result_size_ = 0;
    for (const auto &resultset : result.ResultSets()) {
      this->result_size_ += resultset.size();
    }
  } else {
    this->result_size_ = -10;
  }
  // Return total time.
  return this->time_;
}

// To a record (for printing/sending to client).
void QueryPerf::ToRecords(std::vector<dataflow::Record> *records) const {
  records->emplace_back(F::PERF_LIST_SCHEMA, true, STR(this->query_),
                        I(this->result_size_), U(this->time_));
  for (const auto &[name, time] : this->breakdown_) {
    records->emplace_back(F::PERF_LIST_SCHEMA, true, STR("==== " + name), I(0),
                          U(time));
  }
}

// EndpointPerf.
void EndpointPerf::Initialize(const std::string &name) {
  this->name_ = name;
  this->queries_.clear();
  this->total_time_ = 0;
}

// Adding queries.
void EndpointPerf::AddQuery(const std::string &query) {
  this->queries_.emplace_back(query);
}
void EndpointPerf::DoneQuery(int status) {
  this->total_time_ += this->queries_.back().Done(status);
}
void EndpointPerf::DoneQuery(const sql::SqlResult &result) {
  this->total_time_ += this->queries_.back().Done(result);
}

// Timing breakdown.
void EndpointPerf::StartTime(const std::string &name) {
  this->queries_.back().StartTime(name);
}
void EndpointPerf::EndTime(const std::string &name) {
  this->queries_.back().EndTime(name);
}

void EndpointPerf::ToRecords(std::vector<dataflow::Record> *records) const {
  records->emplace_back(F::PERF_LIST_SCHEMA, true, STR("START: " + this->name_),
                        I(this->queries_.size()), U(this->total_time_));
  for (const QueryPerf &query : this->queries_) {
    query.ToRecords(records);
  }
  records->emplace_back(F::PERF_LIST_SCHEMA, true, STR("---------------------"),
                        I(0), U(0));
}

// PerfManager.
void PerfManager::AddEndPoint(EndpointPerf &&endpoint) {
  shards::UniqueLock writer_lock(&this->mtx_);
  this->endpoints_.insert(std::move(endpoint));
}

std::vector<dataflow::Record> PerfManager::ToRecords() const {
  shards::SharedLock reader_lock(&this->mtx_);
  std::vector<dataflow::Record> records;
  for (const auto &endpoint : this->endpoints_) {
    endpoint.ToRecords(&records);
  }
  return records;
}

}  // namespace perf
}  // namespace pelton
