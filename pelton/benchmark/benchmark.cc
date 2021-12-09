// NOLINTNEXTLINE
#include <chrono>
#include <iostream>
// NOLINTNEXTLINE
#include <thread>

#include "glog/logging.h"
#include "pelton/benchmark/client.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/planner/planner.h"
#include "pelton/sqlast/parser.h"
#include "pelton/util/status.h"

using CType = pelton::sqlast::ColumnDefinition::Type;

namespace pelton {
namespace benchmark {

void ParseSchema(const std::string &table, dataflow::DataFlowState *state) {
  sqlast::SQLParser parser;
  auto result = parser.Parse(table);
  if (!result.ok()) {
    LOG(FATAL) << "Cannot parse schema: " << table;
  }
  auto &statement = result.value();
  if (statement->type() != sqlast::AbstractStatement::Type::CREATE_TABLE) {
    LOG(FATAL) << "Cannot parse schema: " << table;
  }
  state->AddTableSchema(*static_cast<sqlast::CreateTable *>(statement.get()));
}

void CreateFlow(const std::string &query, dataflow::DataFlowState *state) {
  state->AddFlow("bench_flow", pelton::planner::PlanGraph(state, query));
  pelton::planner::ShutdownPlanner();
}

void Thread(size_t index, size_t batch_size, size_t batch_count,
            dataflow::DataFlowState *state,
            std::chrono::time_point<std::chrono::system_clock> *ptr) {
  Client client{index, state};
  *ptr = client.Benchmark(batch_size, batch_count);
}

void Benchmark(const std::vector<std::string> &tables, const std::string &query,
               size_t threads, size_t batch_size, size_t batch_count) {
  // Create an empty state.
  dataflow::DataFlowState state(threads, true);

  // Fill the state with schemas.
  for (const std::string &table : tables) {
    ParseSchema(table, &state);
  }

  // Create the flow.
  CreateFlow(query, &state);

  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << "Starting Benchmark..." << std::endl;

  // Benchmark.
  std::vector<std::thread> clients;
  std::vector<std::chrono::time_point<std::chrono::system_clock> *> starts;
  for (size_t i = 0; i < threads - 1; i++) {
    auto *start = new std::chrono::time_point<std::chrono::system_clock>();
    starts.push_back(start);
    clients.emplace_back(Thread, i, batch_size, batch_count, &state, start);
  }
  // Use current thread as last client.
  std::chrono::time_point<std::chrono::system_clock> min;
  Thread(threads - 1, batch_size, batch_count, &state, &min);

  // Wait until everything is done processing.
  for (auto &client : clients) {
    client.join();
  }
  state.Shutdown();

  // Dataflow is done processing.
  auto end = std::chrono::high_resolution_clock::now();
  for (auto *start : starts) {
    if (*start < min) {
      min = *start;
    }
    delete start;
  }
  auto diff = std::chrono::duration_cast<std::chrono::nanoseconds>(end - min);
  auto time = (diff.count() / 1000.0 / 1000 / 1000);
  auto throughput = (batch_size * batch_count * threads) / time;
  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << "# Threads: " << threads << std::endl;
  std::cout << "# Batches: " << batch_count << std::endl;
  std::cout << "BatchSize: " << batch_size << std::endl;
  std::cout << "# Records: " << threads * batch_count * batch_size << std::endl;
  std::cout << "Total time: " << time << "sec" << std::endl;
  std::cout << "Throughput: " << throughput << " records/sec" << std::endl;

  // Check number of records.
  CHECK_EQ(state.GetFlow("bench_flow").All(-1, 0).size(),
           batch_size * batch_count * threads);
}

}  // namespace benchmark
}  // namespace pelton

int main(int argc, char **argv) {
  // Initialize Google’s logging library.
  google::InitGoogleLogging("cli");

  pelton::benchmark::Benchmark(
      {"CREATE TABLE tbl(col1 INT, col2 VARCHAR, col3 INT);"},
      "SELECT col2, col3 FROM tbl WHERE col3 = ?", 3, 10000, 100);
}
