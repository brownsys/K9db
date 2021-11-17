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
            dataflow::DataFlowState *state) {
  Client client{index, state};
  client.Benchmark(batch_size, batch_count);
}

void Benchmark(const std::vector<std::string> &tables, const std::string &query,
               size_t threads, size_t batch_size, size_t batch_count) {
  // Create an empty state.
  dataflow::DataFlowState state(threads);

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
  for (size_t i = 0; i < threads; i++) {
    clients.emplace_back(Thread, i, batch_size, batch_count, &state);
  }
  for (std::thread &client : clients) {
    client.join();
  }

  // Done.
  std::cout << "Done" << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
  std::cout << std::endl;
}

}  // namespace benchmark
}  // namespace pelton

int main(int argc, char **argv) {
  // Initialize Google’s logging library.
  google::InitGoogleLogging("cli");

  pelton::benchmark::Benchmark(
      {"CREATE TABLE tbl(col1 INT, col2 VARCHAR, col3 INT);"},
      "SELECT col2, col3 FROM tbl WHERE col3 > 10 AND col3 = ?", 3, 1, 100000);
}
