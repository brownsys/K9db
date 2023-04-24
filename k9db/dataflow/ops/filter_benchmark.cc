#include <vector>

#include "benchmark/benchmark.h"
#include "k9db/dataflow/ops/benchmark_utils.h"
#include "k9db/dataflow/ops/filter.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"
#include "k9db/util/ints.h"

namespace k9db {
namespace dataflow {

// NOLINTNEXTLINE
static void FilterPasses(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false, true);
  FilterOperator filter;
  filter.AddLiteralOperation(0, 5_u, FilterOperator::Operation::LESS_THAN);

  // Generator function: generates batches of records for benchmarking.
  RecordGenFunc gen = [schema] {
    std::vector<Record> records;
    records.emplace_back(schema, true, 4_u, 5_u);
    return records;
  };

  size_t processed = 0;
  for (auto _ : state) {
    ProcessBenchmark(&filter, UNDEFINED_NODE_INDEX, gen);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

// NOLINTNEXTLINE
static void FilterDiscards(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false, true);
  FilterOperator filter;
  filter.AddLiteralOperation(0, 5_u, FilterOperator::Operation::LESS_THAN);

  // Generator function: generates batches of records for benchmarking.
  RecordGenFunc gen = [schema] {
    std::vector<Record> records;
    records.emplace_back(schema, true, 6_u, 5_u);
    return records;
  };

  size_t processed = 0;
  for (auto _ : state) {
    ProcessBenchmark(&filter, UNDEFINED_NODE_INDEX, gen);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(FilterPasses);
BENCHMARK(FilterDiscards);

}  // namespace dataflow
}  // namespace k9db
