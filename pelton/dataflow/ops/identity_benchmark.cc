#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/ops/benchmark_utils.h"
#include "pelton/dataflow/ops/identity.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

// NOLINTNEXTLINE
static void IdentityForwards(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false);
  IdentityOperator op;

  // Generator function: generates batches of records for benchmarking.
  RecordGenFunc gen = [schema] {
    std::vector<Record> records;
    records.emplace_back(schema, true, 4_u, 5_u);
    return records;
  };

  size_t processed = 0;
  for (auto _ : state) {
    ProcessBenchmark(&op, UNDEFINED_NODE_INDEX, gen);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(IdentityForwards);

}  // namespace dataflow
}  // namespace pelton
