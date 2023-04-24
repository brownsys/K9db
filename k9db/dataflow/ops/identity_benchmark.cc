#include <vector>

#include "benchmark/benchmark.h"
#include "k9db/dataflow/ops/benchmark_utils.h"
#include "k9db/dataflow/ops/identity.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"
#include "k9db/util/ints.h"

namespace k9db {
namespace dataflow {

// NOLINTNEXTLINE
static void IdentityForwards(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false, true);
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
}  // namespace k9db
