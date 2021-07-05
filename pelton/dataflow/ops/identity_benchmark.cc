#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/benchmark_utils.h"
#include "pelton/dataflow/ops/identity.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

// NOLINTNEXTLINE
static void IdentityForwards(benchmark::State& state) {
  SchemaRef schema = MakeSchema(false);
  IdentityOperator op;

  Record r{schema, true, 4_u, 5_u};
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  std::vector<Record> out_rs;
  size_t processed = 0;
  for (auto _ : state) {
    op.ProcessAndForward(UNDEFINED_NODE_INDEX, rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(IdentityForwards);

}  // namespace dataflow
}  // namespace pelton
