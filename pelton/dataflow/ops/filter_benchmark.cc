#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/benchmark-utils.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

// NOLINTNEXTLINE
static void FilterPasses(benchmark::State& state) {
  SchemaRef schema = MakeSchema(false);
  FilterOperator filter;
  filter.AddOperation(5_u, 0, FilterOperator::Operation::LESS_THAN);

  Record r{schema, true, 4_u, 5_u};
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  std::vector<Record> out_rs;
  size_t processed = 0;
  for (auto _ : state) {
    filter.Process(UNDEFINED_NODE_INDEX, rs, &out_rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

// NOLINTNEXTLINE
static void FilterDiscards(benchmark::State& state) {
  SchemaRef schema = MakeSchema(false);
  FilterOperator filter;
  filter.AddOperation(5_u, 0, FilterOperator::Operation::LESS_THAN);

  Record r{schema, true, 6_u, 5_u};
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  std::vector<Record> out_rs;
  size_t processed = 0;
  for (auto _ : state) {
    filter.Process(UNDEFINED_NODE_INDEX, rs, &out_rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(FilterPasses);
BENCHMARK(FilterDiscards);

}  // namespace dataflow
}  // namespace pelton
