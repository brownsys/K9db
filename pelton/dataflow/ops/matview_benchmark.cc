#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/benchmark-utils.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

// NOLINTNEXTLINE
static void UnorderedMatViewInsert2UInts(benchmark::State& state) {
  SchemaRef schema = MakeSchema(false);
  auto op = new UnorderedMatViewOperator(schema.keys());

  Record r{schema, true, 4_u, 5_u};
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  size_t processed = 0;
  for (auto _ : state) {
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, rs);
    processed++;
  }
  state.SetItemsProcessed(processed);

  delete op;
}

// NOLINTNEXTLINE
static void UnorderedMatViewInsertUIntString(benchmark::State& state) {
  SchemaRef schema = MakeSchema(true);
  auto op = new UnorderedMatViewOperator(schema.keys());

  Record r{schema, true, 4_u, std::make_unique<std::string>("world")};
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  size_t processed = 0;
  for (auto _ : state) {
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, rs);
    processed++;
  }
  state.SetItemsProcessed(processed);

  delete op;
}

// NOLINTNEXTLINE
static void UnorderedMatViewBatchInsert(benchmark::State& state) {
  SchemaRef schema = MakeSchema(false);
  auto op = new UnorderedMatViewOperator(schema.keys());

  std::vector<Record> rs = {};
  for (int i = 0; i < state.range(0); ++i) {
    uint64_t v = i;
    rs.emplace_back(schema, true, v, v + 1);
  }

  size_t processed = 0;
  for (auto _ : state) {
    processed += rs.size();
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, rs);
  }
  state.SetItemsProcessed(processed);

  delete op;
}

BENCHMARK(UnorderedMatViewInsert2UInts);
BENCHMARK(UnorderedMatViewInsertUIntString);
BENCHMARK(UnorderedMatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace dataflow
}  // namespace pelton
