#define PELTON_MATVIEW_BENCHMARK

#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/benchmark_utils.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

// This is a friend of MatViewOperator
void InitializeBenchMarkMatview(MatViewOperator *matview, SchemaRef schema) {
  matview->input_schemas_.push_back(schema);
}

// NOLINTNEXTLINE
static void UnorderedMatViewInsert2UInts(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false);
  auto op = new UnorderedMatViewOperator(schema.keys());
  InitializeBenchMarkMatview(op, schema);

  size_t processed = 0;
  for (auto _ : state) {
    std::vector<Record> rs;
    rs.emplace_back(schema, true, 4_u, 5_u);
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, std::move(rs));
    processed++;
  }
  state.SetItemsProcessed(processed);

  delete op;
}

// NOLINTNEXTLINE
static void UnorderedMatViewInsertUIntString(benchmark::State &state) {
  SchemaRef schema = MakeSchema(true);
  auto op = new UnorderedMatViewOperator(schema.keys());
  InitializeBenchMarkMatview(op, schema);

  size_t processed = 0;
  for (auto _ : state) {
    std::vector<Record> rs;
    rs.emplace_back(schema, true, 4_u, std::make_unique<std::string>("world"));
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, std::move(rs));
    processed++;
  }
  state.SetItemsProcessed(processed);

  delete op;
}

// NOLINTNEXTLINE
static void UnorderedMatViewBatchInsert(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false);
  auto op = new UnorderedMatViewOperator(schema.keys());
  InitializeBenchMarkMatview(op, schema);

  size_t processed = 0;
  for (auto _ : state) {
    std::vector<Record> rs = {};
    for (int i = 0; i < state.range(0); ++i) {
      uint64_t v = i;
      rs.emplace_back(schema, true, v, v + 1);
    }

    processed += rs.size();
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, std::move(rs));
  }
  state.SetItemsProcessed(processed);

  delete op;
}

BENCHMARK(UnorderedMatViewInsert2UInts);
BENCHMARK(UnorderedMatViewInsertUIntString);
BENCHMARK(UnorderedMatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace dataflow
}  // namespace pelton
