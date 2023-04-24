#include <memory>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "k9db/dataflow/graph_partition.h"
#include "k9db/dataflow/ops/benchmark_utils.h"
#include "k9db/dataflow/ops/input.h"
#include "k9db/dataflow/ops/matview.h"
#include "k9db/dataflow/record.h"
#include "k9db/dataflow/schema.h"
#include "k9db/dataflow/types.h"
#include "k9db/util/ints.h"

namespace k9db {
namespace dataflow {

// NOLINTNEXTLINE
static void UnorderedMatViewInsert2UInts(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false, false);

  // Generator function: generates batches of records for benchmarking.
  RecordGenFunc gen = [schema] {
    std::vector<Record> records;
    records.emplace_back(schema, true, 4_u, 5_u);
    return records;
  };

  DataFlowGraphPartition g{0};
  auto in = std::make_unique<InputOperator>("test-table1", schema);
  auto op = std::make_unique<UnorderedMatViewOperator>(schema.keys());
  auto in_ptr = in.get();
  auto op_ptr = op.get();
  g.AddInputNode(std::move(in));
  g.AddOutputOperator(std::move(op), in_ptr);

  size_t processed = 0;
  for (auto _ : state) {
    ProcessBenchmark(op_ptr, UNDEFINED_NODE_INDEX, gen);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

// NOLINTNEXTLINE
static void UnorderedMatViewInsertUIntString(benchmark::State &state) {
  SchemaRef schema = MakeSchema(true, false);

  // Generator function: generates batches of records for benchmarking.
  RecordGenFunc gen = [schema] {
    std::vector<Record> records;
    records.emplace_back(schema, true, 4_u,
                         std::make_unique<std::string>("hello!"));
    return records;
  };

  DataFlowGraphPartition g{0};
  auto in = std::make_unique<InputOperator>("test-table1", schema);
  auto op = std::make_unique<UnorderedMatViewOperator>(schema.keys());
  auto in_ptr = in.get();
  auto op_ptr = op.get();
  g.AddInputNode(std::move(in));
  g.AddOutputOperator(std::move(op), in_ptr);

  size_t processed = 0;
  for (auto _ : state) {
    ProcessBenchmark(op_ptr, UNDEFINED_NODE_INDEX, gen);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

// NOLINTNEXTLINE
static void UnorderedMatViewBatchInsert(benchmark::State &state) {
  SchemaRef schema = MakeSchema(false, false);

  // Generator function: generates batches of records for benchmarking.
  RecordGenFunc gen = [schema, &state] {
    std::vector<Record> records;
    for (int i = 0; i < state.range(0); ++i) {
      records.emplace_back(schema, true, uint64_t(i), uint64_t(i) + 1);
    }
    return records;
  };

  DataFlowGraphPartition g{0};
  auto in = std::make_unique<InputOperator>("test-table1", schema);
  auto op = std::make_unique<UnorderedMatViewOperator>(schema.keys());
  auto in_ptr = in.get();
  auto op_ptr = op.get();
  g.AddInputNode(std::move(in));
  g.AddOutputOperator(std::move(op), in_ptr);

  size_t processed = 0;
  for (auto _ : state) {
    ProcessBenchmark(op_ptr, UNDEFINED_NODE_INDEX, gen);
    processed += state.range(0);
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(UnorderedMatViewInsert2UInts);
BENCHMARK(UnorderedMatViewInsertUIntString);
BENCHMARK(UnorderedMatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace dataflow
}  // namespace k9db
