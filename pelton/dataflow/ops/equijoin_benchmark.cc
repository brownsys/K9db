#include <memory>
#include <utility>
#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/graph_partition.h"
#include "pelton/dataflow/ops/benchmark_utils.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// NOLINTNEXTLINE
static void JoinOneToOne(benchmark::State &state) {
  SchemaRef leftSchema = MakeSchema(false, true);
  SchemaRef rightSchema = MakeSchema(false, true);

  // Generator function: generates batches of records for benchmarking.
  RecordGenFunc gen = [rightSchema] {
    std::vector<Record> rightRecords;
    rightRecords.emplace_back(rightSchema, true, 4_u, 5_u);
    return rightRecords;
  };

  DataFlowGraphPartition g{0};
  auto iop1 = std::make_unique<InputOperator>("test-table1", leftSchema);
  auto iop2 = std::make_unique<InputOperator>("test-table2", rightSchema);
  auto op = std::make_unique<EquiJoinOperator>(0, 0);

  auto iop1_ptr = iop1.get();
  auto iop2_ptr = iop2.get();
  auto op_ptr = op.get();

  g.AddInputNode(std::move(iop1));
  g.AddInputNode(std::move(iop2));
  g.AddNode(std::move(op), {iop1_ptr, iop2_ptr});

  std::vector<Record> leftRecords;
  leftRecords.emplace_back(leftSchema, true, 4_u, 5_u);
  g.Process("test-table1", std::move(leftRecords));

  size_t processed = 0;
  for (auto _ : state) {
    ProcessBenchmark(op_ptr, 1, gen);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(JoinOneToOne);

}  // namespace dataflow
}  // namespace pelton
