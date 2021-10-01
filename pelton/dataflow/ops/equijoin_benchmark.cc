#define PELTON_BENCHMARK

#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
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
void JoinOneToOne(benchmark::State &state) {
  SchemaRef leftSchema = MakeSchema(false);
  SchemaRef rightSchema = MakeSchema(false);

  DataFlowGraph g;
  auto iop1 = std::make_shared<InputOperator>("test-table1", leftSchema);
  auto iop2 = std::make_shared<InputOperator>("test-table2", rightSchema);
  auto op = std::make_shared<EquiJoinOperator>(0, 0);
  g.AddInputNode(iop1);
  g.AddInputNode(iop2);
  g.AddNode(op, {iop1, iop2});

  std::vector<Record> leftRecords;
  leftRecords.emplace_back(leftSchema, true, 4_u, 5_u);
  op->Process(0, std::move(leftRecords));

  size_t processed = 0;
  for (auto _ : state) {
    std::vector<Record> rightRecords;
    rightRecords.emplace_back(rightSchema, true, 4_u, 5_u);
    op->Process(1, std::move(rightRecords));
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(JoinOneToOne);

}  // namespace dataflow
}  // namespace pelton
