#define PELTON_BENCHMARK

#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/benchmark-utils.h"
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
void JoinOneToOne(benchmark::State& state) {
  SchemaRef leftSchema = MakeSchema(false);
  SchemaRef rightSchema = MakeSchema(false);

  DataFlowGraph g;
  auto iop1 = std::make_shared<InputOperator>("test-table1", leftSchema);
  auto iop2 = std::make_shared<InputOperator>("test-table2", rightSchema);
  auto op = std::make_shared<EquiJoinOperator>(0, 0);
  g.AddInputNode(iop1);
  g.AddInputNode(iop2);
  g.AddNode(op, {iop1, iop2});

  Record lr1{leftSchema, true, 4_u, 5_u};
  Record rr1{rightSchema, true, 4_u, 5_u};
  std::vector<Record> leftRecords;
  std::vector<Record> rightRecords;
  leftRecords.emplace_back(std::move(lr1));
  rightRecords.emplace_back(std::move(rr1));

  std::vector<Record> out_rs;
  op->Process(0, leftRecords, &out_rs);
  size_t processed = 0;
  for (auto _ : state) {
    op->Process(1, rightRecords, &out_rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(JoinOneToOne);

}  // namespace dataflow
}  // namespace pelton
