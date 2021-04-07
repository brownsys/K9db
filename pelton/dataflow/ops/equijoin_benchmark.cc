#include <vector>

#include "benchmark/benchmark.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/benchmark-utils.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/identity.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

static void JoinOneToOne(benchmark::State& state) {
  SchemaOwner leftSchema = MakeSchema(false);
  SchemaOwner rightSchema = MakeSchema(false);

  std::shared_ptr<IdentityOperator> iop1 = std::make_shared<IdentityOperator>();
  std::shared_ptr<IdentityOperator> iop2 = std::make_shared<IdentityOperator>();
  std::shared_ptr<EquiJoinOperator> op =
      std::make_shared<EquiJoinOperator>(0, 0);
  iop1->SetIndex(0);
  iop2->SetIndex(1);
  op->parents_ = {std::make_shared<Edge>(iop1, op),
                  std::make_shared<Edge>(iop2, op)};
  op->input_schemas_ = {SchemaRef(leftSchema), SchemaRef(rightSchema)};
  op->ComputeOutputSchema();

  Record lr1(SchemaRef(leftSchema), true, static_cast<uint64_t>(4),
             static_cast<uint64_t>(5));
  Record rr1(SchemaRef(rightSchema), true, static_cast<uint64_t>(4),
             static_cast<uint64_t>(5));
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
