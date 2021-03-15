#include <vector>

#include "benchmark/benchmark.h"

#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/benchmark-utils.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

static void MatViewInsert2UInts(benchmark::State& state) {
  SchemaOwner schema = MakeSchema(false);
  auto op = std::make_shared<MatViewOperator>(schema.keys());

  Record r(SchemaRef(schema), true, static_cast<uint64_t>(4),
           static_cast<uint64_t>(5));
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  size_t processed = 0;
  for (auto _ : state) {
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

static void MatViewInsertUIntString(benchmark::State& state) {
  SchemaOwner schema = MakeSchema(true);
  auto op = std::make_shared<MatViewOperator>(schema.keys());

  std::unique_ptr<std::string> s = std::make_unique<std::string>("world");
  Record r(SchemaRef(schema), true, static_cast<uint64_t>(4), std::move(s));
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  size_t processed = 0;
  for (auto _ : state) {
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

static void MatViewBatchInsert(benchmark::State& state) {
  SchemaOwner schema = MakeSchema(false);
  auto op = std::make_shared<MatViewOperator>(schema.keys());

  std::vector<Record> rs = {};
  for (int i = 0; i < state.range(0); ++i) {
    Record r(SchemaRef(schema), true, static_cast<uint64_t>(i),
             static_cast<uint64_t>(i + 1));
    rs.emplace_back(std::move(r));
  }

  size_t processed = 0;
  for (auto _ : state) {
    processed += rs.size();
    op->ProcessAndForward(UNDEFINED_NODE_INDEX, rs);
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(MatViewInsert2UInts);
BENCHMARK(MatViewInsertUIntString);
BENCHMARK(MatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

}  // namespace dataflow
}  // namespace pelton
