#include <iostream>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "dataflow/graph.h"
#include "dataflow/operator.h"
#include "dataflow/ops/matview.h"
#include "dataflow/record.h"
#include "dataflow/types.h"

DEFINE_bool(verbose, false, "Verbose output");

// void simpleGraphSetup() {
//  DataFlowGraph g;
//
//  auto in = std::make_shared<InputOperator>();
//  auto ident = std::make_shared<IdentityOperator>();
//
//  CHECK(g.AddInputNode(in));
//  CHECK(g.AddNode(OperatorType::IDENTITY, ident, in));
//  std::vector<ColumnID> keycol = {0};
//  CHECK(g.AddNode(OperatorType::MAT_VIEW,
//                  std::make_shared<MatViewOperator>(keycol), ident));
//
//  return g;
//}

static auto uint_schema = dataflow::SchemaFactory::create_or_get(
    std::vector<dataflow::DataType>({dataflow::kUInt, dataflow::kUInt}));

void GenerateSimpleUIntRecords(std::vector<dataflow::Record>& vec,
                               size_t count) {
  for (size_t i = 0; i < count; ++i) {
    dataflow::Record r(uint_schema);
    r.set_uint(0, i);
    r.set_uint(1, i + 1ULL);
    vec.emplace_back(r);
  }
}

static void BM_MatViewInsert(benchmark::State& state) {
  std::vector<dataflow::ColumnID> kc = {0};
  uint_schema.set_key_columns(kc);
  auto op = std::make_shared<dataflow::MatViewOperator>(kc);

  std::vector<dataflow::Record> rs;
  std::vector<dataflow::Record> out_rs;
  GenerateSimpleUIntRecords(rs, 1);

  for (auto _ : state) {
    op->process(rs, out_rs);
  }
}

static void BM_MatViewBatchInsert(benchmark::State& state) {
  std::vector<dataflow::ColumnID> kc = {0};
  uint_schema.set_key_columns(kc);
  auto op = std::make_shared<dataflow::MatViewOperator>(kc);

  std::vector<dataflow::Record> rs;
  GenerateSimpleUIntRecords(rs, state.range(0));
  std::vector<dataflow::Record> out_rs;

  for (auto _ : state) {
    op->process(rs, out_rs);
  }
}

BENCHMARK(BM_MatViewInsert);
BENCHMARK(BM_MatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_MAIN();
