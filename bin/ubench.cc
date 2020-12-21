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

static void BM_MatViewInsert(benchmark::State& state) {
  std::vector<dataflow::ColumnID> kc = {0};
  auto op = std::make_shared<dataflow::MatViewOperator>(kc);

  std::vector<dataflow::RecordData> rd = {dataflow::RecordData(4ULL),
                                          dataflow::RecordData(5ULL)};
  std::vector<dataflow::Record> rs = {dataflow::Record(true, rd, 3ULL)};
  std::vector<dataflow::Record> out_rs;

  for (auto _ : state) {
    op->process(rs, out_rs);
  }
}

static void BM_MatViewBatchInsert(benchmark::State& state) {
  std::vector<dataflow::ColumnID> kc = {0};
  auto op = std::make_shared<dataflow::MatViewOperator>(kc);

  std::vector<dataflow::Record> rs = {};
  for (int i = 0; i < state.range(0); ++i) {
    std::vector<dataflow::RecordData> rd = {dataflow::RecordData(i),
                                            dataflow::RecordData(i + 1)};
    rs.push_back(dataflow::Record(true, rd, 3ULL));
  }
  std::vector<dataflow::Record> out_rs;

  for (auto _ : state) {
    op->process(rs, out_rs);
  }
}

BENCHMARK(BM_MatViewInsert);
BENCHMARK(BM_MatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_MAIN();
