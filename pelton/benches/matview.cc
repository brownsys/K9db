#include <iostream>
#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "gflags/gflags.h"
#include "glog/logging.h"

#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

DEFINE_bool(verbose, false, "Verbose output");

namespace dataflow = pelton::dataflow;

using CType = pelton::sqlast::ColumnDefinition::Type;
using ColumnID = dataflow::ColumnID;

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
//

dataflow::SchemaOwner MakeSchema() {
  std::vector<std::string> names = {"Col1", "Col2"};
  std::vector<CType> types = {CType::UINT, CType::UINT};
  std::vector<ColumnID> keys = {0};
  dataflow::SchemaOwner schema{names, types, keys};

  return schema;
}

static void BM_MatViewInsert(benchmark::State& state) {
  dataflow::SchemaOwner schema = MakeSchema();
  auto op = std::make_shared<dataflow::MatViewOperator>(schema.keys());

  dataflow::Record r(dataflow::SchemaRef(schema), true,
		     static_cast<uint64_t>(4), static_cast<uint64_t>(5));
  std::vector<dataflow::Record> rs;
  rs.emplace_back(std::move(r));

  for (auto _ : state) {
    op->ProcessAndForward(dataflow::UNDEFINED_NODE_INDEX, rs);
  }
}

//static void BM_MatViewBatchInsert(benchmark::State& state) {
//  std::vector<dataflow::ColumnID> kc = {0};
//  auto op = std::make_shared<dataflow::MatViewOperator>(kc);
//
//  std::vector<dataflow::Record> rs = {};
//  for (int i = 0; i < state.range(0); ++i) {
//    std::vector<dataflow::RecordData> rd = {dataflow::RecordData(i),
//                                            dataflow::RecordData(i + 1)};
//    rs.push_back(dataflow::Record(true, rd, 3ULL));
//  }
//  std::vector<dataflow::Record> out_rs;
//
//  for (auto _ : state) {
//    op->process(rs, out_rs);
//  }
//}

BENCHMARK(BM_MatViewInsert);
//BENCHMARK(BM_MatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_MAIN();
