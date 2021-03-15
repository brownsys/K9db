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

dataflow::SchemaOwner MakeSchema(bool use_strings) {
  std::vector<std::string> names = {"Col1", "Col2"};
  CType t;
  if (use_strings) {
    t = CType::TEXT;
  } else {
    t = CType::UINT;
  }
  std::vector<CType> types = {CType::UINT, t};
  std::vector<ColumnID> keys = {0};
  dataflow::SchemaOwner schema{names, types, keys};

  return schema;
}

static void BM_MatViewInsert2UInts(benchmark::State& state) {
  dataflow::SchemaOwner schema = MakeSchema(false);
  auto op = std::make_shared<dataflow::MatViewOperator>(schema.keys());

  dataflow::Record r(dataflow::SchemaRef(schema), true,
                     static_cast<uint64_t>(4), static_cast<uint64_t>(5));
  std::vector<dataflow::Record> rs;
  rs.emplace_back(std::move(r));

  size_t processed = 0;
  for (auto _ : state) {
    op->ProcessAndForward(dataflow::UNDEFINED_NODE_INDEX, rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

static void BM_MatViewInsert2Strings(benchmark::State& state) {
  dataflow::SchemaOwner schema = MakeSchema(true);
  auto op = std::make_shared<dataflow::MatViewOperator>(schema.keys());

  std::unique_ptr<std::string> s = std::make_unique<std::string>("world");
  dataflow::Record r(dataflow::SchemaRef(schema), true,
                     static_cast<uint64_t>(4), std::move(s));
  std::vector<dataflow::Record> rs;
  rs.emplace_back(std::move(r));

  size_t processed = 0;
  for (auto _ : state) {
    op->ProcessAndForward(dataflow::UNDEFINED_NODE_INDEX, rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

static void BM_MatViewBatchInsert(benchmark::State& state) {
  dataflow::SchemaOwner schema = MakeSchema(false);
  auto op = std::make_shared<dataflow::MatViewOperator>(schema.keys());

  std::vector<dataflow::Record> rs = {};
  for (int i = 0; i < state.range(0); ++i) {
    dataflow::Record r(dataflow::SchemaRef(schema), true,
                       static_cast<uint64_t>(i), static_cast<uint64_t>(i + 1));
    rs.emplace_back(std::move(r));
  }

  size_t processed = 0;
  for (auto _ : state) {
    processed += rs.size();
    op->ProcessAndForward(dataflow::UNDEFINED_NODE_INDEX, rs);
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(BM_MatViewInsert2UInts);
BENCHMARK(BM_MatViewInsert2Strings);
BENCHMARK(BM_MatViewBatchInsert)->Arg(10)->Arg(100)->Arg(1000);

BENCHMARK_MAIN();
