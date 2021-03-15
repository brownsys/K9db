#include <vector>

#include "benchmark/benchmark.h"

#include "pelton/dataflow/key.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

SchemaOwner MakeSchema(bool use_strings) {
  std::vector<std::string> names = {"Col1", "Col2"};
  CType t;
  if (use_strings) {
    t = CType::TEXT;
  } else {
    t = CType::UINT;
  }
  std::vector<CType> types = {CType::UINT, t};
  std::vector<ColumnID> keys = {0};
  SchemaOwner schema{names, types, keys};

  return schema;
}

static void FilterPasses(benchmark::State& state) {
  SchemaOwner schema = MakeSchema(false);
  FilterOperator filter;
  filter.AddOperation(5UL, 0, FilterOperator::Operation::LESS_THAN);

  Record r(SchemaRef(schema), true, static_cast<uint64_t>(4),
           static_cast<uint64_t>(5));
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  std::vector<Record> out_rs;
  size_t processed = 0;
  for (auto _ : state) {
    filter.Process(UNDEFINED_NODE_INDEX, rs, &out_rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

static void FilterDiscards(benchmark::State& state) {
  SchemaOwner schema = MakeSchema(false);
  FilterOperator filter;
  filter.AddOperation(5UL, 0, FilterOperator::Operation::LESS_THAN);

  Record r(SchemaRef(schema), true, static_cast<uint64_t>(6),
           static_cast<uint64_t>(5));
  std::vector<Record> rs;
  rs.emplace_back(std::move(r));

  std::vector<Record> out_rs;
  size_t processed = 0;
  for (auto _ : state) {
    filter.Process(UNDEFINED_NODE_INDEX, rs, &out_rs);
    processed++;
  }
  state.SetItemsProcessed(processed);
}

BENCHMARK(FilterPasses);
BENCHMARK(FilterDiscards);

}  // namespace dataflow
}  // namespace pelton
