#ifndef PELTON_DATAFLOW_BENCHMARK_UTILS_H_
#define PELTON_DATAFLOW_BENCHMARK_UTILS_H_

#include <memory>
#include <vector>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/aggregate.h"
#include "pelton/dataflow/ops/equijoin.h"
#include "pelton/dataflow/ops/filter.h"
#include "pelton/dataflow/ops/identity.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/ops/matview.h"
#include "pelton/dataflow/ops/project.h"
#include "pelton/dataflow/ops/union.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {
namespace utils {

// These graphs correspond to the ones defined in graph_test_utils.h
enum class GraphType {
  FILTER_GRAPH,
  EQUIJOIN_GRAPH_WITH_EXCHANGE,
  EQUIJOIN_GRAPH_WITHOUT_EXCHANGE,
};

using CType = sqlast::ColumnDefinition::Type;

extern SchemaRef MakeFilterSchema();
extern SchemaRef MakeEquiJoinLeftSchema();
extern SchemaRef MakeEquiJoinRightSchema();
extern std::vector<std::vector<Record>> MakeFilterBatches(uint64_t num_batches,
                                                          uint64_t batch_size);
extern std::vector<std::vector<Record>> MakeEquiJoinLeftBatches(
    uint64_t num_batches, uint64_t batch_size);
extern std::vector<std::vector<Record>> MakeEquiJoinRightBatches(
    uint64_t num_batches, uint64_t batch_size);
extern std::shared_ptr<DataFlowGraph> MakeFilterGraph(ColumnID keycol,
                                                      const SchemaRef &schema);

extern std::shared_ptr<DataFlowGraph> MakeEquiJoinGraph(
    ColumnID ok, ColumnID lk, ColumnID rk, const SchemaRef &lschema,
    const SchemaRef &rschema);

}  // namespace utils
}  // namespace dataflow
}  // namespace pelton

#endif  // PELTON_DATAFLOW_BENCHMARK_UTILS_H_
